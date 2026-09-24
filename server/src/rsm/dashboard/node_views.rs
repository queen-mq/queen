//! Raft-mode ports of the dashboard's node-metric stored procedures.
//!
//! Postgres answers three operator routes of the dashboard with stored
//! procedures over the per-replica metric tables. Raft mode has no database:
//! every node keeps its own [`WorkerRow`] / [`SystemRow`] / [`QueueRow`] rows
//! (see [`super::model`]), a dashboard read gathers every node's rows, and the
//! functions below re-aggregate them into the JSON the Postgres path serves
//! (same keys, nesting, types, null / 0 / `[]` semantics, ordering and bucket
//! rules), so the Vue dashboard (app/src) works unchanged.
//!
//! | function | stored procedure | route (Postgres handler) |
//! |---|---|---|
//! | [`system_metrics_json`] | `queen.get_system_metrics_v1` (015_status.sql:19) | `GET /api/v1/analytics/system-metrics` (handlers/analytics.rs:84) |
//! | [`worker_metrics_json`] | `queen.get_worker_metrics_timeseries_v1` (019_worker_metrics.sql:1094) | `GET /api/v1/analytics/worker-metrics` (handlers/analytics.rs:97) |
//! | [`status_json`] | `queen.get_status_v3` (019_worker_metrics.sql:711) | `GET /api/v1/status` (handlers/status.rs:106) |
//!
//! None of the three handlers post-processes the SP result: it is served
//! verbatim through `sp_result_to_response`, so these functions return the
//! whole body.
//!
//! Rules shared by the three, each reproduced from the SQL on purpose:
//!
//! * **Rows in**: pass every retained row of every node, not only the rows of
//!   `[from, to]` (and note `DashStore::range` is half-open where the SQL's
//!   range is closed). The range is applied here; the lifetime totals,
//!   `statsAge` and the 2- / 5-minute worker lists read outside it.
//! * **Filters** are the object the Postgres handler builds with
//!   `filters_from_query(params, KEYS)` (handlers/mod.rs:671, non-empty query
//!   values only); the `*_FILTER_KEYS` constants are those KEYS. A value is read
//!   like `p_filters->>'key'`: a JSON string as is, JSON null or a missing key
//!   as SQL NULL, any other JSON value as its JSON text.
//! * **Range**: `from` / `to` default to `now - 1 h` / `now`, both bounds
//!   inclusive. A value that does not parse (or a `workerId` that is not an
//!   integer) answers `{"error": "<Postgres' message>"}`: that is where the SP
//!   raises and the Postgres handler answers HTTP 500, so a caller maps a
//!   top-level `error` key to 500 the way `sp_result_to_response` does.
//! * **Bucket width** from the range length in minutes (`EXTRACT(EPOCH ...) / 60`
//!   assigned to an INTEGER, i.e. rounded half away from zero): `<= 60` gives 1,
//!   `<= 360` gives 5, `<= 1440` gives 15, `<= 10080` gives 60, anything longer
//!   360. A row's bucket is `date_trunc('minute', t) - (minute_of_hour(t) %
//!   width)` minutes in UTC, so the 360-minute width buckets by the HOUR while
//!   the per-second rates still divide by 21 600 s. A quirk of the SQL, kept.
//! * **Timestamps**: `timeRange` and the system-metrics points are
//!   `YYYY-MM-DDTHH:MM:SS.mmmZ`, the throughput points and `lastSeen`
//!   `YYYY-MM-DDTHH:MM:SSZ` (the SQL's two `to_char` masks).
//! * **Numbers**: sums, counts and `ROUND(x)` are JSON integers; `ROUND(x, n)`
//!   and the SQL's unrounded numeric divisions are JSON floats (same value,
//!   Postgres prints more trailing digits). Integer arithmetic is exact (i128)
//!   and rounds half away from zero like `numeric`.
//! * **Duplicate rows** collapse the way the tables' unique keys collapse them:
//!   `system_metrics` keeps the LAST row per `(timestamp, hostname, port,
//!   worker_id)` (its insert upserts), `worker_metrics` the FIRST per
//!   `(hostname, worker_id, pid, bucket_time)` (its insert does nothing on
//!   conflict, and neither does the summary trigger).
//! * **Lifetime totals**: Postgres keeps them in `queen.worker_metrics_summary`
//!   (019_worker_metrics.sql:376), a trigger rollup of every `worker_metrics`
//!   row ever inserted (update_worker_metrics_summary, :400). Here they are the
//!   sums over ALL worker rows passed in, whatever the range: they cover what
//!   the nodes still retain, not the cell's whole life.
//! * **Not measured in raft mode**: `worker_metrics.db_connections` /
//!   `avg_free_slots` / `min_free_slots` (the Postgres pool gauges) have no
//!   [`WorkerRow`] field, so every key read from them is null; the job-queue and
//!   backoff keys are null in Postgres too (`NULLIF(.., 0)` over columns nothing
//!   writes). The `database` gauge family is absent from raft `metrics_json`,
//!   so its leaves and the `dbPool*` points are null.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::{json, Map, Number, Value};

use super::model::*;

/// Query keys `GET /api/v1/analytics/system-metrics` turns into the filter
/// object (handlers/analytics.rs:88).
pub const SYSTEM_METRICS_FILTER_KEYS: [&str; 4] = ["from", "to", "hostname", "workerId"];

/// Query keys `GET /api/v1/analytics/worker-metrics` turns into the filter
/// object (handlers/analytics.rs:101). `queue` is forwarded, but the SP never
/// reads it.
pub const WORKER_METRICS_FILTER_KEYS: [&str; 5] = ["from", "to", "queue", "hostname", "workerId"];

/// Query keys `GET /api/v1/status` turns into the filter object
/// (handlers/status.rs:110).
pub const STATUS_FILTER_KEYS: [&str; 5] = ["from", "to", "queue", "namespace", "task"];

// ---------------------------------------------------------------------------
// Inputs of status_json
// ---------------------------------------------------------------------------

/// Everything `queen.get_status_v3` (019_worker_metrics.sql:711) reads besides
/// `worker_metrics` and `system_metrics`. Postgres reads these from replicated
/// tables; in raft mode they come from the answering node's store. The view is
/// cell-wide: the SP has no tenant predicate anywhere, so nothing here is
/// tenant-filtered.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StatusState {
    /// Every queue of every tenant, each with its `queen.stats` 'queue' row
    /// (`queen.queues LEFT JOIN queen.stats`, 019:964). Serves the `queues`
    /// list (filtered by queue / namespace / task, and only queues whose
    /// `total_messages > 0`), the cell-wide `messages.pending` / `processing`
    /// sums (never filtered, 019:979), and the namespace / task lookup of the
    /// queue-scoped throughput branch: that branch's `JOIN queen.queues`, so
    /// rows of a queue that is not listed here are dropped, as the FK cascade
    /// drops them in Postgres.
    pub queues: Vec<StatusQueue>,
    /// `queen.queue_lag_metrics` counters: every tenant's [`QueueRow`]s,
    /// merged across nodes ([`merge_queue_rows`]). Read only when a `queue`,
    /// `namespace` or `task` filter is set (019:761).
    pub queue_rows: Vec<QueueRow>,
    /// The partition-lifecycle rows ([`ChurnRow`], the answering node's).
    /// Postgres keeps them in `queue_lag_metrics` too, so a bucket that only
    /// saw partitions created or deleted is still a throughput point, with
    /// every counter 0. Read only when a queue / namespace / task filter is set.
    pub churn_rows: Vec<ChurnRow>,
    /// `queen.log_consumers` rows (019:1026). Any cursor may be passed: only
    /// the rows whose `lease_expires_us` is set and later than `now_us` count,
    /// the SP's `lease_expires_at > NOW()` (it does not look at `worker_id`).
    pub leases: Vec<StatusLease>,
    /// `queen.log_dlq`, aggregated (019:1044).
    pub dlq: StatusDlq,
}

/// One queue and its `queen.stats` 'queue' row, as
/// `queen.log_refresh_all_stats_v1` (011_log_stats.sql:106) writes it. A queue
/// with no stats row yet reads as all zeros, exactly like the SQL's COALESCEs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StatusQueue {
    /// `queen.queues.id` as text (raft: the queue's UUID string).
    pub id: String,
    /// The tenant key [`QueueRow::tenant`] / [`ChurnRow::tenant`] carry: joins
    /// those rows to this queue. Not emitted.
    pub tenant: String,
    /// `queen.queues.name`.
    pub name: String,
    /// `queen.queues.namespace`; `None` is SQL NULL (emitted as null, matched
    /// by no namespace filter). An empty string is a value, as in Postgres.
    pub namespace: Option<String>,
    /// `queen.queues.task`; `None` is SQL NULL.
    pub task: Option<String>,
    /// `stats.child_count`: the queue's live partitions (`queen.log_partitions`
    /// rows).
    pub partitions: i64,
    /// `stats.total_messages`: retained frames, the sum over the partitions of
    /// `GREATEST(last_offset - log_start + 1, 0)`.
    pub total_messages: i64,
    /// `stats.pending_messages`: the sum over the partitions of
    /// `GREATEST(last_offset - GREATEST(worst, log_start - 1), 0)`, where worst
    /// is `MIN(committed)` over the named consumer groups, else the
    /// `__QUEUE_MODE__` cursor, else -1.
    pub pending_messages: i64,
    /// `stats.processing_messages`: `LEAST(Σ live leases' (batch_end -
    /// committed), pending_messages)`.
    pub processing_messages: i64,
    /// `stats.completed_messages`: `GREATEST(0, total - pending - dlq)`,
    /// emitted as the queue's `totalConsumed`.
    pub completed_messages: i64,
}

/// One `queen.log_consumers` row (001_log_schema.sql:217), reduced to what the
/// lease block reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StatusLease {
    /// `partition_id` (raft: the partition's pid). Only compared for equality
    /// (`partitionsWithLeases` is a distinct count).
    pub partition_id: u64,
    /// `committed`: the last acked offset, -1 when nothing is acked.
    pub committed: i64,
    /// `batch_end`: inclusive end of the leased batch; `None` is SQL NULL.
    pub batch_end: Option<i64>,
    /// `lease_expires_at` in epoch microseconds; `None` is SQL NULL.
    pub lease_expires_us: Option<i64>,
}

/// `queen.log_dlq` (005_log_ack.sql:60) aggregated over every tenant.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StatusDlq {
    /// `SELECT COUNT(*) FROM queen.log_dlq`.
    pub current_messages: i64,
    /// `COUNT(DISTINCT partition_id)` over `queen.log_dlq`.
    pub affected_partitions: i64,
    /// Row counts per distinct `error` text. The view groups them by
    /// `COALESCE(error, 'unknown')`, orders by count descending (ties by error
    /// text; Postgres leaves them unspecified) and keeps five. Entries with a
    /// count below 1 are ignored: a group exists only when it has rows.
    pub errors: Vec<DlqErrorCount>,
}

/// Rows of `queen.log_dlq` carrying one `error` text.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DlqErrorCount {
    /// `error`; `None` is SQL NULL and reports as `"unknown"`.
    pub error: Option<String>,
    pub count: i64,
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/system-metrics
// ---------------------------------------------------------------------------

/// Port of `queen.get_system_metrics_v1` (015_status.sql:19), the body of
/// `GET /api/v1/analytics/system-metrics` (handlers/analytics.rs:84).
///
/// Filters ([`SYSTEM_METRICS_FILTER_KEYS`]): `from`, `to`, `hostname` (exact),
/// `workerId` (exact text: the rows' `"worker-0"`). One point per replica
/// `(hostname, port, workerId)` and bucket. Every gauge leaf is `{avg, min,
/// max, last}`: `avg` is `Σ(avg × sample_count) / Σ sample_count` with the
/// denominator over every row of the bucket, `min` / `max` run over the rows
/// that carry the leaf, `last` is the bucket's latest row's value (null when
/// that row lacks it).
///
/// The SQL names every path it aggregates rather than walking the object, so
/// this does too: `database` and `threadpool` come out as null leaves (raft
/// rows carry no `database` family and no collector writes `threadpool`),
/// `uptime_seconds` is dropped, and `shared_state` keeps its mixed shape
/// (`enabled` is `bool_or`, the counters are the latest row's `last`,
/// `queue_backoff_summary` the latest value present; the raft collector
/// writes no `shared_state`, so all of it is null there, where Postgres rows
/// read `false` / 0 / `[]`; no view reads it). The one addition to the
/// SP's shape is the raft family, `raft.{inflight, applied_lag, log_bytes,
/// log_files, map_used_pct}`, aggregated like the SQL's gauges. Replicas are
/// ordered by `(hostname, port, workerId)` (the SQL leaves the order
/// unspecified), points by time ascending.
pub fn system_metrics_json(filters: &Map<String, Value>, now_us: i64, rows: &[SystemRow]) -> Value {
    let range = match Range::resolve(filters, now_us) {
        Ok(r) => r,
        Err(e) => return error_json(e),
    };
    let hostname = filter_text(filters, "hostname");
    let worker_id = filter_text(filters, "workerId");

    type ReplicaKey<'a> = (&'a str, i32, &'a str);
    let mut replicas: BTreeMap<ReplicaKey, BTreeMap<i64, Vec<GaugeSample>>> = BTreeMap::new();
    // dedup_systems sorts by (hostname, port, worker_id, at_us): each bucket's
    // samples arrive in time order, so the last one is the bucket's latest.
    for row in dedup_systems(rows) {
        if !range.contains(row.at_us)
            || hostname.as_deref().is_some_and(|h| h != row.hostname)
            || worker_id.as_deref().is_some_and(|w| w != row.worker_id)
        {
            continue;
        }
        replicas
            .entry((row.hostname.as_str(), row.port, row.worker_id.as_str()))
            .or_default()
            .entry(range.bucket(row.at_us))
            .or_default()
            .push(GaugeSample::of(row));
    }

    let mut point_count = 0usize;
    let mut replicas_json = Vec::with_capacity(replicas.len());
    for ((host, port, wid), buckets) in replicas {
        let mut series = Vec::with_capacity(buckets.len());
        for (bucket, samples) in buckets {
            point_count += 1;
            let sample_count: i64 = samples.iter().map(|s| s.sample_count).sum();
            series.push(json!({
                "timestamp": ts_ms(bucket),
                "sampleCount": sample_count,
                "metrics": gauge_metrics(&samples),
            }));
        }
        replicas_json.push(json!({
            "hostname": host,
            "port": port,
            "workerId": wid,
            "timeSeries": series,
        }));
    }
    json!({
        "timeRange": range.time_range_json(),
        "replicaCount": replicas_json.len(),
        "replicas": replicas_json,
        "bucketMinutes": range.bucket_minutes,
        "pointCount": point_count,
    })
}

/// One `system_metrics` row inside a bucket: its weight and parsed gauges. A
/// `metrics_json` that does not parse reads as JSON null: the row still
/// counts (its `sample_count` weighs every `avg`), every leaf of it is missing.
struct GaugeSample {
    sample_count: i64,
    metrics: Value,
}

impl GaugeSample {
    fn of(row: &SystemRow) -> GaugeSample {
        GaugeSample {
            sample_count: i64::from(row.sample_count),
            metrics: serde_json::from_str(&row.metrics_json).unwrap_or(Value::Null),
        }
    }
}

/// The `metrics` object of one point: the `jsonb_build_object` of
/// 015_status.sql:76, path by path, plus the raft family.
fn gauge_metrics(s: &[GaugeSample]) -> Value {
    let g = |path: &[&str]| leaf_stats(s, path);
    let last = |path: &[&str]| latest_leaf_last(s, path);
    let ops = |op: &str| {
        json!({
            "count": last(&["shared_state", "sidecar_ops", op, "count"]),
            "latency_us": last(&["shared_state", "sidecar_ops", op, "latency_us"]),
            "items": last(&["shared_state", "sidecar_ops", op, "items"]),
        })
    };
    json!({
        "cpu": {
            "user_us": g(&["cpu", "user_us"]),
            "system_us": g(&["cpu", "system_us"]),
        },
        "memory": {
            "rss_bytes": g(&["memory", "rss_bytes"]),
            "virtual_bytes": g(&["memory", "virtual_bytes"]),
        },
        "database": {
            "pool_size": g(&["database", "pool_size"]),
            "pool_idle": g(&["database", "pool_idle"]),
            "pool_active": g(&["database", "pool_active"]),
        },
        "threadpool": {
            "db": {
                "pool_size": g(&["threadpool", "db", "pool_size"]),
                "queue_size": g(&["threadpool", "db", "queue_size"]),
            },
            "system": {
                "pool_size": g(&["threadpool", "system", "pool_size"]),
                "queue_size": g(&["threadpool", "system", "queue_size"]),
            },
        },
        "registries": {
            "response": g(&["registries", "response"]),
        },
        "shared_state": {
            "enabled": bool_or(s, &["shared_state", "enabled"]),
            "sidecar_ops": {
                "push": ops("push"),
                "pop": ops("pop"),
                "ack": ops("ack"),
            },
            "queue_backoff": {
                "queues_with_backoff": last(&["shared_state", "queue_backoff", "queues_with_backoff"]),
                "total_backed_off_groups": last(&["shared_state", "queue_backoff", "total_backed_off_groups"]),
                "avg_interval_ms": last(&["shared_state", "queue_backoff", "avg_interval_ms"]),
            },
            "queue_backoff_summary": latest_present(s, &["shared_state", "queue_backoff_summary"]),
            "queue_config_cache": {
                "size": last(&["shared_state", "queue_config_cache", "size"]),
                "hits": last(&["shared_state", "queue_config_cache", "hits"]),
                "misses": last(&["shared_state", "queue_config_cache", "misses"]),
            },
            "consumer_presence": {
                "queues_tracked": last(&["shared_state", "consumer_presence", "queues_tracked"]),
                "servers_tracked": last(&["shared_state", "consumer_presence", "servers_tracked"]),
                "total_registrations": last(&["shared_state", "consumer_presence", "total_registrations"]),
            },
            "server_health": {
                "alive": last(&["shared_state", "server_health", "alive"]),
                "dead": last(&["shared_state", "server_health", "dead"]),
            },
            "transport": {
                "sent": last(&["shared_state", "transport", "sent"]),
                "received": last(&["shared_state", "transport", "received"]),
                "dropped": last(&["shared_state", "transport", "dropped"]),
            },
        },
        "raft": {
            "inflight": g(&["raft", "inflight"]),
            "applied_lag": g(&["raft", "applied_lag"]),
            "log_bytes": g(&["raft", "log_bytes"]),
            "log_files": g(&["raft", "log_files"]),
            "map_used_pct": g(&["raft", "map_used_pct"]),
        },
    })
}

/// `{avg, min, max, last}` of one gauge leaf over a bucket's samples.
fn leaf_stats(samples: &[GaugeSample], path: &[&str]) -> Value {
    let mut weighted: Option<f64> = None;
    let mut weight: i64 = 0;
    let mut min: Option<(f64, Value)> = None;
    let mut max: Option<(f64, Value)> = None;
    for s in samples {
        // NULLIF(SUM(sample_count), 0) runs over every row, leaf or not.
        weight += s.sample_count;
        let leaf = at(&s.metrics, path);
        if let Some((avg, _)) = numeric(leaf.and_then(|l| l.get("avg"))) {
            *weighted.get_or_insert(0.0) += avg * s.sample_count as f64;
        }
        if let Some((v, raw)) = numeric(leaf.and_then(|l| l.get("min"))) {
            if min.as_ref().is_none_or(|(m, _)| v < *m) {
                min = Some((v, raw));
            }
        }
        if let Some((v, raw)) = numeric(leaf.and_then(|l| l.get("max"))) {
            if max.as_ref().is_none_or(|(m, _)| v > *m) {
                max = Some((v, raw));
            }
        }
    }
    let avg = match weighted {
        Some(sum) if weight != 0 => float(sum / weight as f64),
        _ => Value::Null,
    };
    json!({
        "avg": avg,
        "min": min.map_or(Value::Null, |(_, raw)| raw),
        "max": max.map_or(Value::Null, |(_, raw)| raw),
        "last": latest_leaf_last(samples, path),
    })
}

/// `(array_agg((leaf->>'last')::numeric ORDER BY timestamp DESC))[1]`: the
/// latest sample's value, null when that sample lacks it (array_agg keeps
/// NULLs, so an older sample never stands in).
fn latest_leaf_last(samples: &[GaugeSample], path: &[&str]) -> Value {
    samples
        .last()
        .and_then(|s| numeric(at(&s.metrics, path).and_then(|l| l.get("last"))))
        .map_or(Value::Null, |(_, raw)| raw)
}

/// `bool_or((path)::boolean)`: null when no sample carries the flag.
fn bool_or(samples: &[GaugeSample], path: &[&str]) -> Value {
    let mut out: Option<bool> = None;
    for s in samples {
        if let Some(b) = boolean(at(&s.metrics, path)) {
            out = Some(out.unwrap_or(false) || b);
        }
    }
    out.map_or(Value::Null, Value::Bool)
}

/// `(array_agg(path ORDER BY timestamp DESC) FILTER (WHERE path IS NOT
/// NULL))[1]`: the latest sample in which the key is present. A JSON null is
/// present (it is not SQL NULL).
fn latest_present(samples: &[GaugeSample], path: &[&str]) -> Value {
    samples
        .iter()
        .rev()
        .find_map(|s| at(&s.metrics, path))
        .cloned()
        .unwrap_or(Value::Null)
}

// ---------------------------------------------------------------------------
// GET /api/v1/analytics/worker-metrics
// ---------------------------------------------------------------------------

/// Port of `queen.get_worker_metrics_timeseries_v1` (019_worker_metrics.sql:1094),
/// the body of `GET /api/v1/analytics/worker-metrics` (handlers/analytics.rs:97).
///
/// Filters ([`WORKER_METRICS_FILTER_KEYS`]): `from`, `to`, `hostname`,
/// `workerId` (cast to integer; not an integer answers the SQL's error) and
/// `queue`, which the SP ignores. `hostname` / `workerId` narrow `timeSeries`
/// only.
///
/// * `timeSeries`: one point per bucket over the rows of every node, newest
///   first; counters summed, `avgEventLoopLagMs` = `ROUND(AVG(..))`,
///   `avgLagMs` = `ROUND(Σ(avg_lag × lag_count) / Σ lag_count)` (0 when no lag
///   was sampled), per-second rates = `ROUND(sum / (bucketMinutes × 60), 2)`,
///   `jobsDone` = push requests (what the Postgres insert writes there).
/// * `workers`: rows of the last 5 minutes by `(hostname, workerId)`, no
///   filter applied.
/// * `queues`: the [`QueueRow`]s in range grouped by queue NAME (the SQL joins
///   `queen.queues` without a tenant predicate, so pass every tenant's rows
///   for parity), `avgLagMs` weighted by `pop_count`, ordered by `popCount`
///   descending (ties by name).
/// * `summary`: the lifetime totals (see the module notes) over every row.
pub fn worker_metrics_json(
    filters: &Map<String, Value>,
    now_us: i64,
    workers: &[WorkerRow],
    queues: &[QueueRow],
) -> Value {
    // Statement order of the SQL: from, to, hostname, workerId, bucket width.
    let (from_us, to_us) = match parse_range(filters, now_us) {
        Ok(r) => r,
        Err(e) => return error_json(e),
    };
    let hostname = filter_text(filters, "hostname");
    let worker_id = match filter_int(filters, "workerId") {
        Ok(v) => v,
        Err(e) => return error_json(e),
    };
    let range = match Range::new(from_us, to_us) {
        Ok(r) => r,
        Err(e) => return error_json(e),
    };
    let rows = dedup_workers(workers);

    let mut buckets: BTreeMap<i64, WorkerAcc> = BTreeMap::new();
    for r in &rows {
        if range.contains(r.at_us)
            && hostname.as_deref().is_none_or(|h| h == r.hostname)
            && worker_id.is_none_or(|w| w == r.worker_id)
        {
            buckets.entry(range.bucket(r.at_us)).or_default().add(r);
        }
    }
    let time_series: Vec<Value> = buckets
        .iter()
        .rev()
        .map(|(bucket, a)| {
            json!({
                "timestamp": ts_s(*bucket),
                "pushMessages": int(a.push_msg),
                "popMessages": int(a.pop_msg),
                "ackMessages": int(a.ack_msg),
                "pushRequests": int(a.push_req),
                "popRequests": int(a.pop_req),
                "ackRequests": int(a.ack_req),
                "jobsDone": int(a.push_req),
                "pushPerSecond": range.rate(a.push_msg),
                "popPerSecond": range.rate(a.pop_msg),
                "ackPerSecond": range.rate(a.ack_msg),
                "avgEventLoopLagMs": a.avg_event_loop(),
                "maxEventLoopLagMs": opt_int(a.el_max),
                "avgFreeSlots": Value::Null,
                "minFreeSlots": Value::Null,
                "dbConnections": Value::Null,
                "avgJobQueueSize": Value::Null,
                "maxJobQueueSize": Value::Null,
                "backoffSize": Value::Null,
                "avgLagMs": if a.lag_count > 0 {
                    int(div_round(a.lag_weighted, a.lag_count))
                } else {
                    int(0)
                },
                "maxLagMs": opt_int(a.lag_max),
                "lagCount": int(a.lag_count),
                "dbErrors": int(a.db_errors),
                "ackSuccess": int(a.ack_success),
                "ackFailed": int(a.ack_failed),
                "dlqCount": int(a.dlq),
            })
        })
        .collect();

    let since = now_us.saturating_sub(5 * US_PER_MIN);
    let mut per_worker: BTreeMap<(&str, i32), WorkerAcc> = BTreeMap::new();
    for r in &rows {
        if r.at_us >= since {
            per_worker
                .entry((r.hostname.as_str(), r.worker_id))
                .or_default()
                .add(r);
        }
    }
    let workers_json: Vec<Value> = per_worker
        .iter()
        .map(|((host, wid), a)| {
            json!({
                "hostname": host,
                "workerId": wid,
                "avgEventLoopLagMs": a.avg_event_loop(),
                "maxEventLoopLagMs": opt_int(a.el_max),
                "freeSlots": Value::Null,
                "dbConnections": Value::Null,
                "jobQueueSize": Value::Null,
                "backoffSize": Value::Null,
                "messagesProcessed": int(a.push_msg + a.ack_msg),
                "lastSeen": a.last_at_us.map_or(Value::Null, |t| Value::String(ts_s(t))),
            })
        })
        .collect();

    let mut per_queue: BTreeMap<&str, QueueLagAcc> = BTreeMap::new();
    for q in queues {
        if range.contains(q.bucket_us) {
            per_queue.entry(q.queue.as_str()).or_default().add(q);
        }
    }
    let mut per_queue: Vec<(&str, QueueLagAcc)> = per_queue.into_iter().collect();
    // ORDER BY pop_cnt DESC; the BTreeMap order breaks ties by name.
    per_queue.sort_by(|a, b| b.1.pops.cmp(&a.1.pops));
    let queues_json: Vec<Value> = per_queue
        .iter()
        .map(|(name, a)| {
            json!({
                "queueName": name,
                "popCount": int(a.pops),
                "avgLagMs": if a.pops > 0 {
                    int(div_round(a.lag_weighted, a.pops))
                } else {
                    int(0)
                },
                "maxLagMs": opt_int(a.lag_max),
            })
        })
        .collect();

    let t = Totals::of(&rows);
    json!({
        "timeRange": range.time_range_json(),
        "bucketMinutes": range.bucket_minutes,
        "pointCount": time_series.len(),
        "timeSeries": time_series,
        "workers": workers_json,
        "queues": queues_json,
        "summary": {
            "totalPushMessages": int(t.push_messages),
            "totalPopMessages": int(t.pop_messages),
            "totalAckMessages": int(t.ack_messages),
            "totalPushRequests": int(t.push_requests),
            "totalPopRequests": int(t.pop_requests),
            "totalAckRequests": int(t.ack_requests),
            "totalDbErrors": int(t.db_errors),
            "totalAckFailed": int(t.ack_failed),
            "totalDlq": int(t.dlq),
            "pendingMessages": int(t.push_messages - t.pop_messages),
        },
    })
}

// ---------------------------------------------------------------------------
// GET /api/v1/status
// ---------------------------------------------------------------------------

/// Port of `queen.get_status_v3` (019_worker_metrics.sql:711), the body of
/// `GET /api/v1/status` (handlers/status.rs:106; the handler adds nothing).
///
/// Filters ([`STATUS_FILTER_KEYS`]): `from`, `to`, `queue`, `namespace`, `task`.
///
/// * `throughput`, newest first. With no queue / namespace / task filter, one
///   point per bucket of the worker rows (every node): counters summed,
///   `avgLagMs` = `Σ(avg_lag × lag_count) / Σ lag_count` UNROUNDED (0 when no
///   lag was sampled). With any of those filters, one point per bucket of the
///   selected queues' [`QueueRow`] / [`ChurnRow`]s (`ingested` = pushed,
///   `processed` = acked, `avgLagMs` weighted by pops), with the worker
///   signals (`avgEventLoopLagMs`, `maxEventLoopLagMs`, `dbErrors`,
///   `dlqCount`) attached by bucket and null where no worker row landed.
///   Either way `queenCpuUserPct` / `queenCpuSysPct` (`ROUND(AVG(avg / 100), 2)`),
///   `queenRssMb` (`ROUND(AVG(avg / 1 MiB), 1)`) and `dbPool*` are the plain
///   average over every system row of the bucket (all replicas), null where
///   none landed; `minFreeSlots` is null (not measured).
/// * `workers`: worker rows of the last 2 minutes by `(hostname, workerId)`.
/// * `queues`, `messages.pending` / `processing`, `leases` and
///   `deadLetterQueue.currentMessages` / `affectedPartitions` / `topErrors`
///   come from `state`; `messages.total` / `completed` / `failed` /
///   `deadLetter` / `requests` / `batchEfficiency`, `errors` and
///   `deadLetterQueue.totalMessages` are the lifetime totals of the worker rows.
/// * `statsAge`: seconds since the newest worker row (Postgres: since the
///   summary's last trigger update), -1 when there is none. Postgres never
///   reaches -1 (the summary row is seeded at schema creation), so an empty
///   raft cell reads -1 where a fresh Postgres cell reads its age.
pub fn status_json(
    filters: &Map<String, Value>,
    now_us: i64,
    workers: &[WorkerRow],
    systems: &[SystemRow],
    state: &StatusState,
) -> Value {
    let range = match Range::resolve(filters, now_us) {
        Ok(r) => r,
        Err(e) => return error_json(e),
    };
    let queue = filter_text(filters, "queue");
    let namespace = filter_text(filters, "namespace");
    let task = filter_text(filters, "task");
    let rows = dedup_workers(workers);

    // wm: worker signals per bucket, never scoped to a queue (019:789, :854).
    let mut wm: BTreeMap<i64, WorkerAcc> = BTreeMap::new();
    for r in &rows {
        if range.contains(r.at_us) {
            wm.entry(range.bucket(r.at_us)).or_default().add(r);
        }
    }
    // sm: CPU / RSS / pool gauges per bucket across replicas (019:802, :875).
    let mut sm: BTreeMap<i64, SystemAcc> = BTreeMap::new();
    for row in dedup_systems(systems) {
        if range.contains(row.at_us) {
            let metrics: Value = serde_json::from_str(&row.metrics_json).unwrap_or(Value::Null);
            sm.entry(range.bucket(row.at_us)).or_default().add(&metrics);
        }
    }

    let queue_scoped = queue.is_some() || namespace.is_some() || task.is_some();
    let queue_selected = |q: &StatusQueue| {
        queue.as_deref().is_none_or(|v| q.name == v)
            && namespace
                .as_deref()
                .is_none_or(|v| q.namespace.as_deref() == Some(v))
            && task.as_deref().is_none_or(|v| q.task.as_deref() == Some(v))
    };

    let throughput: Vec<Value> = if queue_scoped {
        // qlm (019:761): the selected queues' ops counters, JOIN queen.queues.
        let catalog: HashMap<(&str, &str), &StatusQueue> = state
            .queues
            .iter()
            .map(|q| ((q.tenant.as_str(), q.name.as_str()), q))
            .collect();
        let selected = |tenant: &str, name: &str| {
            catalog
                .get(&(tenant, name))
                .is_some_and(|q| queue_selected(q))
        };
        let mut qlm: BTreeMap<i64, QueueOpsAcc> = BTreeMap::new();
        for r in &state.queue_rows {
            if range.contains(r.bucket_us) && selected(&r.tenant, &r.queue) {
                qlm.entry(range.bucket(r.bucket_us)).or_default().add(r);
            }
        }
        for c in &state.churn_rows {
            if range.contains(c.bucket_us) && selected(&c.tenant, &c.queue) {
                qlm.entry(range.bucket(c.bucket_us))
                    .or_default()
                    .add_churn();
            }
        }
        qlm.iter()
            .rev()
            .map(|(bucket, q)| {
                let w = wm.get(bucket);
                let s = sm.get(bucket);
                json!({
                    "timestamp": ts_s(*bucket),
                    "ingested": int(q.push_msg),
                    "processed": int(q.ack_msg),
                    "popMessages": int(q.pop_msg),
                    "ingestedPerSecond": range.rate(q.push_msg),
                    "processedPerSecond": range.rate(q.ack_msg),
                    "popPerSecond": range.rate(q.pop_msg),
                    "avgLagMs": if q.pop_msg > 0 {
                        float(ratio(q.lag_weighted, q.pop_msg))
                    } else {
                        int(0)
                    },
                    "maxLagMs": opt_int(q.lag_max),
                    "avgEventLoopLagMs": w.map_or(Value::Null, WorkerAcc::avg_event_loop),
                    "maxEventLoopLagMs": opt_int(w.and_then(|w| w.el_max)),
                    "minFreeSlots": Value::Null,
                    "dbErrors": w.map_or(Value::Null, |w| int(w.db_errors)),
                    "ackFailed": int(q.ack_failed),
                    "dlqCount": w.map_or(Value::Null, |w| int(w.dlq)),
                    "queenCpuUserPct": rounded(s.and_then(|s| s.cpu_user.get()), 2),
                    "queenCpuSysPct": rounded(s.and_then(|s| s.cpu_sys.get()), 2),
                    "queenRssMb": rounded(s.and_then(|s| s.rss_mb.get()), 1),
                    "dbPoolActive": rounded(s.and_then(|s| s.pool_active.get()), 1),
                    "dbPoolIdle": rounded(s.and_then(|s| s.pool_idle.get()), 1),
                    "dbPoolSize": rounded(s.and_then(|s| s.pool_size.get()), 1),
                })
            })
            .collect()
    } else {
        wm.iter()
            .rev()
            .map(|(bucket, w)| {
                let s = sm.get(bucket);
                json!({
                    "timestamp": ts_s(*bucket),
                    "ingested": int(w.push_msg),
                    "processed": int(w.ack_msg),
                    "popMessages": int(w.pop_msg),
                    "ingestedPerSecond": range.rate(w.push_msg),
                    "processedPerSecond": range.rate(w.ack_msg),
                    "popPerSecond": range.rate(w.pop_msg),
                    "avgLagMs": if w.lag_count > 0 {
                        float(ratio(w.lag_weighted, w.lag_count))
                    } else {
                        int(0)
                    },
                    "maxLagMs": opt_int(w.lag_max),
                    "avgEventLoopLagMs": w.avg_event_loop(),
                    "maxEventLoopLagMs": opt_int(w.el_max),
                    "minFreeSlots": Value::Null,
                    "dbErrors": int(w.db_errors),
                    "ackFailed": int(w.ack_failed),
                    "dlqCount": int(w.dlq),
                    "queenCpuUserPct": rounded(s.and_then(|s| s.cpu_user.get()), 2),
                    "queenCpuSysPct": rounded(s.and_then(|s| s.cpu_sys.get()), 2),
                    "queenRssMb": rounded(s.and_then(|s| s.rss_mb.get()), 1),
                    "dbPoolActive": rounded(s.and_then(|s| s.pool_active.get()), 1),
                    "dbPoolIdle": rounded(s.and_then(|s| s.pool_idle.get()), 1),
                    "dbPoolSize": rounded(s.and_then(|s| s.pool_size.get()), 1),
                })
            })
            .collect()
    };

    // Worker health, last 2 minutes (019:925).
    let since = now_us.saturating_sub(2 * US_PER_MIN);
    let mut per_worker: BTreeMap<(&str, i32), WorkerAcc> = BTreeMap::new();
    for r in &rows {
        if r.at_us >= since {
            per_worker
                .entry((r.hostname.as_str(), r.worker_id))
                .or_default()
                .add(r);
        }
    }
    let workers_json: Vec<Value> = per_worker
        .iter()
        .map(|((host, wid), a)| {
            json!({
                "hostname": host,
                "workerId": wid,
                "avgEventLoopLagMs": a.avg_event_loop(),
                "maxEventLoopLagMs": opt_int(a.el_max),
                "freeSlots": Value::Null,
                "dbConnections": Value::Null,
                "jobQueueSize": Value::Null,
                "messagesProcessed": int(a.push_msg + a.ack_msg),
            })
        })
        .collect();

    // Active queues (019:954): filtered, only queues holding retained frames.
    // jsonb_agg has no ORDER BY there; name order is the deterministic choice.
    let mut listed: Vec<&StatusQueue> = state
        .queues
        .iter()
        .filter(|q| q.total_messages > 0 && queue_selected(q))
        .collect();
    listed.sort_by(|a, b| (&a.name, &a.tenant, &a.id).cmp(&(&b.name, &b.tenant, &b.id)));
    let queues_json: Vec<Value> = listed
        .iter()
        .map(|q| {
            json!({
                "id": q.id,
                "name": q.name,
                "namespace": q.namespace,
                "task": q.task,
                "partitions": q.partitions,
                "totalConsumed": q.completed_messages,
            })
        })
        .collect();

    let t = Totals::of(&rows);
    // Live watermarks over every queue, unfiltered (019:979).
    let pending: i128 = state
        .queues
        .iter()
        .map(|q| i128::from(q.pending_messages))
        .sum();
    let processing: i128 = state
        .queues
        .iter()
        .map(|q| i128::from(q.processing_messages))
        .sum();
    let messages = json!({
        "total": int(t.push_messages),
        "pending": int((pending - processing).max(0)),
        "processing": int(processing),
        "completed": int(t.ack_success),
        "failed": int(t.ack_failed),
        "deadLetter": int(t.dlq),
        "requests": {
            "push": int(t.push_requests),
            "pop": int(t.pop_requests),
            "ack": int(t.ack_requests),
        },
        "batchEfficiency": {
            "push": per_request(t.push_messages, t.push_requests),
            "pop": per_request(t.pop_messages, t.pop_requests),
            "ack": per_request(t.ack_messages, t.ack_requests),
        },
    });

    // Leases (019:1026): live leases only; totalAcked is a structural 0.
    let live: Vec<&StatusLease> = state
        .leases
        .iter()
        .filter(|l| l.lease_expires_us.is_some_and(|e| e > now_us))
        .collect();
    let leased_partitions: BTreeSet<u64> = live.iter().map(|l| l.partition_id).collect();
    let batch: i128 = live
        .iter()
        .map(|l| {
            let end = i128::from(l.batch_end.unwrap_or(l.committed));
            (end - i128::from(l.committed)).max(0)
        })
        .sum();
    let leases = json!({
        "active": live.len(),
        "partitionsWithLeases": leased_partitions.len(),
        "totalBatchSize": int(batch),
        "totalAcked": 0,
    });

    // DLQ (019:1044): top five errors by row count.
    let mut by_error: BTreeMap<&str, i128> = BTreeMap::new();
    for e in &state.dlq.errors {
        if e.count > 0 {
            *by_error
                .entry(e.error.as_deref().unwrap_or("unknown"))
                .or_default() += i128::from(e.count);
        }
    }
    let mut top: Vec<(&str, i128)> = by_error.into_iter().collect();
    top.sort_by(|a, b| b.1.cmp(&a.1));
    top.truncate(5);
    let dead_letter_queue = json!({
        "totalMessages": int(t.dlq),
        "currentMessages": state.dlq.current_messages,
        "affectedPartitions": state.dlq.affected_partitions,
        "topErrors": top
            .iter()
            .map(|(error, count)| json!({"error": error, "count": int(*count)}))
            .collect::<Vec<Value>>(),
    });

    let stats_age = match t.last_at_us {
        Some(last) => int(div_round(
            i128::from(now_us) - i128::from(last),
            i128::from(US_PER_SEC),
        )),
        None => int(-1),
    };

    json!({
        "timeRange": range.time_range_json(),
        "bucketMinutes": range.bucket_minutes,
        "pointCount": throughput.len(),
        "throughput": throughput,
        "queues": queues_json,
        "messages": messages,
        "leases": leases,
        "deadLetterQueue": dead_letter_queue,
        "workers": workers_json,
        "errors": {
            "dbErrors": int(t.db_errors),
            "ackFailed": int(t.ack_failed),
            "dlqMessages": int(t.dlq),
        },
        "statsAge": stats_age,
    })
}

// ---------------------------------------------------------------------------
// Accumulators
// ---------------------------------------------------------------------------

/// The per-group sums of `queen.worker_metrics` the two worker-row SPs take.
#[derive(Default)]
struct WorkerAcc {
    rows: i128,
    push_msg: i128,
    pop_msg: i128,
    ack_msg: i128,
    push_req: i128,
    pop_req: i128,
    ack_req: i128,
    el_sum: i128,
    el_max: Option<i32>,
    /// `Σ(avg_lag_ms × lag_count)`.
    lag_weighted: i128,
    lag_count: i128,
    lag_max: Option<i64>,
    db_errors: i128,
    ack_success: i128,
    ack_failed: i128,
    dlq: i128,
    last_at_us: Option<i64>,
}

impl WorkerAcc {
    fn add(&mut self, r: &WorkerRow) {
        self.rows += 1;
        self.push_msg += i128::from(r.push_messages);
        self.pop_msg += i128::from(r.pop_messages);
        self.ack_msg += i128::from(r.ack_messages);
        self.push_req += i128::from(r.push_requests);
        self.pop_req += i128::from(r.pop_requests);
        self.ack_req += i128::from(r.ack_requests);
        self.el_sum += i128::from(r.avg_event_loop_lag_ms);
        self.el_max = Some(
            self.el_max
                .map_or(r.max_event_loop_lag_ms, |m| m.max(r.max_event_loop_lag_ms)),
        );
        self.lag_weighted += i128::from(r.avg_lag_ms) * i128::from(r.lag_count);
        self.lag_count += i128::from(r.lag_count);
        self.lag_max = Some(self.lag_max.map_or(r.max_lag_ms, |m| m.max(r.max_lag_ms)));
        self.db_errors += i128::from(r.db_errors);
        self.ack_success += i128::from(r.ack_success);
        self.ack_failed += i128::from(r.ack_failed);
        self.dlq += i128::from(r.dlq);
        self.last_at_us = Some(self.last_at_us.map_or(r.at_us, |t| t.max(r.at_us)));
    }

    /// `ROUND(AVG(avg_event_loop_lag_ms))`.
    fn avg_event_loop(&self) -> Value {
        if self.rows > 0 {
            int(div_round(self.el_sum, self.rows))
        } else {
            Value::Null
        }
    }
}

/// get_status_v3's per-bucket `queue_lag_metrics` sums (019:761).
#[derive(Default)]
struct QueueOpsAcc {
    pop_msg: i128,
    push_msg: i128,
    /// `Σ(ack_success + ack_failed)`.
    ack_msg: i128,
    ack_failed: i128,
    /// `Σ(avg_lag_ms × pop_count)`.
    lag_weighted: i128,
    lag_max: Option<i64>,
}

impl QueueOpsAcc {
    fn add(&mut self, r: &QueueRow) {
        self.pop_msg += i128::from(r.pop_messages);
        self.push_msg += i128::from(r.push_messages);
        self.ack_msg += i128::from(r.ack_success) + i128::from(r.ack_failed);
        self.ack_failed += i128::from(r.ack_failed);
        self.lag_weighted += i128::from(r.avg_lag_ms) * i128::from(r.pop_messages);
        self.lag_max = Some(self.lag_max.map_or(r.max_lag_ms, |m| m.max(r.max_lag_ms)));
    }

    /// A partition-lifecycle-only row: every counter at its DDL default 0.
    fn add_churn(&mut self) {
        self.lag_max = Some(self.lag_max.map_or(0, |m| m.max(0)));
    }
}

/// get_worker_metrics_timeseries_v1's per-queue lag sums (019:1240).
#[derive(Default)]
struct QueueLagAcc {
    pops: i128,
    /// `Σ(avg_lag_ms × pop_count)`.
    lag_weighted: i128,
    lag_max: Option<i64>,
}

impl QueueLagAcc {
    fn add(&mut self, r: &QueueRow) {
        self.pops += i128::from(r.pop_messages);
        self.lag_weighted += i128::from(r.avg_lag_ms) * i128::from(r.pop_messages);
        self.lag_max = Some(self.lag_max.map_or(r.max_lag_ms, |m| m.max(r.max_lag_ms)));
    }
}

/// get_status_v3's per-bucket `system_metrics` averages (019:802, :875).
#[derive(Default)]
struct SystemAcc {
    cpu_user: Mean,
    cpu_sys: Mean,
    rss_mb: Mean,
    pool_active: Mean,
    pool_idle: Mean,
    pool_size: Mean,
}

impl SystemAcc {
    fn add(&mut self, m: &Value) {
        let avg = |path: &[&str]| numeric(at(m, path).and_then(|l| l.get("avg"))).map(|(v, _)| v);
        self.cpu_user
            .add(avg(&["cpu", "user_us"]).map(|v| v / 100.0));
        self.cpu_sys
            .add(avg(&["cpu", "system_us"]).map(|v| v / 100.0));
        self.rss_mb
            .add(avg(&["memory", "rss_bytes"]).map(|v| v / 1_048_576.0));
        self.pool_active.add(avg(&["database", "pool_active"]));
        self.pool_idle.add(avg(&["database", "pool_idle"]));
        self.pool_size.add(avg(&["database", "pool_size"]));
    }
}

/// SQL `AVG` over the non-NULL values.
#[derive(Default)]
struct Mean {
    sum: f64,
    n: u64,
}

impl Mean {
    fn add(&mut self, v: Option<f64>) {
        if let Some(v) = v {
            self.sum += v;
            self.n += 1;
        }
    }

    fn get(&self) -> Option<f64> {
        (self.n > 0).then(|| self.sum / self.n as f64)
    }
}

/// `queen.worker_metrics_summary` (019_worker_metrics.sql:376) derived from the
/// rows: what its insert trigger would have summed, plus the newest row as its
/// `last_updated_at`.
#[derive(Default)]
struct Totals {
    push_requests: i128,
    pop_requests: i128,
    ack_requests: i128,
    push_messages: i128,
    pop_messages: i128,
    ack_messages: i128,
    ack_success: i128,
    ack_failed: i128,
    db_errors: i128,
    dlq: i128,
    last_at_us: Option<i64>,
}

impl Totals {
    fn of(rows: &[&WorkerRow]) -> Totals {
        let mut t = Totals::default();
        for r in rows {
            t.push_requests += i128::from(r.push_requests);
            t.pop_requests += i128::from(r.pop_requests);
            t.ack_requests += i128::from(r.ack_requests);
            t.push_messages += i128::from(r.push_messages);
            t.pop_messages += i128::from(r.pop_messages);
            t.ack_messages += i128::from(r.ack_messages);
            t.ack_success += i128::from(r.ack_success);
            t.ack_failed += i128::from(r.ack_failed);
            t.db_errors += i128::from(r.db_errors);
            t.dlq += i128::from(r.dlq);
            t.last_at_us = Some(t.last_at_us.map_or(r.at_us, |l| l.max(r.at_us)));
        }
        t
    }
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

/// `worker_metrics` is UNIQUE(hostname, worker_id, pid, bucket_time) and its
/// insert is ON CONFLICT DO NOTHING: the first row of a key is the one kept.
fn dedup_workers(rows: &[WorkerRow]) -> Vec<&WorkerRow> {
    let mut seen: BTreeMap<(&str, i32, i32, i64), &WorkerRow> = BTreeMap::new();
    for r in rows {
        seen.entry((r.hostname.as_str(), r.worker_id, r.pid, r.at_us))
            .or_insert(r);
    }
    seen.into_values().collect()
}

/// `system_metrics` is UNIQUE(timestamp, hostname, port, worker_id) and its
/// insert upserts: the last row of a key is the one kept. Sorted by
/// `(hostname, port, worker_id, at_us)`.
fn dedup_systems(rows: &[SystemRow]) -> Vec<&SystemRow> {
    let mut seen: BTreeMap<(&str, i32, &str, i64), &SystemRow> = BTreeMap::new();
    for r in rows {
        seen.insert(
            (r.hostname.as_str(), r.port, r.worker_id.as_str(), r.at_us),
            r,
        );
    }
    seen.into_values().collect()
}

// ---------------------------------------------------------------------------
// Filters, range and buckets
// ---------------------------------------------------------------------------

/// The resolved `from` / `to` and the bucket width the SQL derives from them.
struct Range {
    from_us: i64,
    to_us: i64,
    bucket_minutes: i64,
}

impl Range {
    fn resolve(filters: &Map<String, Value>, now_us: i64) -> Result<Range, String> {
        let (from_us, to_us) = parse_range(filters, now_us)?;
        Range::new(from_us, to_us)
    }

    fn new(from_us: i64, to_us: i64) -> Result<Range, String> {
        Ok(Range {
            from_us,
            to_us,
            bucket_minutes: bucket_minutes_for(from_us, to_us)?,
        })
    }

    /// `t >= v_from_ts AND t <= v_to_ts`.
    fn contains(&self, t: i64) -> bool {
        t >= self.from_us && t <= self.to_us
    }

    fn bucket(&self, t: i64) -> i64 {
        bucket_start(t, self.bucket_minutes)
    }

    /// `ROUND(n::numeric / (v_bucket_minutes * 60), 2)`.
    fn rate(&self, n: i128) -> Value {
        let secs = i128::from(self.bucket_minutes) * 60;
        float(div_round(n * 100, secs) as f64 / 100.0)
    }

    fn time_range_json(&self) -> Value {
        json!({"from": ts_ms(self.from_us), "to": ts_ms(self.to_us)})
    }
}

/// `COALESCE((p_filters->>'from')::timestamptz, NOW() - INTERVAL '1 hour')`
/// and `COALESCE((p_filters->>'to')::timestamptz, NOW())`.
fn parse_range(filters: &Map<String, Value>, now_us: i64) -> Result<(i64, i64), String> {
    let from_us = match filter_text(filters, "from") {
        Some(s) => parse_ts_us(&s).ok_or_else(|| timestamptz_error(&s))?,
        None => now_us.saturating_sub(3_600 * US_PER_SEC),
    };
    let to_us = match filter_text(filters, "to") {
        Some(s) => parse_ts_us(&s).ok_or_else(|| timestamptz_error(&s))?,
        None => now_us,
    };
    Ok((from_us, to_us))
}

fn timestamptz_error(s: &str) -> String {
    format!("invalid input syntax for type timestamp with time zone: \"{s}\"")
}

/// `v_duration_minutes := EXTRACT(EPOCH FROM (to - from)) / 60` (numeric
/// assigned to INTEGER: rounded half away from zero, out of range raises) and
/// the width CASE of every SP here.
fn bucket_minutes_for(from_us: i64, to_us: i64) -> Result<i64, String> {
    let minutes = div_round(
        i128::from(to_us) - i128::from(from_us),
        i128::from(US_PER_MIN),
    );
    if minutes > i128::from(i32::MAX) || minutes < i128::from(i32::MIN) {
        return Err("integer out of range".to_string());
    }
    Ok(match minutes {
        m if m <= 60 => 1,
        m if m <= 360 => 5,
        m if m <= 1_440 => 15,
        m if m <= 10_080 => 60,
        _ => 360,
    })
}

/// `date_trunc('minute', t) - (EXTRACT(minute FROM t)::integer % width) *
/// INTERVAL '1 minute'` in UTC. The minute of the hour never reaches 360, so a
/// 360-minute width truncates to the hour, exactly like the SQL.
fn bucket_start(t_us: i64, bucket_minutes: i64) -> i64 {
    let minute = trunc_us(t_us, US_PER_MIN);
    let minute_of_hour = minute.div_euclid(US_PER_MIN).rem_euclid(60);
    minute - (minute_of_hour % bucket_minutes.max(1)) * US_PER_MIN
}

/// `p_filters->>'key'`.
fn filter_text(filters: &Map<String, Value>, key: &str) -> Option<String> {
    match filters.get(key)? {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// `(p_filters->>'key')::integer`, with Postgres' error text when the value is
/// not a (32-bit) integer.
fn filter_int(filters: &Map<String, Value>, key: &str) -> Result<Option<i32>, String> {
    let Some(s) = filter_text(filters, key) else {
        return Ok(None);
    };
    let t = s.trim();
    let digits = t.strip_prefix(['+', '-']).unwrap_or(t);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("invalid input syntax for type integer: \"{s}\""));
    }
    t.parse::<i32>()
        .map(Some)
        .map_err(|_| format!("value \"{s}\" is out of range for type integer"))
}

// ---------------------------------------------------------------------------
// JSON and numeric helpers
// ---------------------------------------------------------------------------

/// The `{"error": ...}` body of the SP-raises path.
fn error_json(message: String) -> Value {
    json!({ "error": message })
}

/// `to_char(t AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`:
/// milliseconds, truncated.
fn ts_ms(us: i64) -> String {
    let iso = iso_us(us);
    match iso.rfind('.').and_then(|dot| iso.get(..dot + 4)) {
        Some(head) => format!("{head}Z"),
        None => iso,
    }
}

/// `to_char(t AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')`.
fn ts_s(us: i64) -> String {
    let iso = iso_us(us);
    match iso.rfind('.').and_then(|dot| iso.get(..dot)) {
        Some(head) => format!("{head}Z"),
        None => iso,
    }
}

/// Walk `path` through nested objects (`->` returns NULL on anything else).
fn at<'a>(v: &'a Value, path: &[&str]) -> Option<&'a Value> {
    path.iter()
        .try_fold(v, |node, key| node.as_object()?.get(*key))
}

/// `(... ->> 'k')::numeric`: a JSON number, or a string holding one. The raw
/// value is kept for the outputs that pass a stored number through unchanged
/// (`MIN`, `MAX`, `last`). Anything else, JSON null included, is NULL.
fn numeric(v: Option<&Value>) -> Option<(f64, Value)> {
    match v? {
        Value::Number(n) => n.as_f64().map(|f| (f, Value::Number(n.clone()))),
        Value::String(s) => {
            let f = s.trim().parse::<f64>().ok().filter(|f| f.is_finite())?;
            Number::from_f64(f).map(|n| (f, Value::Number(n)))
        }
        _ => None,
    }
}

/// `(... ->> 'k')::boolean`: a JSON bool, a boolean literal string, or 0 / 1.
fn boolean(v: Option<&Value>) -> Option<bool> {
    match v? {
        Value::Bool(b) => Some(*b),
        Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "t" | "true" | "y" | "yes" | "on" | "1" => Some(true),
            "f" | "false" | "n" | "no" | "off" | "0" => Some(false),
            _ => None,
        },
        Value::Number(n) => match n.as_i64() {
            Some(1) => Some(true),
            Some(0) => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// `num / den` rounded half away from zero (numeric `ROUND`, and numeric to
/// integer casts). 0 when `den` is 0; the callers guard that case themselves.
fn div_round(num: i128, den: i128) -> i128 {
    if den == 0 {
        return 0;
    }
    let (n, d) = if den < 0 { (-num, -den) } else { (num, den) };
    let q = n / d;
    let r = (n % d).unsigned_abs();
    // |r| >= d - |r|  <=>  the remainder is at least half the divisor.
    if r >= d.unsigned_abs() - r {
        q + n.signum()
    } else {
        q
    }
}

/// `num / den` as a float (the SQL's unrounded numeric division). One
/// correctly rounded division while both operands are exact in an f64, the
/// exact integer part first beyond that. 0 when `den` is 0.
fn ratio(num: i128, den: i128) -> f64 {
    const EXACT: u128 = 1 << 53;
    if den == 0 {
        return 0.0;
    }
    if num.unsigned_abs() <= EXACT && den.unsigned_abs() <= EXACT {
        return num as f64 / den as f64;
    }
    (num / den) as f64 + (num % den) as f64 / den as f64
}

/// `CASE WHEN requests > 0 THEN ROUND(messages::numeric / requests, 2) ELSE 0 END`.
fn per_request(messages: i128, requests: i128) -> Value {
    if requests > 0 {
        float(div_round(messages * 100, requests) as f64 / 100.0)
    } else {
        int(0)
    }
}

/// `ROUND(v, places)`, NULL staying NULL.
fn rounded(v: Option<f64>, places: i32) -> Value {
    match v {
        Some(v) => {
            let scale = 10f64.powi(places);
            float((v * scale).round() / scale)
        }
        None => Value::Null,
    }
}

fn int(v: i128) -> Value {
    match i64::try_from(v) {
        Ok(v) => Value::from(v),
        Err(_) => float(v as f64),
    }
}

fn opt_int<T: Into<i128>>(v: Option<T>) -> Value {
    v.map_or(Value::Null, |v| int(v.into()))
}

fn float(v: f64) -> Value {
    Number::from_f64(v).map_or(Value::Null, Value::Number)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 60 * US_PER_MIN;

    fn t(s: &str) -> i64 {
        parse_ts_us(s).expect("test timestamp")
    }

    fn filters(pairs: &[(&str, &str)]) -> Map<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
            .collect()
    }

    fn gauge(v: f64) -> Value {
        json!({"avg": v, "min": v, "max": v, "last": v})
    }

    /// The raft collector's metrics JSON: syscollect.rs's shape without the
    /// `database` family, plus `raft`.
    fn raft_metrics(cpu_user: f64, rss: f64, inflight: f64) -> Value {
        let zero = || gauge(0.0);
        json!({
            "cpu": {"user_us": gauge(cpu_user), "system_us": gauge(cpu_user / 2.0)},
            "memory": {"rss_bytes": gauge(rss), "virtual_bytes": zero()},
            "registries": {"response": zero()},
            "uptime_seconds": 42,
            "shared_state": {
                "enabled": false,
                "sidecar_ops": {
                    "push": {"count": zero(), "latency_us": zero(), "items": zero()},
                    "pop": {"count": zero(), "latency_us": zero(), "items": zero()},
                    "ack": {"count": zero(), "latency_us": zero(), "items": zero()},
                },
                "queue_backoff": {
                    "queues_with_backoff": zero(),
                    "total_backed_off_groups": zero(),
                    "avg_interval_ms": zero(),
                },
                "queue_backoff_summary": [],
                "queue_config_cache": {"size": zero(), "hits": zero(), "misses": zero()},
                "consumer_presence": {
                    "queues_tracked": zero(),
                    "servers_tracked": zero(),
                    "total_registrations": zero(),
                },
                "server_health": {"alive": gauge(1.0), "dead": zero()},
                "transport": {"sent": zero(), "received": zero(), "dropped": zero()},
            },
            "raft": {
                "inflight": gauge(inflight),
                "applied_lag": zero(),
                "log_bytes": gauge(1_048_576.0),
                "log_files": gauge(4.0),
                "map_used_pct": gauge(0.5),
            },
        })
    }

    fn sys(host: &str, at_us: i64, sample_count: i32, metrics: Value) -> SystemRow {
        SystemRow {
            at_us,
            hostname: host.to_string(),
            port: 6632,
            worker_id: "worker-0".to_string(),
            sample_count,
            metrics_json: metrics.to_string(),
        }
    }

    fn wrk(host: &str, at_us: i64) -> WorkerRow {
        WorkerRow {
            at_us,
            hostname: host.to_string(),
            pid: 7,
            ..Default::default()
        }
    }

    fn qrow(queue: &str, bucket_us: i64) -> QueueRow {
        QueueRow {
            bucket_us,
            tenant: "t1".to_string(),
            queue: queue.to_string(),
            ..Default::default()
        }
    }

    fn squeue(name: &str, namespace: Option<&str>) -> StatusQueue {
        StatusQueue {
            id: format!("id-{name}"),
            tenant: "t1".to_string(),
            name: name.to_string(),
            namespace: namespace.map(str::to_string),
            task: None,
            ..Default::default()
        }
    }

    // ------------------------------------------------------------- empty

    #[test]
    fn empty_system_metrics_is_the_sp_answer_for_an_empty_table() {
        let now = t("2026-09-24T10:30:15.123456Z");
        let v = system_metrics_json(&Map::new(), now, &[]);
        assert_eq!(
            v,
            json!({
                "timeRange": {"from": "2026-09-24T09:30:15.123Z", "to": "2026-09-24T10:30:15.123Z"},
                "replicas": [],
                "replicaCount": 0,
                "bucketMinutes": 1,
                "pointCount": 0,
            })
        );
    }

    #[test]
    fn empty_worker_metrics_is_the_sp_answer_for_an_empty_table() {
        let now = t("2026-09-24T10:30:00Z");
        let v = worker_metrics_json(&Map::new(), now, &[], &[]);
        assert_eq!(
            v,
            json!({
                "timeRange": {"from": "2026-09-24T09:30:00.000Z", "to": "2026-09-24T10:30:00.000Z"},
                "bucketMinutes": 1,
                "pointCount": 0,
                "timeSeries": [],
                "workers": [],
                "queues": [],
                "summary": {
                    "totalPushMessages": 0, "totalPopMessages": 0, "totalAckMessages": 0,
                    "totalPushRequests": 0, "totalPopRequests": 0, "totalAckRequests": 0,
                    "totalDbErrors": 0, "totalAckFailed": 0, "totalDlq": 0,
                    "pendingMessages": 0,
                },
            })
        );
    }

    #[test]
    fn empty_status_is_the_sp_answer_for_empty_tables() {
        let now = t("2026-09-24T10:30:00Z");
        let v = status_json(&Map::new(), now, &[], &[], &StatusState::default());
        assert_eq!(
            v,
            json!({
                "timeRange": {"from": "2026-09-24T09:30:00.000Z", "to": "2026-09-24T10:30:00.000Z"},
                "bucketMinutes": 1,
                "pointCount": 0,
                "throughput": [],
                "queues": [],
                "messages": {
                    "total": 0, "pending": 0, "processing": 0, "completed": 0,
                    "failed": 0, "deadLetter": 0,
                    "requests": {"push": 0, "pop": 0, "ack": 0},
                    "batchEfficiency": {"push": 0, "pop": 0, "ack": 0},
                },
                "leases": {"active": 0, "partitionsWithLeases": 0, "totalBatchSize": 0, "totalAcked": 0},
                "deadLetterQueue": {
                    "totalMessages": 0, "currentMessages": 0, "affectedPartitions": 0, "topErrors": [],
                },
                "workers": [],
                "errors": {"dbErrors": 0, "ackFailed": 0, "dlqMessages": 0},
                "statsAge": -1,
            })
        );
    }

    // ------------------------------------------------------------- range and buckets

    #[test]
    fn bucket_width_follows_the_rounded_range_length() {
        let from = t("2026-09-24T00:00:00Z");
        let width = |minutes: i64, extra_s: i64| {
            bucket_minutes_for(from, from + minutes * US_PER_MIN + extra_s * US_PER_SEC)
        };
        assert_eq!(width(60, 0), Ok(1));
        assert_eq!(width(60, 29), Ok(1)); // 60.48 rounds to 60
        assert_eq!(width(60, 30), Ok(5)); // 60.5 rounds half away to 61
        assert_eq!(width(360, 0), Ok(5));
        assert_eq!(width(361, 0), Ok(15));
        assert_eq!(width(1_440, 0), Ok(15));
        assert_eq!(width(1_441, 0), Ok(60));
        assert_eq!(width(10_080, 0), Ok(60));
        assert_eq!(width(10_081, 0), Ok(360));
        assert_eq!(width(-500, 0), Ok(1)); // from after to
        assert!(bucket_minutes_for(i64::MIN, i64::MAX).is_err());
    }

    #[test]
    fn buckets_align_to_the_minute_of_the_hour() {
        let ts = t("2026-09-24T10:47:31.5Z");
        assert_eq!(bucket_start(ts, 1), t("2026-09-24T10:47:00Z"));
        assert_eq!(bucket_start(ts, 5), t("2026-09-24T10:45:00Z"));
        assert_eq!(bucket_start(ts, 15), t("2026-09-24T10:45:00Z"));
        assert_eq!(bucket_start(ts, 60), t("2026-09-24T10:00:00Z"));
        // minute % 360 == minute: the "6 h" width is an hourly truncation.
        assert_eq!(bucket_start(ts, 360), t("2026-09-24T10:00:00Z"));
        assert_eq!(
            bucket_start(t("1969-12-31T23:59:30Z"), 5),
            t("1969-12-31T23:55:00Z")
        );
    }

    #[test]
    fn timestamp_masks() {
        let us = t("2026-09-24T10:47:31.987654Z");
        assert_eq!(ts_ms(us), "2026-09-24T10:47:31.987Z");
        assert_eq!(ts_s(us), "2026-09-24T10:47:31Z");
    }

    #[test]
    fn explicit_range_is_echoed_and_picks_the_width() {
        let now = t("2026-09-25T00:00:00Z");
        let f = filters(&[
            ("from", "2026-09-24T00:00:00.000Z"),
            ("to", "2026-09-24T06:00:00.000Z"),
        ]);
        let v = worker_metrics_json(&f, now, &[], &[]);
        assert_eq!(v["bucketMinutes"], 5);
        assert_eq!(v["timeRange"]["from"], "2026-09-24T00:00:00.000Z");
        assert_eq!(v["timeRange"]["to"], "2026-09-24T06:00:00.000Z");
        let f = filters(&[
            ("from", "2026-09-17T00:00:00Z"),
            ("to", "2026-09-24T06:00:00Z"),
        ]);
        assert_eq!(
            status_json(&f, now, &[], &[], &StatusState::default())["bucketMinutes"],
            360
        );
    }

    #[test]
    fn unparseable_filters_answer_the_sp_error() {
        let now = t("2026-09-24T10:00:00Z");
        let v = system_metrics_json(&filters(&[("from", "yesterday-ish")]), now, &[]);
        assert_eq!(
            v,
            json!({"error": "invalid input syntax for type timestamp with time zone: \"yesterday-ish\""})
        );
        let v = worker_metrics_json(&filters(&[("workerId", "abc")]), now, &[], &[]);
        assert_eq!(
            v,
            json!({"error": "invalid input syntax for type integer: \"abc\""})
        );
        let v = worker_metrics_json(&filters(&[("workerId", "99999999999")]), now, &[], &[]);
        assert_eq!(
            v,
            json!({"error": "value \"99999999999\" is out of range for type integer"})
        );
        assert!(status_json(
            &filters(&[("to", "")]),
            now,
            &[],
            &[],
            &StatusState::default()
        )
        .get("error")
        .is_some());
        // A JSON null filter is SQL NULL: the default applies.
        let mut f = Map::new();
        f.insert("from".to_string(), Value::Null);
        assert_eq!(
            system_metrics_json(&f, now, &[])["timeRange"]["from"],
            "2026-09-24T09:00:00.000Z"
        );
    }

    // ------------------------------------------------------------- system metrics

    #[test]
    fn system_metrics_weights_avg_by_sample_count_and_keeps_min_max_last() {
        let now = t("2026-09-24T10:30:00Z");
        let a = sys(
            "n1",
            t("2026-09-24T10:05:10Z"),
            60,
            raft_metrics(1000.0, 100.0, 2.0),
        );
        let b = sys(
            "n1",
            t("2026-09-24T10:05:50Z"),
            20,
            raft_metrics(3000.0, 300.0, 6.0),
        );
        let v = system_metrics_json(&Map::new(), now, &[b, a]);
        assert_eq!(v["replicaCount"], 1);
        assert_eq!(v["pointCount"], 1);
        let point = &v["replicas"][0]["timeSeries"][0];
        assert_eq!(point["timestamp"], "2026-09-24T10:05:00.000Z");
        assert_eq!(point["sampleCount"], 80);
        let cpu = &point["metrics"]["cpu"]["user_us"];
        assert_eq!(cpu["avg"], json!((1000.0 * 60.0 + 3000.0 * 20.0) / 80.0));
        assert_eq!(cpu["min"], json!(1000.0));
        assert_eq!(cpu["max"], json!(3000.0));
        assert_eq!(cpu["last"], json!(3000.0));
        let raft = &point["metrics"]["raft"];
        assert_eq!(
            raft["inflight"]["avg"],
            json!((2.0 * 60.0 + 6.0 * 20.0) / 80.0)
        );
        assert_eq!(raft["inflight"]["last"], json!(6.0));
        assert_eq!(raft["map_used_pct"]["max"], json!(0.5));
        // Named paths the raft rows lack: null leaves, never zeros.
        let null_leaf = json!({"avg": null, "min": null, "max": null, "last": null});
        assert_eq!(point["metrics"]["database"]["pool_active"], null_leaf);
        assert_eq!(
            point["metrics"]["threadpool"]["db"]["queue_size"],
            null_leaf
        );
        assert!(point["metrics"].get("uptime_seconds").is_none());
        let shared = &point["metrics"]["shared_state"];
        assert_eq!(shared["enabled"], json!(false));
        assert_eq!(shared["server_health"]["alive"], json!(1.0));
        assert_eq!(shared["sidecar_ops"]["push"]["count"], json!(0.0));
        assert_eq!(shared["queue_backoff_summary"], json!([]));
        let replica = &v["replicas"][0];
        assert_eq!(replica["hostname"], "n1");
        assert_eq!(replica["port"], 6632);
        assert_eq!(replica["workerId"], "worker-0");
    }

    #[test]
    fn system_metrics_last_is_the_latest_row_even_when_null() {
        let now = t("2026-09-24T10:30:00Z");
        let old = sys(
            "n1",
            t("2026-09-24T10:05:00Z"),
            60,
            raft_metrics(1000.0, 1.0, 1.0),
        );
        let mut newer = raft_metrics(2000.0, 1.0, 1.0);
        if let Some(raft) = newer.get_mut("raft").and_then(Value::as_object_mut) {
            raft.remove("inflight");
        }
        let new = sys("n1", t("2026-09-24T10:05:30Z"), 60, newer);
        let v = system_metrics_json(&Map::new(), now, &[old, new]);
        let inflight = &v["replicas"][0]["timeSeries"][0]["metrics"]["raft"]["inflight"];
        // avg's denominator still counts the row without the leaf.
        assert_eq!(inflight["avg"], json!(0.5));
        assert_eq!(inflight["min"], json!(1.0));
        assert_eq!(inflight["last"], Value::Null);
    }

    #[test]
    fn system_metrics_groups_two_replicas_and_filters() {
        let now = t("2026-09-24T10:30:00Z");
        let rows = vec![
            sys(
                "n2",
                t("2026-09-24T10:01:00Z"),
                60,
                raft_metrics(10.0, 1.0, 0.0),
            ),
            sys(
                "n1",
                t("2026-09-24T10:02:00Z"),
                60,
                raft_metrics(20.0, 1.0, 0.0),
            ),
            sys(
                "n1",
                t("2026-09-24T10:01:00Z"),
                60,
                raft_metrics(30.0, 1.0, 0.0),
            ),
            // Outside the default hour: dropped.
            sys(
                "n1",
                t("2026-09-24T09:29:59Z"),
                60,
                raft_metrics(40.0, 1.0, 0.0),
            ),
            // Same key as the 10:01 n1 row: the later write replaces it.
            sys(
                "n1",
                t("2026-09-24T10:01:00Z"),
                60,
                raft_metrics(35.0, 1.0, 0.0),
            ),
        ];
        let v = system_metrics_json(&Map::new(), now, &rows);
        assert_eq!(v["replicaCount"], 2);
        assert_eq!(v["pointCount"], 3);
        assert_eq!(v["replicas"][0]["hostname"], "n1");
        let n1 = &v["replicas"][0]["timeSeries"];
        assert_eq!(n1[0]["timestamp"], "2026-09-24T10:01:00.000Z");
        assert_eq!(n1[0]["metrics"]["cpu"]["user_us"]["avg"], json!(35.0));
        assert_eq!(n1[1]["timestamp"], "2026-09-24T10:02:00.000Z");
        assert_eq!(v["replicas"][1]["hostname"], "n2");

        let only_n2 = system_metrics_json(&filters(&[("hostname", "n2")]), now, &rows);
        assert_eq!(only_n2["replicaCount"], 1);
        assert_eq!(only_n2["replicas"][0]["hostname"], "n2");
        let wrong_worker = system_metrics_json(&filters(&[("workerId", "0")]), now, &rows);
        assert_eq!(wrong_worker["replicas"], json!([]));
        let right_worker = system_metrics_json(&filters(&[("workerId", "worker-0")]), now, &rows);
        assert_eq!(right_worker["pointCount"], 3);
    }

    #[test]
    fn system_metrics_rolls_up_by_width_and_survives_bad_json() {
        let now = t("2026-09-24T12:00:00Z");
        let f = filters(&[
            ("from", "2026-09-24T06:00:00Z"),
            ("to", "2026-09-24T12:00:00Z"),
        ]);
        let mut bad = sys("n1", t("2026-09-24T10:06:00Z"), 60, json!({}));
        bad.metrics_json = "{not json".to_string();
        let rows = vec![
            sys(
                "n1",
                t("2026-09-24T10:05:00Z"),
                60,
                raft_metrics(100.0, 1.0, 0.0),
            ),
            bad,
            sys(
                "n1",
                t("2026-09-24T10:10:00Z"),
                60,
                raft_metrics(100.0, 1.0, 0.0),
            ),
        ];
        let v = system_metrics_json(&f, now, &rows);
        assert_eq!(v["bucketMinutes"], 5);
        let series = &v["replicas"][0]["timeSeries"];
        assert_eq!(series.as_array().map(Vec::len), Some(2));
        assert_eq!(series[0]["timestamp"], "2026-09-24T10:05:00.000Z");
        assert_eq!(series[0]["sampleCount"], 120);
        assert_eq!(series[0]["metrics"]["cpu"]["user_us"]["avg"], json!(50.0));
        assert_eq!(series[0]["metrics"]["cpu"]["user_us"]["last"], Value::Null);
        assert_eq!(
            series[0]["metrics"]["shared_state"]["queue_backoff_summary"],
            json!([])
        );
    }

    // ------------------------------------------------------------- worker metrics

    fn busy(host: &str, at_us: i64) -> WorkerRow {
        WorkerRow {
            push_requests: 10,
            push_messages: 100,
            pop_requests: 5,
            pop_messages: 50,
            ack_requests: 4,
            ack_messages: 40,
            ack_success: 38,
            ack_failed: 2,
            dlq: 1,
            db_errors: 0,
            avg_event_loop_lag_ms: 3,
            max_event_loop_lag_ms: 9,
            avg_lag_ms: 100,
            max_lag_ms: 400,
            lag_count: 1,
            ..wrk(host, at_us)
        }
    }

    #[test]
    fn worker_metrics_sums_two_hosts_per_bucket_with_rates_and_weighted_lag() {
        let now = t("2026-09-24T10:30:00Z");
        let a = busy("n1", t("2026-09-24T10:10:00Z"));
        let b = WorkerRow {
            avg_event_loop_lag_ms: 4,
            max_event_loop_lag_ms: 20,
            avg_lag_ms: 201,
            max_lag_ms: 900,
            lag_count: 2,
            ..busy("n2", t("2026-09-24T10:10:00Z"))
        };
        let c = busy("n1", t("2026-09-24T10:11:00Z"));
        let v = worker_metrics_json(&Map::new(), now, &[a, b, c], &[]);
        assert_eq!(v["pointCount"], 2);
        let ts = &v["timeSeries"];
        // Newest first.
        assert_eq!(ts[0]["timestamp"], "2026-09-24T10:11:00Z");
        let p = &ts[1];
        assert_eq!(p["timestamp"], "2026-09-24T10:10:00Z");
        assert_eq!(p["pushMessages"], 200);
        assert_eq!(p["popMessages"], 100);
        assert_eq!(p["ackMessages"], 80);
        assert_eq!(p["pushRequests"], 20);
        assert_eq!(p["jobsDone"], 20);
        assert_eq!(p["pushPerSecond"], json!(3.33)); // 200 / 60
        assert_eq!(p["popPerSecond"], json!(1.67)); // 100 / 60 rounds up
        assert_eq!(p["ackPerSecond"], json!(1.33));
        assert_eq!(p["avgEventLoopLagMs"], 4); // ROUND(3.5) away from zero
        assert_eq!(p["maxEventLoopLagMs"], 20);
        // ROUND((100×1 + 201×2) / 3) = ROUND(167.33)
        assert_eq!(p["avgLagMs"], 167);
        assert_eq!(p["maxLagMs"], 900);
        assert_eq!(p["lagCount"], 3);
        assert_eq!(p["ackSuccess"], 76);
        assert_eq!(p["ackFailed"], 4);
        assert_eq!(p["dlqCount"], 2);
        assert_eq!(p["dbErrors"], 0);
        for k in [
            "avgFreeSlots",
            "minFreeSlots",
            "dbConnections",
            "avgJobQueueSize",
            "maxJobQueueSize",
            "backoffSize",
        ] {
            assert_eq!(p[k], Value::Null, "{k}");
        }
        assert_eq!(v["summary"]["totalPushMessages"], 300);
        assert_eq!(v["summary"]["totalPopMessages"], 150);
        assert_eq!(v["summary"]["pendingMessages"], 150);
        assert_eq!(v["summary"]["totalDlq"], 3);
    }

    #[test]
    fn worker_metrics_lag_without_samples_is_zero_and_rates_use_the_width() {
        let now = t("2026-09-24T12:00:00Z");
        let f = filters(&[
            ("from", "2026-09-24T00:00:00Z"),
            ("to", "2026-09-24T12:00:00Z"),
        ]);
        let mut a = busy("n1", t("2026-09-24T10:14:59Z"));
        a.lag_count = 0;
        let v = worker_metrics_json(&f, now, &[a], &[]);
        assert_eq!(v["bucketMinutes"], 15);
        let p = &v["timeSeries"][0];
        assert_eq!(p["timestamp"], "2026-09-24T10:00:00Z");
        assert_eq!(p["avgLagMs"], 0);
        assert_eq!(p["pushPerSecond"], json!(0.11)); // 100 / 900
    }

    #[test]
    fn worker_metrics_filters_narrow_the_series_only() {
        let now = t("2026-09-24T10:30:00Z");
        let rows = vec![
            busy("n1", t("2026-09-24T10:27:00Z")),
            WorkerRow {
                worker_id: 1,
                ..busy("n1", t("2026-09-24T10:27:00Z"))
            },
            busy("n2", t("2026-09-24T10:28:00Z")),
            // Duplicate key: the first insert wins, the second is dropped.
            WorkerRow {
                push_messages: 999_999,
                ..busy("n2", t("2026-09-24T10:28:00Z"))
            },
        ];
        let f = filters(&[("hostname", "n1"), ("workerId", "1"), ("queue", "ignored")]);
        let v = worker_metrics_json(&f, now, &rows, &[]);
        assert_eq!(v["pointCount"], 1);
        assert_eq!(v["timeSeries"][0]["pushMessages"], 100);
        // workers: last 5 minutes, unfiltered, ordered by (hostname, workerId).
        let w = &v["workers"];
        assert_eq!(w.as_array().map(Vec::len), Some(3));
        assert_eq!(
            (w[0]["hostname"].clone(), w[0]["workerId"].clone()),
            (json!("n1"), json!(0))
        );
        assert_eq!(
            (w[1]["hostname"].clone(), w[1]["workerId"].clone()),
            (json!("n1"), json!(1))
        );
        assert_eq!(w[2]["hostname"], "n2");
        assert_eq!(w[2]["messagesProcessed"], 140);
        assert_eq!(w[2]["lastSeen"], "2026-09-24T10:28:00Z");
        assert_eq!(w[2]["freeSlots"], Value::Null);
        assert_eq!(w[2]["backoffSize"], Value::Null);
        // The summary ignores every filter.
        assert_eq!(v["summary"]["totalPushMessages"], 300);
        // Too old for the workers list.
        let v = worker_metrics_json(&Map::new(), now + 10 * US_PER_MIN, &rows, &[]);
        assert_eq!(v["workers"], json!([]));
    }

    #[test]
    fn worker_metrics_queues_weight_lag_by_pops_and_sort_by_pops() {
        let now = t("2026-09-24T10:30:00Z");
        let q = |name: &str, tenant: &str, at: &str, pops: i64, avg: i64, max: i64| QueueRow {
            tenant: tenant.to_string(),
            pop_messages: pops,
            avg_lag_ms: avg,
            max_lag_ms: max,
            ..qrow(name, t(at))
        };
        let rows = vec![
            q("orders", "t1", "2026-09-24T10:01:00Z", 10, 100, 150),
            q("orders", "t2", "2026-09-24T10:02:00Z", 30, 201, 900),
            q("idle", "t1", "2026-09-24T10:02:00Z", 0, 0, 0),
            q("mails", "t1", "2026-09-24T10:03:00Z", 40, 5, 7),
            q("orders", "t1", "2026-09-24T08:00:00Z", 1_000, 1, 1), // out of range
        ];
        let v = worker_metrics_json(&Map::new(), now, &[], &rows);
        assert_eq!(
            v["queues"],
            json!([
                // Tie at 40 pops: name order.
                {"queueName": "mails", "popCount": 40, "avgLagMs": 5, "maxLagMs": 7},
                // Grouped by name across tenants; ROUND((100×10 + 201×30) / 40) = 176.
                {"queueName": "orders", "popCount": 40, "avgLagMs": 176, "maxLagMs": 900},
                {"queueName": "idle", "popCount": 0, "avgLagMs": 0, "maxLagMs": 0},
            ])
        );
    }

    // ------------------------------------------------------------- status

    #[test]
    fn status_system_wide_throughput_joins_cpu_by_bucket() {
        let now = t("2026-09-24T10:30:00Z");
        let workers = vec![
            busy("n1", t("2026-09-24T10:10:00Z")),
            WorkerRow {
                avg_lag_ms: 201,
                lag_count: 2,
                ..busy("n2", t("2026-09-24T10:10:00Z"))
            },
            WorkerRow {
                lag_count: 0,
                ..busy("n1", t("2026-09-24T10:11:00Z"))
            },
        ];
        let systems = vec![
            sys(
                "n1",
                t("2026-09-24T10:10:30Z"),
                60,
                raft_metrics(1000.0, 104_857_600.0, 0.0),
            ),
            sys(
                "n2",
                t("2026-09-24T10:10:40Z"),
                60,
                raft_metrics(3000.0, 209_715_200.0, 0.0),
            ),
            // A bucket without worker rows is dropped by the LEFT JOIN.
            sys(
                "n1",
                t("2026-09-24T10:20:00Z"),
                60,
                raft_metrics(1.0, 1.0, 0.0),
            ),
        ];
        let v = status_json(
            &Map::new(),
            now,
            &workers,
            &systems,
            &StatusState::default(),
        );
        assert_eq!(v["pointCount"], 2);
        let newest = &v["throughput"][0];
        assert_eq!(newest["timestamp"], "2026-09-24T10:11:00Z");
        assert_eq!(newest["avgLagMs"], 0);
        assert_eq!(newest["queenCpuUserPct"], Value::Null);
        let p = &v["throughput"][1];
        assert_eq!(p["ingested"], 200);
        assert_eq!(p["processed"], 80);
        assert_eq!(p["popMessages"], 100);
        assert_eq!(p["ingestedPerSecond"], json!(3.33));
        // Unrounded: (100×1 + 201×2) / 3.
        assert_eq!(p["avgLagMs"], json!(502.0 / 3.0));
        assert_eq!(p["maxLagMs"], 400);
        assert_eq!(p["avgEventLoopLagMs"], 3);
        assert_eq!(p["dlqCount"], 2);
        assert_eq!(p["ackFailed"], 4);
        assert_eq!(p["minFreeSlots"], Value::Null);
        // Plain average across the two replicas: (10% + 30%) / 2.
        assert_eq!(p["queenCpuUserPct"], json!(20.0));
        assert_eq!(p["queenCpuSysPct"], json!(10.0));
        assert_eq!(p["queenRssMb"], json!(150.0));
        assert_eq!(p["dbPoolActive"], Value::Null);
        assert_eq!(p["dbPoolSize"], Value::Null);
    }

    #[test]
    fn status_workers_totals_efficiency_and_stats_age() {
        let now = t("2026-09-24T10:30:00Z");
        let workers = vec![
            busy("n2", t("2026-09-24T10:29:10Z")),
            busy("n1", t("2026-09-24T10:28:30Z")),
            busy("n1", t("2026-09-24T10:27:00Z")), // older than 2 minutes
            WorkerRow {
                push_requests: 3,
                ..busy("n3", t("2026-09-24T09:00:00Z"))
            },
        ];
        let v = status_json(&Map::new(), now, &workers, &[], &StatusState::default());
        assert_eq!(
            v["workers"],
            json!([
                {"hostname": "n1", "workerId": 0, "avgEventLoopLagMs": 3, "maxEventLoopLagMs": 9,
                 "freeSlots": null, "dbConnections": null, "jobQueueSize": null, "messagesProcessed": 140},
                {"hostname": "n2", "workerId": 0, "avgEventLoopLagMs": 3, "maxEventLoopLagMs": 9,
                 "freeSlots": null, "dbConnections": null, "jobQueueSize": null, "messagesProcessed": 140},
            ])
        );
        // Lifetime totals: every row, in range or not.
        let m = &v["messages"];
        assert_eq!(m["total"], 400);
        assert_eq!(m["completed"], 152);
        assert_eq!(m["failed"], 8);
        assert_eq!(m["deadLetter"], 4);
        assert_eq!(m["requests"], json!({"push": 33, "pop": 20, "ack": 16}));
        // 400 / 33 = 12.1212..; 200 / 20; 160 / 16.
        assert_eq!(
            m["batchEfficiency"],
            json!({"push": 12.12, "pop": 10.0, "ack": 10.0})
        );
        assert_eq!(
            v["errors"],
            json!({"dbErrors": 0, "ackFailed": 8, "dlqMessages": 4})
        );
        assert_eq!(v["deadLetterQueue"]["totalMessages"], 4);
        // 50 s since the newest row.
        assert_eq!(v["statsAge"], 50);
    }

    #[test]
    fn status_queue_scope_uses_queue_rows_catalog_and_churn() {
        let now = t("2026-09-24T10:30:00Z");
        let mut state = StatusState {
            queues: vec![
                StatusQueue {
                    total_messages: 10,
                    completed_messages: 7,
                    partitions: 2,
                    pending_messages: 3,
                    processing_messages: 1,
                    ..squeue("orders", Some("shop"))
                },
                StatusQueue {
                    total_messages: 5,
                    pending_messages: 5,
                    ..squeue("mails", None)
                },
                StatusQueue {
                    tenant: "t2".to_string(),
                    ..squeue("orders", Some("other"))
                },
            ],
            ..Default::default()
        };
        state.queue_rows = vec![
            QueueRow {
                push_messages: 120,
                pop_messages: 30,
                ack_success: 20,
                ack_failed: 4,
                avg_lag_ms: 100,
                max_lag_ms: 250,
                ..qrow("orders", t("2026-09-24T10:05:00Z"))
            },
            QueueRow {
                pop_messages: 10,
                avg_lag_ms: 201,
                max_lag_ms: 999,
                ..qrow("orders", t("2026-09-24T10:05:00Z"))
            },
            QueueRow {
                push_messages: 1_000,
                ..qrow("mails", t("2026-09-24T10:05:00Z"))
            },
            QueueRow {
                tenant: "t2".to_string(),
                push_messages: 7,
                ..qrow("orders", t("2026-09-24T10:06:00Z"))
            },
            // No catalog entry: a deleted queue's rows are gone in Postgres.
            QueueRow {
                push_messages: 5,
                ..qrow("ghost", t("2026-09-24T10:05:00Z"))
            },
        ];
        state.churn_rows = vec![ChurnRow {
            bucket_us: t("2026-09-24T10:07:00Z"),
            tenant: "t1".to_string(),
            queue: "orders".to_string(),
            created: 0,
            deleted: 2,
        }];
        let workers = vec![busy("n1", t("2026-09-24T10:05:00Z"))];
        let systems = vec![sys(
            "n1",
            t("2026-09-24T10:07:00Z"),
            60,
            raft_metrics(500.0, 1.0, 0.0),
        )];

        let v = status_json(
            &filters(&[("namespace", "shop")]),
            now,
            &workers,
            &systems,
            &state,
        );
        assert_eq!(v["pointCount"], 2);
        let churn_only = &v["throughput"][0];
        assert_eq!(churn_only["timestamp"], "2026-09-24T10:07:00Z");
        assert_eq!(churn_only["ingested"], 0);
        assert_eq!(churn_only["avgLagMs"], 0);
        assert_eq!(churn_only["maxLagMs"], 0);
        assert_eq!(churn_only["avgEventLoopLagMs"], Value::Null);
        assert_eq!(churn_only["dbErrors"], Value::Null);
        assert_eq!(churn_only["dlqCount"], Value::Null);
        assert_eq!(churn_only["queenCpuUserPct"], json!(5.0));
        let p = &v["throughput"][1];
        assert_eq!(p["timestamp"], "2026-09-24T10:05:00Z");
        assert_eq!(p["ingested"], 120);
        assert_eq!(p["processed"], 24);
        assert_eq!(p["popMessages"], 40);
        assert_eq!(p["popPerSecond"], json!(0.67));
        // Weighted by pops, unrounded: (100×30 + 201×10) / 40.
        assert_eq!(p["avgLagMs"], json!(125.25));
        assert_eq!(p["maxLagMs"], 999);
        assert_eq!(p["ackFailed"], 4);
        assert_eq!(p["avgEventLoopLagMs"], 3);
        assert_eq!(p["dlqCount"], 1);
        assert_eq!(p["queenCpuUserPct"], Value::Null);
        // The list is filtered too; pending / processing are not.
        assert_eq!(
            v["queues"],
            json!([{"id": "id-orders", "name": "orders", "namespace": "shop", "task": null,
                    "partitions": 2, "totalConsumed": 7}])
        );
        assert_eq!(v["messages"]["pending"], 7); // (3 + 5 + 0) - 1
        assert_eq!(v["messages"]["processing"], 1);

        // By name: both tenants' "orders"; the queue list skips the empty one.
        let v = status_json(&filters(&[("queue", "orders")]), now, &workers, &[], &state);
        assert_eq!(v["pointCount"], 3);
        assert_eq!(v["throughput"][1]["ingested"], 7);
        assert_eq!(v["queues"].as_array().map(Vec::len), Some(1));
        // A NULL namespace matches no namespace filter.
        let v = status_json(&filters(&[("task", "x")]), now, &workers, &[], &state);
        assert_eq!(v["throughput"], json!([]));
        assert_eq!(v["pointCount"], 0);
    }

    #[test]
    fn status_leases_and_dead_letters() {
        let now = t("2026-09-24T10:30:00Z");
        let lease = |pid: u64, committed: i64, end: Option<i64>, exp: Option<i64>| StatusLease {
            partition_id: pid,
            committed,
            batch_end: end,
            lease_expires_us: exp,
        };
        let state = StatusState {
            leases: vec![
                lease(1, 10, Some(20), Some(now + 1)),
                lease(1, 5, Some(8), Some(now + 60 * US_PER_SEC)),
                lease(2, 7, None, Some(now + 1)),
                lease(3, 9, Some(4), Some(now + 1)), // torn read: floored at 0
                lease(4, 0, Some(100), Some(now)),   // expired: lease_expires_at > NOW()
                lease(5, 0, Some(100), None),
            ],
            dlq: StatusDlq {
                current_messages: 42,
                affected_partitions: 3,
                errors: vec![
                    DlqErrorCount {
                        error: Some("timeout".to_string()),
                        count: 10,
                    },
                    DlqErrorCount {
                        error: None,
                        count: 4,
                    },
                    DlqErrorCount {
                        error: Some("unknown".to_string()),
                        count: 3,
                    },
                    DlqErrorCount {
                        error: Some("a".to_string()),
                        count: 2,
                    },
                    DlqErrorCount {
                        error: Some("b".to_string()),
                        count: 2,
                    },
                    DlqErrorCount {
                        error: Some("c".to_string()),
                        count: 2,
                    },
                    DlqErrorCount {
                        error: Some("d".to_string()),
                        count: 1,
                    },
                    DlqErrorCount {
                        error: Some("zero".to_string()),
                        count: 0,
                    },
                ],
            },
            ..Default::default()
        };
        let v = status_json(&Map::new(), now, &[], &[], &state);
        assert_eq!(
            v["leases"],
            json!({"active": 4, "partitionsWithLeases": 3, "totalBatchSize": 13, "totalAcked": 0})
        );
        assert_eq!(
            v["deadLetterQueue"],
            json!({
                "totalMessages": 0,
                "currentMessages": 42,
                "affectedPartitions": 3,
                "topErrors": [
                    {"error": "timeout", "count": 10},
                    {"error": "unknown", "count": 7},
                    {"error": "a", "count": 2},
                    {"error": "b", "count": 2},
                    {"error": "c", "count": 2},
                ],
            })
        );
    }

    #[test]
    fn status_pending_is_floored_at_zero() {
        let now = t("2026-09-24T10:30:00Z");
        let state = StatusState {
            queues: vec![StatusQueue {
                pending_messages: 1,
                processing_messages: 4,
                ..squeue("q", None)
            }],
            ..Default::default()
        };
        let v = status_json(&Map::new(), now, &[], &[], &state);
        assert_eq!(v["messages"]["pending"], 0);
        assert_eq!(v["messages"]["processing"], 4);
        // total_messages 0: not an "active" queue.
        assert_eq!(v["queues"], json!([]));
    }

    #[test]
    fn rounding_is_half_away_from_zero() {
        assert_eq!(div_round(5, 2), 3);
        assert_eq!(div_round(-5, 2), -3);
        assert_eq!(div_round(4, 3), 1);
        assert_eq!(div_round(5, 3), 2);
        assert_eq!(div_round(-4, 3), -1);
        assert_eq!(div_round(7, -2), -4);
        assert_eq!(div_round(0, 9), 0);
        assert_eq!(div_round(1, 0), 0);
        // 18 msgs in an hour bucket: 0.005/s rounds up to 0.01.
        let r = Range {
            from_us: 0,
            to_us: HOUR,
            bucket_minutes: 60,
        };
        assert_eq!(r.rate(18), json!(0.01));
        assert_eq!(r.rate(17), json!(0.0));
        assert_eq!(per_request(1, 8), json!(0.13)); // 0.125
        assert_eq!(rounded(Some(2.345), 1), json!(2.3));
        assert_eq!(ratio(7, 2), 3.5);
    }

    #[test]
    fn filter_values_read_like_the_text_operator() {
        let mut f = Map::new();
        f.insert("hostname".to_string(), json!(7));
        f.insert("workerId".to_string(), json!(" 3 "));
        f.insert("queue".to_string(), Value::Null);
        assert_eq!(filter_text(&f, "hostname"), Some("7".to_string()));
        assert_eq!(filter_int(&f, "workerId"), Ok(Some(3)));
        assert_eq!(filter_text(&f, "queue"), None);
        assert_eq!(filter_text(&f, "missing"), None);
        f.insert("workerId".to_string(), json!(1.5));
        assert!(filter_int(&f, "workerId").is_err());
        // An empty queue filter is a filter (on the empty name), not "no filter".
        let now = t("2026-09-24T10:30:00Z");
        let workers = vec![busy("n1", t("2026-09-24T10:05:00Z"))];
        let v = status_json(
            &filters(&[("queue", "")]),
            now,
            &workers,
            &[],
            &StatusState::default(),
        );
        assert_eq!(v["throughput"], json!([]));
        let v = status_json(&Map::new(), now, &workers, &[], &StatusState::default());
        assert_eq!(v["pointCount"], 1);
    }

    #[test]
    fn gauge_leaves_cast_like_numeric_and_boolean() {
        assert_eq!(numeric(Some(&json!("12.5"))).map(|(v, _)| v), Some(12.5));
        assert_eq!(numeric(Some(&json!(7))).map(|(_, raw)| raw), Some(json!(7)));
        assert_eq!(numeric(Some(&json!(true))), None);
        assert_eq!(numeric(Some(&Value::Null)), None);
        assert_eq!(numeric(None), None);
        assert_eq!(boolean(Some(&json!("t"))), Some(true));
        assert_eq!(boolean(Some(&json!(0))), Some(false));
        assert_eq!(boolean(Some(&json!("maybe"))), None);
    }

    #[test]
    fn shared_state_flags_and_summaries_aggregate_like_the_sql() {
        let now = t("2026-09-24T10:30:00Z");
        let mut on = raft_metrics(1.0, 1.0, 0.0);
        if let Some(shared) = on.get_mut("shared_state").and_then(Value::as_object_mut) {
            shared.insert("enabled".to_string(), json!(true));
            shared.insert("queue_backoff_summary".to_string(), json!([{"queue": "q"}]));
        }
        let mut latest = raft_metrics(1.0, 1.0, 0.0);
        if let Some(shared) = latest
            .get_mut("shared_state")
            .and_then(Value::as_object_mut)
        {
            shared.remove("queue_backoff_summary");
            shared.insert(
                "server_health".to_string(),
                json!({"alive": {"last": "3"}, "dead": 5}),
            );
        }
        let rows = vec![
            sys("n1", t("2026-09-24T10:05:00Z"), 60, on),
            sys("n1", t("2026-09-24T10:05:30Z"), 60, latest),
        ];
        let v = system_metrics_json(&Map::new(), now, &rows);
        let shared = &v["replicas"][0]["timeSeries"][0]["metrics"]["shared_state"];
        assert_eq!(shared["enabled"], json!(true)); // bool_or(true, false)
                                                    // The latest row without the key does not hide the older value.
        assert_eq!(shared["queue_backoff_summary"], json!([{"queue": "q"}]));
        // `->>'last'` of a numeric string casts; of a bare number it is NULL.
        assert_eq!(shared["server_health"]["alive"], json!(3.0));
        assert_eq!(shared["server_health"]["dead"], Value::Null);
        let mut no_flag = raft_metrics(1.0, 1.0, 0.0);
        if let Some(shared) = no_flag
            .get_mut("shared_state")
            .and_then(Value::as_object_mut)
        {
            shared.remove("enabled");
        }
        let v = system_metrics_json(
            &Map::new(),
            now,
            &[sys("n1", t("2026-09-24T10:05:00Z"), 60, no_flag)],
        );
        assert_eq!(
            v["replicas"][0]["timeSeries"][0]["metrics"]["shared_state"]["enabled"],
            Value::Null
        );
    }

    #[test]
    fn status_rejects_a_bad_range_like_the_sp() {
        let now = t("2026-09-24T10:30:00Z");
        let v = status_json(
            &filters(&[("from", "2026-13-01")]),
            now,
            &[],
            &[],
            &StatusState::default(),
        );
        assert_eq!(
            v,
            json!({"error": "invalid input syntax for type timestamp with time zone: \"2026-13-01\""})
        );
    }
}
