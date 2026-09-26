//! The dashboard's metric reads in raft mode (PLAN_RAFT.md D17, §14.6):
//! gather every node's rows ([`crate::rsm::dashboard`]), then serve them
//! through the ported dashboard views.

use serde_json::Value;

use super::{ApiOut, RaftFacade, RsmError};
use crate::rsm::dashboard::store::Rows;

/// What a gather collected: the rows of every node that answered, and the
/// nodes that did not (their rows are simply absent).
pub(super) struct Gathered {
    pub(super) rows: Rows,
    pub(super) missing: Vec<u64>,
}

impl RaftFacade {
    /// Every node's dashboard rows stamped in `[from_us, to_us)`: this node's
    /// from its own store, each peer's over `/raft/v1/local`.
    pub(super) async fn gather_rows(&self, from_us: i64, to_us: i64, totals: bool) -> Gathered {
        let mut rows = crate::rsm::dashboard::store::global()
            .map(|s| s.range_with_totals(from_us, to_us, totals))
            .unwrap_or_default();
        let mut missing = Vec::new();
        if let Some(view) = self.repl.cluster_view() {
            let body = bytes::Bytes::from(
                serde_json::json!({"kind":"rows","fromUs":from_us,"toUs":to_us,"totals":totals}).to_string(),
            );
            let calls = view
                .members
                .iter()
                .filter(|m| m.node_id != view.node_id)
                .map(|m| {
                    let repl = self.repl.clone();
                    let body = body.clone();
                    let (id, addr) = (m.node_id, m.raft_addr.clone());
                    async move {
                        let got = repl
                            .call_peer(&addr, "/raft/v1/local", body, super::admin::PEER_GATHER_TTL)
                            .await
                            .and_then(|b| serde_json::from_slice::<Rows>(&b).map_err(|e| e.to_string()));
                        (id, got)
                    }
                });
            for (id, got) in futures_util::future::join_all(calls).await {
                match got {
                    Ok(peer) => {
                        rows.worker.extend(peer.worker);
                        rows.system.extend(peer.system);
                        rows.queue.extend(peer.queue);
                        rows.parked.extend(peer.parked);
                    }
                    Err(e) => {
                        tracing::debug!(target: "rsm", node = id, error = %e, "dashboard gather: peer did not answer");
                        missing.push(id);
                    }
                }
            }
        }
        Gathered { rows, missing }
    }
}

/// A view's JSON as a 200.
pub(super) fn ok(v: Value) -> Result<ApiOut, RsmError> {
    Ok(ApiOut::json(200, v.to_string()))
}

/// The filter object built the way `filters_from_query`
/// (handlers/mod.rs ≈671) does: the listed keys with a non-empty value.
pub(super) fn filters(query: Option<&str>, keys: &[&str]) -> serde_json::Map<String, Value> {
    let q = super::query_map(query);
    keys.iter()
        .filter_map(|k| {
            q.get(*k)
                .filter(|v| !v.is_empty())
                .map(|v| (k.to_string(), Value::String(v.clone())))
        })
        .collect()
}

/// `[from, to]` of a filter object as the views resolve it (default the last
/// hour), widened by `pad_us` on the left; an unparsable bound falls back to
/// the default (the view itself answers the error).
fn window(f: &serde_json::Map<String, Value>, now: i64, pad_us: i64) -> (i64, i64) {
    use crate::rsm::dashboard::model::parse_ts_us;
    let get = |k: &str| f.get(k).and_then(Value::as_str).and_then(parse_ts_us);
    let to = get("to").unwrap_or(now);
    let from = get("from").unwrap_or(to - 3_600_000_000);
    (from.min(to) - pad_us, to.max(from) + 1)
}

/// A view's answer: a top-level `error` means the input failed to parse,
/// served as a 500.
fn answer(v: Value) -> Result<ApiOut, RsmError> {
    if v.get("error").is_some() {
        return Ok(ApiOut::json(500, v.to_string()));
    }
    ok(v)
}

impl RaftFacade {
    /// `GET /api/v1/analytics/system-metrics` — `get_system_metrics_v1`
    /// over every node's system rows.
    pub(super) async fn api_system_metrics(
        &self,
        _ctx: super::ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        use crate::rsm::dashboard::node_views as nv;
        let f = filters(query, &nv::SYSTEM_METRICS_FILTER_KEYS);
        let now = super::super::wall_micros();
        let (from, to) = window(&f, now, 0);
        let g = self.gather_rows(from, to, false).await;
        answer(nv::system_metrics_json(&f, now, &g.rows.system))
    }

    /// `GET /api/v1/analytics/worker-metrics` — `get_worker_metrics_timeseries_v1`
    /// over every node's worker rows (and, for its per-queue part, the merged
    /// queue rows).
    pub(super) async fn api_worker_metrics(
        &self,
        _ctx: super::ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        use crate::rsm::dashboard::node_views as nv;
        let f = filters(query, &nv::WORKER_METRICS_FILTER_KEYS);
        let now = super::super::wall_micros();
        let (from, to) = window(&f, now, 0);
        let g = self.gather_rows(from, to, true).await;
        let queues = crate::rsm::dashboard::model::merge_queue_rows(g.rows.queue);
        answer(nv::worker_metrics_json(&f, now, &g.rows.worker, &queues))
    }

    /// `GET /api/v1/status` — `get_status_v3`: the node rows of every node
    /// (the window, the last five minutes for the worker list, and each node's
    /// older totals), plus the replicated state ([`RaftFacade::status_state`]).
    pub(super) async fn api_status_v3(
        &self,
        ctx: super::ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        use crate::rsm::dashboard::node_views as nv;
        let f = filters(query, &nv::STATUS_FILTER_KEYS);
        let now = super::super::wall_micros();
        let (from, to) = window(&f, now, 0);
        let (from, to) = (from.min(now - 5 * 60_000_000), to.max(now + 1));
        let g = self.gather_rows(from, to, true).await;
        let mut state = self.status_state(&ctx.tenant).await?;
        state.queue_rows = crate::rsm::dashboard::model::merge_queue_rows(g.rows.queue);
        state.churn_rows = self.local_metrics.churn_rows(&ctx.tenant, from, to);
        let mut v = nv::status_json(&f, now, &g.rows.worker, &g.rows.system, &state);
        if let Some(o) = v.as_object_mut() {
            o.insert("engine".into(), Value::String("raft".into()));
            if !g.missing.is_empty() {
                o.insert("nodesMissing".into(), serde_json::json!(g.missing));
            }
        }
        answer(v)
    }

    /// The replicated half of `get_status_v3`: the tenant's queues with their
    /// counts, the live leases and the dead letters.
    async fn status_state(&self, tenant: &str) -> Result<crate::rsm::dashboard::node_views::StatusState, RsmError> {
        use crate::rsm::dashboard::node_views::{DlqErrorCount, StatusDlq, StatusLease, StatusQueue, StatusState};
        use crate::rsm::store::{Store, TypedReads};
        let snaps = self.queue_snapshots(tenant).await?;
        let queues: Vec<StatusQueue> = snaps
            .iter()
            .map(|q| {
                let n = |p: &str| q.pointer(p).and_then(Value::as_i64).unwrap_or(0);
                let s = |k: &str| {
                    q.get(k)
                        .and_then(Value::as_str)
                        .filter(|v| !v.is_empty())
                        .map(str::to_string)
                };
                StatusQueue {
                    id: s("id").unwrap_or_default(),
                    tenant: tenant.to_string(),
                    name: s("name").unwrap_or_default(),
                    namespace: s("namespace"),
                    task: s("task"),
                    partitions: n("/partitions"),
                    total_messages: n("/messages/total"),
                    // The list reports pending net of what is leased.
                    pending_messages: n("/messages/pending") + n("/messages/processing"),
                    processing_messages: n("/messages/processing"),
                    completed_messages: n("/messages/completed"),
                }
            })
            .collect();
        let store = self.store.clone();
        let t = tenant.to_string();
        let now = super::super::wall_micros();
        let (leases, dlq) = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut leases = Vec::new();
                let mut qs = Vec::new();
                r.scan_queues(&t, usize::MAX, &mut |q, _| {
                    qs.push(q.to_string());
                    true
                })?;
                for q in qs {
                    let mut pids = Vec::new();
                    r.scan_queue_partitions(&t, &q, None, usize::MAX, &mut |pid| {
                        pids.push(pid);
                        true
                    })?;
                    for pid in pids {
                        r.scan_cursors(pid, usize::MAX, &mut |_, c| {
                            if c.lease_expires_at_us.is_some_and(|e| e > now) {
                                leases.push(StatusLease {
                                    partition_id: pid,
                                    committed: c.committed,
                                    batch_end: c.batch_end.map(|e| e as i64),
                                    lease_expires_us: c.lease_expires_at_us,
                                });
                            }
                            true
                        })?;
                    }
                }
                let mut errors: std::collections::BTreeMap<Option<String>, i64> =
                    std::collections::BTreeMap::new();
                let mut pids = std::collections::HashSet::new();
                let rows = super::reads::scan_dlq_rows(r, &t, None, None)?;
                for (_, _, _, row) in &rows {
                    pids.insert(row.pid);
                    let e = Some(row.error.clone()).filter(|e| !e.is_empty());
                    *errors.entry(e).or_default() += 1;
                }
                Ok((
                    leases,
                    StatusDlq {
                        current_messages: rows.len() as i64,
                        affected_partitions: pids.len() as i64,
                        errors: errors
                            .into_iter()
                            .map(|(error, count)| DlqErrorCount { error, count })
                            .collect(),
                    },
                ))
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("status read: {e}")))?
        .map_err(super::read_error)?;
        Ok(StatusState {
            queues,
            queue_rows: Vec::new(),
            churn_rows: Vec::new(),
            leases,
            dlq,
        })
    }
}

/// The raw query as the queue views read it: every key, empty values
/// included (`namespace=` filters on the empty namespace in `get_workload_v1`).
fn raw_filters(query: Option<&str>) -> serde_json::Map<String, Value> {
    super::query_map(query)
        .into_iter()
        .map(|(k, v)| (k, Value::String(v)))
        .collect()
}

impl RaftFacade {
    /// The tenant's queue catalog for the queue views: every queue with its
    /// `queen.queues` columns ([`QueueMeta`]) and its live figures
    /// ([`QueueNow`], the `queen.stats` 'queue' row and the group count).
    async fn queue_catalog(
        &self,
        tenant: &str,
    ) -> Result<
        (
            Vec<crate::rsm::dashboard::queue_views::QueueMeta>,
            Vec<crate::rsm::dashboard::queue_views::QueueNow>,
        ),
        RsmError,
    > {
        use crate::rsm::dashboard::model::parse_ts_us;
        use crate::rsm::dashboard::queue_views::{QueueMeta, QueueNow};
        use crate::rsm::store::{Store, TypedReads};
        let snaps = self.queue_snapshots(tenant).await?;
        let store = self.store.clone();
        let t = tenant.to_string();
        let names: Vec<String> = snaps
            .iter()
            .filter_map(|q| q.get("name").and_then(Value::as_str).map(str::to_string))
            .collect();
        let groups: std::collections::HashMap<String, i64> = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut out = std::collections::HashMap::new();
                for q in names {
                    let mut n = 0i64;
                    r.scan_groups(&t, &q, usize::MAX, &mut |_, g| {
                        if g.meta.partition_name.is_empty() {
                            n += 1;
                        }
                        true
                    })?;
                    out.insert(q, n);
                }
                Ok(out)
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("group count read: {e}")))?
        .map_err(super::read_error)?;
        let mut metas = Vec::with_capacity(snaps.len());
        let mut nows = Vec::with_capacity(snaps.len());
        for q in &snaps {
            let n = |p: &str| q.pointer(p).and_then(Value::as_i64).unwrap_or(0);
            let s = |k: &str| {
                q.get(k)
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
            };
            let meta = QueueMeta {
                name: s("name").unwrap_or_default(),
                namespace: s("namespace"),
                task: s("task"),
                created_us: q
                    .get("createdAt")
                    .and_then(Value::as_str)
                    .and_then(parse_ts_us)
                    .unwrap_or(0),
            };
            let processing = n("/messages/processing");
            nows.push(QueueNow {
                groups: groups.get(&meta.name).copied().unwrap_or(0),
                meta: meta.clone(),
                // The list reports pending net of what is leased.
                pending: n("/messages/pending") + processing,
                processing,
                dead_letter: n("/messages/deadLetter"),
                retained_bytes: n("/retainedBytes"),
                partitions: n("/partitions"),
            });
            metas.push(meta);
        }
        Ok((metas, nows))
    }

    /// Every node's queue rows of the tenant in the window, merged (see
    /// [`crate::rsm::dashboard::model::merge_queue_rows`]), plus this node's
    /// churn rows.
    async fn tenant_queue_rows(
        &self,
        tenant: &str,
        f: &serde_json::Map<String, Value>,
    ) -> (
        Vec<crate::rsm::dashboard::model::QueueRow>,
        Vec<crate::rsm::dashboard::model::ParkedRow>,
        Vec<crate::rsm::dashboard::model::ChurnRow>,
        (i64, i64),
    ) {
        use crate::rsm::dashboard::model::{merge_queue_rows, US_PER_MIN};
        let now = super::super::wall_micros();
        // A bucket is the flush minute: widen by one on each side so a bucket
        // straddling a bound is whole.
        let (from, to) = window(f, now, US_PER_MIN);
        let g = self.gather_rows(from, to + US_PER_MIN, false).await;
        let queue = merge_queue_rows(g.rows.queue.into_iter().filter(|r| r.tenant == tenant));
        let parked = g.rows.parked.into_iter().filter(|r| r.tenant == tenant).collect();
        let churn = self.local_metrics.churn_rows(tenant, from, to + US_PER_MIN);
        (queue, parked, churn, (from, to))
    }

    /// `GET /api/v1/analytics/queue-ops` — `get_queue_ops_v1`.
    pub(super) async fn api_queue_ops_v1(
        &self,
        ctx: super::ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let f = raw_filters(query);
        let (rows, _, churn, _) = self.tenant_queue_rows(&ctx.tenant, &f).await;
        let (metas, _) = self.queue_catalog(&ctx.tenant).await?;
        let now = super::super::wall_micros();
        answer(crate::rsm::dashboard::queue_views::queue_ops_json(
            &f, now, &rows, &churn, &metas,
        ))
    }

    /// `GET /api/v1/analytics/queue-parked-replicas` —
    /// `get_queue_parked_per_replica_v1`.
    pub(super) async fn api_parked_replicas_v1(
        &self,
        ctx: super::ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let f = raw_filters(query);
        let (_, parked, _, _) = self.tenant_queue_rows(&ctx.tenant, &f).await;
        let now = super::super::wall_micros();
        answer(crate::rsm::dashboard::queue_views::parked_replicas_json(
            &f, now, &parked,
        ))
    }

    /// `GET /api/v1/analytics/workload` — `get_workload_v1` (400 on an
    /// unknown `groupBy`).
    pub(super) async fn api_workload_v1(
        &self,
        ctx: super::ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let f = raw_filters(query);
        let (rows, _, churn, (from, to)) = self.tenant_queue_rows(&ctx.tenant, &f).await;
        let (_, nows) = self.queue_catalog(&ctx.tenant).await?;
        let retention = self.local_metrics.retention_rows(&ctx.tenant, from, to + 1);
        let now = super::super::wall_micros();
        match crate::rsm::dashboard::queue_views::workload_json(
            &f, now, &rows, &churn, &retention, &nows,
        ) {
            Ok(v) => answer(v),
            Err(e) => Ok(ApiOut::json(400, serde_json::json!({"error":e}).to_string())),
        }
    }

    /// `GET /api/v1/analytics/retention` — `get_retention_timeseries_v1`.
    pub(super) async fn api_retention_v1(
        &self,
        ctx: super::ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let f = raw_filters(query);
        let now = super::super::wall_micros();
        let (from, to) = window(&f, now, 0);
        let rows = self.local_metrics.retention_rows(&ctx.tenant, from, to);
        let (metas, _) = self.queue_catalog(&ctx.tenant).await?;
        match crate::rsm::dashboard::queue_views::retention_json(&f, now, &rows, &metas) {
            Ok(v) => answer(v),
            Err(e) => Ok(ApiOut::json(400, serde_json::json!({"error":e}).to_string())),
        }
    }

    /// `GET /api/v1/analytics/queue-lag` — `get_queue_lag_v1`: a bare array.
    pub(super) async fn api_queue_lag_v1(
        &self,
        ctx: super::ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let f = raw_filters(query);
        let (rows, _, churn, _) = self.tenant_queue_rows(&ctx.tenant, &f).await;
        let (metas, _) = self.queue_catalog(&ctx.tenant).await?;
        let get = |k: &str| f.get(k).and_then(Value::as_str).filter(|v| !v.is_empty());
        let now = super::super::wall_micros();
        answer(crate::rsm::dashboard::queue_views::queue_lag_json(
            get("from"),
            get("to"),
            get("queue"),
            now,
            &rows,
            &churn,
            &metas,
        ))
    }
}
