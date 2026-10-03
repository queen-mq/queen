//! Phase-2 Queen API over the ordered state machine.

use std::collections::BTreeMap;

use serde_json::{json, Map, Value};

use super::RaftFacade;
use crate::frames::{uuid_bytes_to_string, uuid_string_to_bytes};
use crate::rsm::batcher::{Command, Reply};
use crate::rsm::effect::{
    Effect, GarbageScope, QueueConfig, QuotaGrant, QuotaKind, StreamsQueryRow,
};
use crate::rsm::facade::{ApiOut, ApiReq, ReqCtx, RsmError};
use crate::rsm::planner::EffectsCommand;
use crate::rsm::replicator::{ProposeError, Replicator};
use crate::rsm::store::keys::{self, Counter};
use crate::rsm::store::rows;
use crate::rsm::store::{Keyspace, Reads, Store, StoreError, TypedReads};

mod admin;
mod dash;
mod offsets;
mod positions;
mod reads;
mod stats;
mod streams;

/// The answer to a peer's `/raft/v1/local` gather (D17: node-local data,
/// cluster views gather from every node). `{"kind":"node"}` is this node's
/// block of the Raft view.
pub(super) fn local_gather(
    repl: &crate::rsm::replicator::node::NodeReplicator<crate::rsm::store::heed_store::HeedStore>,
    store: &crate::rsm::store::heed_store::HeedStore,
    body: &[u8],
) -> Result<bytes::Bytes, String> {
    let req: Value = serde_json::from_slice(body).map_err(|e| format!("bad gather: {e}"))?;
    match req.get("kind").and_then(Value::as_str) {
        Some("node") => Ok(bytes::Bytes::from(
            admin::local_node_json(repl, store).to_string(),
        )),
        Some("rows") => {
            let from = req.get("fromUs").and_then(Value::as_i64).unwrap_or(0);
            let to = req.get("toUs").and_then(Value::as_i64).unwrap_or(i64::MAX);
            let totals = req.get("totals").and_then(Value::as_bool).unwrap_or(false);
            let rows = crate::rsm::dashboard::store::global()
                .map(|s| s.range_with_totals(from, to, totals))
                .unwrap_or_default();
            serde_json::to_vec(&rows)
                .map(bytes::Bytes::from)
                .map_err(|e| format!("rows answer: {e}"))
        }
        other => Err(format!("unknown gather kind {other:?}")),
    }
}

impl RaftFacade {
    pub(super) async fn api_impl(&self, ctx: ReqCtx, req: ApiReq) -> Result<ApiOut, RsmError> {
        match (req.method.as_str(), req.path.as_str()) {
            ("POST", "/api/v1/configure") => self.api_configure(ctx, &req.body).await,
            ("GET", "/api/v1/resources/queues") => {
                self.api_list_queues(ctx, req.query.as_deref()).await
            }
            ("GET", "/api/v1/resources/partitions") => {
                self.api_partitions(ctx, req.query.as_deref()).await
            }
            ("GET", "/api/v1/resources/overview") => self.api_overview(ctx).await,
            ("GET", "/api/v1/resources/namespaces") => self.api_labels(ctx, true).await,
            ("GET", "/api/v1/resources/tasks") => self.api_labels(ctx, false).await,
            ("DELETE", "/api/v1/resources/tenant") => {
                self.api_delete_tenant(ctx, req.query.as_deref(), &req.body)
                    .await
            }
            ("POST", "/api/v1/resources/quota")
            | ("POST", "/api/v1/system/quota")
            | ("POST", "/api/v1/system/quotas") => self.api_quota_set(ctx, &req.body).await,
            ("POST", "/api/v1/fetch") => self.api_fetch(ctx, &req.body).await,
            ("POST", "/api/v1/fetch/offsets") => self.api_fetch_offsets(ctx, &req.body).await,
            ("POST", "/api/v1/partitions/changed") => {
                self.api_partitions_changed(ctx, &req.body).await
            }
            ("GET", "/api/v1/messages") => self.api_messages(ctx, req.query.as_deref()).await,
            ("GET", "/api/v1/dlq") => self.api_dlq(ctx, req.query.as_deref()).await,
            ("DELETE", "/api/v1/dlq") => self.api_dlq_purge(ctx, req.query.as_deref()).await,
            ("GET", "/api/v1/consumer-groups") => self.api_groups(ctx, None).await,
            ("POST", "/api/v1/consumer-groups/positions") => {
                self.api_group_positions(ctx, &req.body).await
            }
            ("GET", "/api/v1/consumer-groups/lagging") => {
                self.api_lagging_groups(ctx, req.query.as_deref()).await
            }
            ("POST", "/api/v1/traces") => self.api_trace_record(ctx, &req.body).await,
            ("GET", "/api/v1/traces/names") => {
                self.api_trace_names(ctx, req.query.as_deref()).await
            }
            ("GET", "/api/v1/status") => self.api_status_v3(ctx, req.query.as_deref()).await,
            ("GET", "/api/v1/raft/status") => self.api_raft_status(ctx).await,
            ("GET", "/api/v1/raft/members") => self.api_raft_members(ctx).await,
            ("GET", "/api/v1/raft/liveness") => self.api_raft_liveness(ctx).await,
            ("GET", "/api/v1/status/queues") => self.api_status_queues(ctx).await,
            ("GET", "/api/v1/status/analytics") => self.api_status_analytics(ctx).await,
            ("GET", "/api/v1/analytics/system-metrics") => {
                self.api_system_metrics(ctx, req.query.as_deref()).await
            }
            ("GET", "/api/v1/analytics/worker-metrics") => {
                self.api_worker_metrics(ctx, req.query.as_deref()).await
            }
            ("GET", "/api/v1/analytics/queue-lag") => {
                self.api_queue_lag_v1(ctx, req.query.as_deref()).await
            }
            ("GET", "/api/v1/analytics/queue-ops") => {
                self.api_queue_ops_v1(ctx, req.query.as_deref()).await
            }
            ("GET", "/api/v1/analytics/workload") => {
                self.api_workload_v1(ctx, req.query.as_deref()).await
            }
            ("GET", "/api/v1/analytics/queue-parked-replicas") => {
                self.api_parked_replicas_v1(ctx, req.query.as_deref()).await
            }
            ("GET", "/api/v1/analytics/retention") => {
                self.api_retention_v1(ctx, req.query.as_deref()).await
            }
            ("GET", "/api/v1/analytics/dlq-signatures") => {
                self.api_dlq_signatures(ctx, req.query.as_deref()).await
            }
            ("GET", "/api/v1/analytics/partition-liveness") => {
                self.api_partition_liveness(ctx, req.query.as_deref()).await
            }
            ("GET", "/api/v1/system/kv-timers") => self.api_flag_get("kv_timers_enabled").await,
            ("POST", "/api/v1/system/kv-timers") => {
                self.api_flag_set(ctx, "kv_timers_enabled", &req.body).await
            }
            ("GET", "/api/v1/system/ephemeral") => self.api_flag_get("ephemeral_enabled").await,
            ("POST", "/api/v1/system/ephemeral") => {
                self.api_flag_set(ctx, "ephemeral_enabled", &req.body).await
            }
            ("GET", "/api/v1/system/shared-state") => self.api_shared_state(ctx).await,
            ("POST", "/api/v1/ephemeral/configure") => {
                self.api_ephemeral_configure(ctx, &req.body).await
            }
            ("POST", "/streams/v1/queries") => self.api_streams_register(ctx, &req.body).await,
            ("POST", "/streams/v1/state/get") => self.api_streams_state_get(ctx, &req.body).await,
            ("POST", "/streams/v1/cycle") => self.api_streams_cycle(ctx, &req.body).await,
            _ => {
                if let Some(out) = self.api_dynamic(&ctx, &req).await? {
                    return Ok(out);
                }
                Ok(ApiOut::json(
                    404,
                    json!({"code":"no_such_route","error":"not found","path":req.path}).to_string(),
                ))
            }
        }
    }

    async fn submit_effects(&self, ctx: &ReqCtx, effects: Vec<Effect>) -> Result<(), RsmError> {
        let cmd = Command::Effects(EffectsCommand {
            request_id: ctx.request_id,
            tenant: ctx.tenant.clone(),
            effects,
        });
        match self.submit(ctx, cmd).await? {
            Reply::Done { .. } => Ok(()),
            Reply::Refused(r) if r.retryable => Err(RsmError::Retry { leader_hint: None }),
            Reply::Refused(r) => Err(RsmError::Rejected {
                code: r.code,
                message: r.message,
            }),
            Reply::Retry { hint } => Err(RsmError::Retry {
                leader_hint: hint.map(|h| h.to_string()),
            }),
        }
    }

    /// Wait until this node has applied everything the cluster had committed
    /// when the read began (a no-op on a single node). In a cluster every node
    /// serves its own clients and a write waits only for the node that took it,
    /// so a read from local state without this can miss a write another node
    /// already answered.
    pub(super) async fn linearizable(&self, ctx: &ReqCtx) -> Result<(), RsmError> {
        self.repl
            .read_barrier(ctx.deadline.instant())
            .await
            .map(|_| ())
            .map_err(map_propose)
    }

    /// [`RaftFacade::linearizable`] for a read the leader could answer
    /// instead: `true` once this node has applied everything the cluster had
    /// committed when the read began, `false` when it is still behind that
    /// point a moment later ([`CAUGHT_UP_GRACE`]). A follower's no-wait pop
    /// that looks empty here can ask this before it looks again: behind, it
    /// sends the pop to the leader at once instead of first waiting to catch
    /// up and then again for its claim to apply — the apply lag paid twice.
    /// The leader and a single node are never behind.
    pub(crate) async fn caught_up(&self, ctx: &ReqCtx) -> Result<bool, RsmError> {
        use crate::rsm::replicator::node::NodeReplicator;
        use crate::rsm::replicator::raft::RemoteError;
        let NodeReplicator::Raft(r) = &*self.repl else {
            return Ok(true);
        };
        if !self.offload || self.repl.role().is_leader() {
            self.linearizable(ctx).await?;
            return Ok(true);
        }
        let index = match r.leader_read_index(ctx.deadline.remaining()).await {
            Ok(i) => i,
            Err(RemoteError::NoLeader) => return Err(RsmError::NoLeader),
            Err(RemoteError::Transport(m)) | Err(RemoteError::Lost(m)) => {
                return Err(RsmError::Internal(m))
            }
        };
        let grace = ctx
            .deadline
            .instant()
            .min(std::time::Instant::now() + CAUGHT_UP_GRACE);
        Ok(r.wait_applied(index, grace).await)
    }

    async fn api_configure(&self, ctx: ReqCtx, body: &[u8]) -> Result<ApiOut, RsmError> {
        let root: Value = serde_json::from_slice(body).map_err(|e| rejected("bad_body", e))?;
        let queue = root
            .get("queue")
            .and_then(Value::as_str)
            .ok_or_else(|| reject("bad_request", "queue is required"))?;
        super::super::check_message_key_names(&ctx.tenant, queue, None, None)?;
        let replace = match root.get("mode") {
            None | Some(Value::Null) => false,
            Some(Value::String(s)) if s == "merge" => false,
            Some(Value::String(s)) if s == "replace" => true,
            Some(v) => {
                return Ok(ApiOut::json(
                    400,
                    json!({"error": format!("mode must be \"merge\" or \"replace\", got {v}")})
                        .to_string(),
                ))
            }
        };
        let mut opts = match root.get("options").and_then(Value::as_object) {
            Some(v) => v.clone(),
            None => root.as_object().cloned().unwrap_or_default(),
        };
        for k in ["queue", "options", "mode"] {
            opts.remove(k);
        }
        for k in ["namespace", "task"] {
            if !opts.contains_key(k) {
                if let Some(s) = root
                    .get(k)
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    opts.insert(k.to_string(), Value::String(s.to_string()));
                }
            }
        }

        self.linearizable(&ctx).await?;
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let q = queue.to_string();
        let old = tokio::task::spawn_blocking(move || store.read(|r| r.queue(&tenant, &q)))
            .await
            .map_err(|e| RsmError::Internal(format!("configure read: {e}")))?
            .map_err(read_error)?;
        let now = super::wall_micros();
        let mut cfg = if !replace {
            old.clone().unwrap_or_else(|| configured_defaults(now))
        } else {
            configured_defaults(now)
        };
        if let Some(old) = old {
            cfg.id = old.id;
            cfg.created_at_us = old.created_at_us;
        }
        apply_config_options(&mut cfg, &opts)?;
        self.submit_effects(
            &ctx,
            vec![Effect::QueueUpsert {
                tenant: ctx.tenant.clone(),
                queue: queue.to_string(),
                cfg: cfg.clone(),
            }],
        )
        .await?;
        Ok(ApiOut::json(200, configured_json(queue, &cfg).to_string()))
    }

    async fn api_delete_queue(&self, ctx: ReqCtx, queue: &str) -> Result<ApiOut, RsmError> {
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let q = queue.to_string();
        let (exists, pids) = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let exists = r.queue(&tenant, &q)?.is_some();
                let mut pids = Vec::new();
                r.scan_queue_partitions(&tenant, &q, None, usize::MAX, &mut |pid| {
                    pids.push(pid);
                    true
                })?;
                Ok((exists, pids))
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("delete queue read: {e}")))?
        .map_err(read_error)?;
        if !exists {
            return Ok(ApiOut::json(
                200,
                json!({"deleted":false,"existed":false,"queue":queue,"message":"Queue not found, nothing was deleted"}).to_string(),
            ));
        }
        let mut effects = vec![Effect::QueueDelete {
            tenant: ctx.tenant.clone(),
            queue: queue.to_string(),
        }];
        if !pids.is_empty() {
            effects.push(Effect::GarbageAdd {
                pids: pids.clone(),
                scope: GarbageScope::Queue,
                deleted_at_us: super::wall_micros(),
            });
            effects.push(Effect::DeleteChunk {
                pids: pids.clone(),
                scope: GarbageScope::Queue,
                resume: Vec::new(),
                limit: 1_000,
            });
        }
        self.submit_effects(&ctx, effects).await?;
        self.finish_delete_chunks(&ctx, &pids, GarbageScope::Queue)
            .await?;
        Ok(ApiOut::json(
            200,
            json!({"deleted":true,"existed":true,"queue":queue}).to_string(),
        ))
    }

    /// Finish a pid delete in bounded committed entries. The resume point is
    /// authoritative in each garbage row, so sending an empty resume is safe
    /// after a retry or leader restart. A fresh request id is required for
    /// every chunk: request-id replay intentionally suppresses a duplicate
    /// command.
    async fn finish_delete_chunks(
        &self,
        ctx: &ReqCtx,
        pids: &[u64],
        scope: GarbageScope,
    ) -> Result<(), RsmError> {
        if pids.is_empty() {
            return Ok(());
        }
        loop {
            let store = self.store.clone();
            let check = pids.to_vec();
            let pending = tokio::task::spawn_blocking(move || {
                store.read(|r| {
                    for pid in check {
                        if r.garbage(pid)?.is_some() {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                })
            })
            .await
            .map_err(|e| RsmError::Internal(format!("delete progress read: {e}")))?
            .map_err(read_error)?;
            if !pending {
                return Ok(());
            }
            let chunk_ctx = ReqCtx {
                tenant: ctx.tenant.clone(),
                request_id: crate::util::uuidv7_bytes(),
                deadline: ctx.deadline,
                producer_sub: ctx.producer_sub.clone(),
            };
            self.submit_effects(
                &chunk_ctx,
                vec![Effect::DeleteChunk {
                    pids: pids.to_vec(),
                    scope: scope.clone(),
                    resume: Vec::new(),
                    limit: 1_000,
                }],
            )
            .await?;
        }
    }

    async fn api_delete_tenant(
        &self,
        ctx: ReqCtx,
        query: Option<&str>,
        body: &[u8],
    ) -> Result<ApiOut, RsmError> {
        let body_tenant = if body.iter().all(u8::is_ascii_whitespace) {
            None
        } else {
            serde_json::from_slice::<Value>(body)
                .map_err(|e| rejected("bad_body", e))?
                .get("tenant")
                .and_then(Value::as_str)
                .map(str::to_string)
        };
        let tenant = query_value(query, "tenant")
            .or(body_tenant)
            .unwrap_or_else(|| ctx.tenant.clone());
        if tenant == crate::config::DEFAULT_TENANT {
            return Ok(ApiOut::json(
                400,
                json!({"success":false,"error":"refusing to purge the default tenant"}).to_string(),
            ));
        }
        if crate::config::is_reserved_tenant(&tenant) {
            return Ok(reserved_tenant(&tenant));
        }
        let store = self.store.clone();
        let tenant_read = tenant.clone();
        let pids = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut pids = Vec::new();
                let mut queues = Vec::new();
                r.scan_queues(&tenant_read, usize::MAX, &mut |queue, _| {
                    queues.push(queue.to_string());
                    true
                })?;
                for queue in queues {
                    r.scan_queue_partitions(&tenant_read, &queue, None, usize::MAX, &mut |pid| {
                        pids.push(pid);
                        true
                    })?;
                }
                Ok(pids)
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("tenant purge read: {e}")))?
        .map_err(read_error)?;
        let mut effects = vec![Effect::TenantPurge {
            tenant: tenant.clone(),
        }];
        if !pids.is_empty() {
            effects.push(Effect::GarbageAdd {
                pids: pids.clone(),
                scope: GarbageScope::Tenant,
                deleted_at_us: super::wall_micros(),
            });
            effects.push(Effect::DeleteChunk {
                pids: pids.clone(),
                scope: GarbageScope::Tenant,
                resume: Vec::new(),
                limit: 1_000,
            });
        }
        self.submit_effects(&ctx, effects).await?;
        self.finish_delete_chunks(&ctx, &pids, GarbageScope::Tenant)
            .await?;
        Ok(ApiOut::json(
            200,
            json!({"success":true,"tenant":tenant,"partitionsDeleted":pids.len(),"done":true})
                .to_string(),
        ))
    }

    async fn api_quota_set(&self, ctx: ReqCtx, body: &[u8]) -> Result<ApiOut, RsmError> {
        let value: Value = serde_json::from_slice(body).map_err(|e| rejected("bad_body", e))?;
        let tenant = value
            .get("tenant")
            .or_else(|| value.get("tenantId"))
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .unwrap_or(&ctx.tenant)
            .to_string();
        if crate::config::is_reserved_tenant(&tenant) {
            return Ok(reserved_tenant(&tenant));
        }
        let (kind, kind_name) = match value.get("kind").and_then(Value::as_str) {
            Some("kv") => (QuotaKind::Kv, "kv"),
            Some("ephemeral") => (QuotaKind::Ephemeral, "ephemeral"),
            Some("streams") => (QuotaKind::Streams, "streams"),
            _ => {
                return Ok(ApiOut::json(
                    400,
                    json!({"error":"kind must be kv, ephemeral, or streams"}).to_string(),
                ))
            }
        };
        let i64_field = |camel: &str, snake: &str| -> Result<Option<i64>, RsmError> {
            let Some(v) = value.get(camel).or_else(|| value.get(snake)) else {
                return Ok(None);
            };
            let n = v
                .as_i64()
                .filter(|n| *n >= 0)
                .ok_or_else(|| reject("bad_request", format!("{camel} must be non-negative")))?;
            Ok(Some(n))
        };
        let i32_field = |camel: &str, snake: &str| -> Result<Option<i32>, RsmError> {
            i64_field(camel, snake)?.map_or(Ok(None), |n| {
                i32::try_from(n)
                    .map(Some)
                    .map_err(|_| reject("bad_request", format!("{camel} is too large")))
            })
        };
        let grant = QuotaGrant {
            enabled: value
                .get("enabled")
                .and_then(Value::as_bool)
                .unwrap_or(true),
            max_rows: i64_field("maxRows", "max_rows")?,
            max_bytes: i64_field("maxBytes", "max_bytes")?,
            max_timers: i64_field("maxTimers", "max_timers")?,
            max_timer_horizon_s: i64_field("maxTimerHorizonSeconds", "max_timer_horizon_s")?,
            max_reads_per_sec: i32_field("maxReadsPerSecond", "max_reads_per_sec")?,
            max_writes_per_sec: i32_field("maxWritesPerSecond", "max_writes_per_sec")?,
            max_queues: i32_field("maxQueues", "max_queues")?,
            max_msgs_per_sec: i32_field("maxMessagesPerSecond", "max_msgs_per_sec")?,
            max_queries: i64_field("maxQueries", "max_queries")?,
            updated_at_us: super::wall_micros(),
        };
        self.submit_effects(
            &ctx,
            vec![Effect::QuotaSet {
                kind,
                tenant: tenant.clone(),
                grant: grant.clone(),
            }],
        )
        .await?;
        Ok(ApiOut::json(
            200,
            json!({"success":true,"tenant":tenant,"kind":kind_name,"enabled":grant.enabled,"maxRows":grant.max_rows,"maxBytes":grant.max_bytes,"maxTimers":grant.max_timers,"maxTimerHorizonSeconds":grant.max_timer_horizon_s,"maxReadsPerSecond":grant.max_reads_per_sec,"maxWritesPerSecond":grant.max_writes_per_sec,"maxQueues":grant.max_queues,"maxMessagesPerSecond":grant.max_msgs_per_sec,"maxQueries":grant.max_queries}).to_string(),
        ))
    }

    async fn api_list_queues(&self, ctx: ReqCtx, query: Option<&str>) -> Result<ApiOut, RsmError> {
        if query_value(query, "stats").as_deref() == Some("lanes") {
            return self.api_list_queue_lanes(ctx).await;
        }
        let rows = self.queue_snapshots(&ctx.tenant).await?;
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let (kv_rows, kv_bytes) = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut kv_rows = 0i64;
                let mut kv_bytes = 0i64;
                r.scan_kv_tenant(&tenant, usize::MAX, &mut |_ns, key, row| {
                    kv_rows += 1;
                    kv_bytes += key.len() as i64 + row.value.len() as i64;
                    true
                })?;
                Ok((kv_rows, kv_bytes))
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("kv usage read: {e}")))?
        .map_err(read_error)?;
        Ok(ApiOut::json(
            200,
            json!({"queues": rows, "kvRows":kv_rows, "kvBytes":kv_bytes, "timerRows":0, "timerBytes":0}).to_string(),
        ))
    }

    /// `GET /api/v1/resources/queues?stats=lanes`: every queue's name, id and
    /// LIVE lane count, and nothing else.
    ///
    /// The default list renders the dashboard's statistics, and those cost a
    /// pass over every partition of the tenant — its row, its cursors, its
    /// retained bytes and its segment files — plus a scan of every KV row for
    /// the byte totals: seconds of reads at 500k partitions. A client that only
    /// needs to know which queues exist and how many lanes each has (the Kafka
    /// facade re-reads this every few seconds, `HttpQueen::list_queues` in
    /// queen-kafka) pays one key walk of the queue→partition index instead,
    /// which is exact: the index entry and the partition row are written and
    /// removed together (`create_partition` / `del_partition`). The walk stops
    /// after a hundred times the read budget of keys ([`stats`]); a queue past
    /// it counts the lanes of its last walk (the sampler's), or the keys seen.
    async fn api_list_queue_lanes(&self, ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let known = stats::stats_for(&self.store);
        let rows = tokio::task::spawn_blocking(move || {
            let mut short = false;
            let rows = store.read(|r| {
                let mut queues = Vec::new();
                r.scan_queues(&tenant, usize::MAX, &mut |name, cfg| {
                    queues.push((name.to_string(), cfg));
                    true
                })?;
                let mut left = stats::exact_budget().saturating_mul(100);
                let mut out = Vec::with_capacity(queues.len());
                for (name, cfg) in queues {
                    let cap = usize::try_from(left).unwrap_or(usize::MAX);
                    let mut lanes = 0i64;
                    r.scan_queue_partitions(
                        &tenant,
                        &name,
                        None,
                        cap.saturating_add(1),
                        &mut |_| {
                            lanes += 1;
                            true
                        },
                    )?;
                    if lanes as u64 > left {
                        short = true;
                        lanes = known.lanes(&tenant, &name).unwrap_or(lanes).max(lanes);
                        left = 0;
                    } else {
                        left -= lanes as u64;
                    }
                    out.push(json!({
                        "id": uuid_bytes_to_string(&cfg.id),
                        "name": name,
                        "queue": name,
                        "partitions": lanes,
                        "createdAt": crate::rsm::planner::timers::iso_us(cfg.created_at_us),
                    }));
                }
                Ok(out)
            });
            if short {
                known.wake(None);
            }
            rows
        })
        .await
        .map_err(|e| RsmError::Internal(format!("queue lanes: {e}")))?
        .map_err(read_error)?;
        Ok(ApiOut::json(
            200,
            json!({"queues": rows, "stats": "lanes"}).to_string(),
        ))
    }

    /// `GET /api/v1/resources/queues/:queue/sizes`: the retained bytes of each
    /// partition, by partition name — one counter per partition, where the
    /// queue detail renders a dozen fields and scans every partition's cursors
    /// and segment files (hundreds of MB of JSON at 500k partitions). At most
    /// `QUEEN_DASH_SIZES_MAX` (100,000) partitions, in partition-id order;
    /// `"truncated": true` when the queue has more.
    async fn api_queue_sizes(&self, ctx: ReqCtx, queue: &str) -> Result<ApiOut, RsmError> {
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let q = queue.to_string();
        let max = sizes_max();
        let sizes =
            tokio::task::spawn_blocking(move || store.read(|r| queue_sizes(r, &tenant, &q, max)))
                .await
                .map_err(|e| RsmError::Internal(format!("queue sizes: {e}")))?
                .map_err(read_error)?;
        match sizes {
            Some((sizes, false)) => Ok(ApiOut::json(
                200,
                json!({"queue": queue, "partitions": sizes}).to_string(),
            )),
            Some((sizes, true)) => Ok(ApiOut::json(
                200,
                json!({"queue": queue, "partitions": sizes, "truncated": true}).to_string(),
            )),
            None => Ok(ApiOut::json(
                404,
                json!({"error":"Queue not found"}).to_string(),
            )),
        }
    }

    /// `GET /api/v1/resources/partitions?queue=&limit=`: the partitions
    /// holding the most pending, at most `limit` (default 610, 1..=5000), each
    /// with its queue, pending and leased counts — from the slowest reader, as
    /// the queue detail counts them — and `lagSeconds`, the age of the oldest
    /// message that reader has not consumed (`null` when caught up). The walk
    /// stops after `QUEEN_DASH_SIZES_MAX` partitions (`"truncated": true`).
    /// The dashboard's sunflower asks for it only while it draws one seed per
    /// partition.
    async fn api_partitions(&self, ctx: ReqCtx, query: Option<&str>) -> Result<ApiOut, RsmError> {
        let q = query_map(query);
        let queue = q.get("queue").filter(|v| !v.is_empty()).cloned();
        let limit = q
            .get("limit")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(610)
            .clamp(1, 5000);
        let store = self.store.clone();
        let qlog = self.qlog_reader.clone();
        let reader = self.reader.clone();
        let tenant = ctx.tenant.clone();
        let now = super::wall_micros();
        let max = sizes_max();
        let answer = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                top_partitions(
                    r,
                    qlog.as_ref(),
                    &reader,
                    &tenant,
                    queue.as_deref(),
                    limit,
                    max,
                    now,
                )
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("partitions read: {e}")))?
        .map_err(read_error)?;
        match answer {
            Some(v) => Ok(ApiOut::json(200, v.to_string())),
            None => Ok(ApiOut::json(
                404,
                json!({"error":"Queue not found"}).to_string(),
            )),
        }
    }

    async fn api_get_queue(&self, ctx: ReqCtx, queue: &str) -> Result<ApiOut, RsmError> {
        match self.queue_resource_detail(&ctx.tenant, queue).await? {
            Some(v) => Ok(ApiOut::json(200, v.to_string())),
            None => Ok(ApiOut::json(
                404,
                json!({"error":"Queue not found"}).to_string(),
            )),
        }
    }

    async fn queue_resource_detail(
        &self,
        tenant: &str,
        queue: &str,
    ) -> Result<Option<Value>, RsmError> {
        let store = self.store.clone();
        let tenant = tenant.to_string();
        let queue = queue.to_string();
        let now = super::wall_micros();
        let known = stats::stats_for(&self.store);
        let max = sizes_max();
        tokio::task::spawn_blocking(move || {
            store.read(|r| queue_detail(r, &tenant, &queue, now, max, &known))
        })
        .await
        .map_err(|e| RsmError::Internal(format!("queue detail: {e}")))?
        .map_err(read_error)
    }

    async fn api_queue_depth(
        &self,
        ctx: ReqCtx,
        queue: &str,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        self.linearizable(&ctx).await?;
        let group = query_value(query, "group");
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let queue = queue.to_string();
        let now = super::wall_micros();
        let out = tokio::task::spawn_blocking(move || {
            store.read(|r| depth_json(r, &tenant, &queue, group.as_deref(), now))
        })
        .await
        .map_err(|e| RsmError::Internal(format!("depth read: {e}")))?
        .map_err(read_error)?;
        match out {
            Some(v) => Ok(ApiOut::json(200, v.to_string())),
            None => Ok(ApiOut::json(
                404,
                json!({"error":"Queue not found"}).to_string(),
            )),
        }
    }

    async fn api_overview(&self, ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let stats = self.queue_stats(&ctx.tenant, true).await?;
        let queues = stats.len() as i64;
        let labels = |f: fn(&QueueConfig) -> Option<&String>| {
            stats
                .iter()
                .filter_map(|s| f(&s.cfg))
                .filter(|v| !v.is_empty())
                .collect::<std::collections::BTreeSet<_>>()
                .len() as i64
        };
        let namespaces = labels(|c| c.namespace.as_ref());
        let tasks = labels(|c| c.task.as_ref());
        let (mut partitions, mut total, mut pending, mut processing, mut completed) =
            (0i64, 0i64, 0i64, 0i64, 0i64);
        let mut dead_letter = 0i64;
        for s in &stats {
            let c = SnapCounts::of(s);
            partitions += c.parts;
            total += c.total;
            // As the queue list shows each queue: `pending` net of leased.
            pending += (c.pending - c.processing).max(0);
            processing += c.processing;
            completed += (c.total - c.pending - c.dead_letter).max(0);
            dead_letter += c.dead_letter;
        }
        // Per queue: the oldest unconsumed message's age and the most one
        // cursor has pending; averaged over the queues that have any.
        let mean = |v: &[i64]| {
            if v.is_empty() {
                0
            } else {
                v.iter().sum::<i64>() / v.len() as i64
            }
        };
        let ages: Vec<i64> = stats
            .iter()
            .map(|s| s.fig.lag_age_s)
            .filter(|x| *x > 0)
            .collect();
        let offs: Vec<i64> = stats
            .iter()
            .map(|s| s.fig.lag_pending as i64)
            .filter(|x| *x > 0)
            .collect();
        // How old the oldest figures shown are: 0 when this read walked every
        // queue itself.
        let now = super::wall_micros();
        let stats_age = stats
            .iter()
            .filter(|s| !s.exact && s.fig.at_us > 0)
            .map(|s| (now - s.fig.at_us) / 1_000_000)
            .max()
            .unwrap_or(0)
            .max(0);
        Ok(ApiOut::json(
            200,
            json!({
                "queues":queues,"partitions":partitions,
                "namespaces":namespaces,"tasks":tasks,
                "messages":{"total":total,"pending":pending,"processing":processing,"completed":completed,"failed":0,"deadLetter":dead_letter},
                "lag":{
                    "time":{"avg":mean(&ages),"median":0,"min":0,"max":ages.iter().copied().max().unwrap_or(0)},
                    "offset":{"avg":mean(&offs),"median":0,"min":0,"max":offs.iter().copied().max().unwrap_or(0)}
                },
                "timestamp":reads::iso_ms(now),
                "statsAge":stats_age
            }).to_string(),
        ))
    }

    /// `GET /api/v1/resources/namespaces` and `/tasks`: one row per
    /// non-empty label with its queue and partition counts and messages.
    async fn api_labels(&self, ctx: ReqCtx, namespace: bool) -> Result<ApiOut, RsmError> {
        let rows = self.queue_snapshots(&ctx.tenant).await?;
        let field = if namespace { "namespace" } else { "task" };
        // label -> (queues, partitions, total, pending)
        let mut agg: BTreeMap<String, (i64, i64, i64, i64)> = BTreeMap::new();
        for row in rows {
            let Some(label) = row
                .get(field)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            else {
                continue;
            };
            let e = agg.entry(label.to_string()).or_default();
            e.0 += 1;
            e.1 += row.get("partitions").and_then(Value::as_i64).unwrap_or(0);
            e.2 += row
                .pointer("/messages/total")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            e.3 += row
                .pointer("/messages/pending")
                .and_then(Value::as_i64)
                .unwrap_or(0);
        }
        let values: Vec<Value> = agg
            .into_iter()
            .map(|(label, (queues, partitions, total, pending))| {
                json!({field:label,"queues":queues,"partitions":partitions,"messages":{"total":total,"pending":pending}})
            })
            .collect();
        let key = if namespace { "namespaces" } else { "tasks" };
        Ok(ApiOut::json(200, json!({key:values}).to_string()))
    }

    async fn queue_snapshots(&self, tenant: &str) -> Result<Vec<Value>, RsmError> {
        Ok(self
            .queue_stats(tenant, false)
            .await?
            .into_iter()
            .map(|s| queue_json(&s.name, &s.cfg, &SnapCounts::of(&s)))
            .collect())
    }

    /// Every queue of `tenant` with its figures, newest first, walked within
    /// the read budget and taken from the last walk past it ([`stats`]);
    /// `lag` adds the overview's lag inputs.
    async fn queue_stats(
        &self,
        tenant: &str,
        lag: bool,
    ) -> Result<Vec<stats::QueueStat>, RsmError> {
        let store = self.store.clone();
        let tenant = tenant.to_string();
        let lag_reads = stats::LagReads {
            qlog: self.qlog_reader.clone(),
            reader: self.reader.clone(),
        };
        let known = stats::stats_for(&self.store);
        tokio::task::spawn_blocking(move || {
            let now = super::wall_micros();
            let lag = lag.then_some(&lag_reads);
            let mut out = store.read(|r| {
                stats::tenant_stats(r, &tenant, now, stats::exact_budget(), lag, Some(&known))
            })?;
            if out.iter().any(|s| !s.exact) {
                known.wake(Some(lag_reads.clone()));
            }
            out.sort_by(|a, b| b.cfg.created_at_us.cmp(&a.cfg.created_at_us));
            Ok::<_, StoreError>(out)
        })
        .await
        .map_err(|e| RsmError::Internal(format!("queue snapshot: {e}")))?
        .map_err(read_error)
    }

    async fn api_streams_register(&self, ctx: ReqCtx, body: &[u8]) -> Result<ApiOut, RsmError> {
        self.linearizable(&ctx).await?;
        let root: Value = serde_json::from_slice(body).map_err(|e| rejected("bad_body", e))?;
        let required = |k: &'static str| {
            root.get(k)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| reject("bad_request", format!("{k} is required")))
        };
        let name = required("name")?.to_string();
        let source = required("source_queue")?.to_string();
        let hash = required("config_hash")?.to_string();
        let sink = root
            .get("sink_queue")
            .and_then(Value::as_str)
            .map(str::to_string);
        let reset = root.get("reset").and_then(Value::as_bool).unwrap_or(false);
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let name2 = name.clone();
        let hash_read = hash.clone();
        let existing = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut found = None;
                let mut query_count = 0i64;
                r.scan_streams_queries(&tenant, usize::MAX, &mut |id, row| {
                    query_count += 1;
                    if row.name == name2 {
                        found = Some((id, row));
                    }
                    true
                })?;
                let state = if reset {
                    if let Some((id, old)) = &found {
                        if old.config_hash != hash_read {
                            let prefix = keys::streams_query_state_prefix(id);
                            let mut state = Vec::new();
                            r.scan_raw(
                                Keyspace::StreamsState,
                                &prefix,
                                &prefix,
                                usize::MAX,
                                &mut |k, _| {
                                    if let Some((_, pid, key)) = keys::streams_state_parts(k) {
                                        state.push((pid, key));
                                    }
                                    true
                                },
                            )?;
                            state
                        } else {
                            Vec::new()
                        }
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };
                let quota = r.quota(QuotaKind::Streams, &tenant)?;
                Ok((found, state, quota, query_count))
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("streams register read: {e}")))?
        .map_err(read_error)?;
        let (existing, state, quota, query_count) = existing;
        if let Some((id, old)) = existing {
            if old.config_hash != hash && !reset {
                return Ok(ApiOut::json(409, json!({"success":false,"query_id":uuid_bytes_to_string(&id),"name":name,"error":"config_hash mismatch; retry with reset:true"}).to_string()));
            }
            let now = super::wall_micros();
            let row = StreamsQueryRow {
                name: name.clone(),
                source_queue: source,
                sink_queue: sink,
                config_hash: hash.clone(),
                created_at_us: old.created_at_us,
                updated_at_us: now,
            };
            let did_reset = old.config_hash != hash && reset;
            let mut effects = Vec::with_capacity(state.len() + 1);
            effects.push(Effect::StreamsQueryUpsert {
                query_id: id,
                tenant: ctx.tenant.clone(),
                row,
            });
            effects.extend(
                state
                    .into_iter()
                    .map(|(pid, key)| Effect::StreamsStateDelete {
                        query_id: id,
                        pid,
                        key,
                    }),
            );
            self.submit_effects(&ctx, effects).await?;
            return Ok(ApiOut::json(200, json!({"success":true,"query_id":uuid_bytes_to_string(&id),"name":name,"config_hash":hash,"fresh":false,"reset":did_reset}).to_string()));
        }
        if ctx.tenant != crate::config::DEFAULT_TENANT {
            let Some(grant) = quota.filter(|g| g.enabled) else {
                return Ok(ApiOut::json(
                    403,
                    json!({"success":false,"denied":true,"error":"streams not granted for this tenant"}).to_string(),
                ));
            };
            if grant.max_queries.is_some_and(|max| query_count >= max) {
                return Ok(ApiOut::json(
                    403,
                    json!({"success":false,"denied":true,"error":format!("streams query quota exceeded (max {})",grant.max_queries.unwrap())}).to_string(),
                ));
            }
        }
        let id = crate::util::uuidv7_bytes();
        let now = super::wall_micros();
        let row = StreamsQueryRow {
            name: name.clone(),
            source_queue: source,
            sink_queue: sink,
            config_hash: hash.clone(),
            created_at_us: now,
            updated_at_us: now,
        };
        self.submit_effects(
            &ctx,
            vec![Effect::StreamsQueryUpsert {
                query_id: id,
                tenant: ctx.tenant.clone(),
                row,
            }],
        )
        .await?;
        Ok(ApiOut::json(200, json!({"success":true,"query_id":uuid_bytes_to_string(&id),"name":name,"config_hash":hash,"fresh":true,"reset":false}).to_string()))
    }

    async fn api_streams_state_get(&self, ctx: ReqCtx, body: &[u8]) -> Result<ApiOut, RsmError> {
        self.linearizable(&ctx).await?;
        let root: Value = serde_json::from_slice(body).map_err(|e| rejected("bad_body", e))?;
        let qid = root
            .get("query_id")
            .and_then(Value::as_str)
            .and_then(uuid_string_to_bytes)
            .ok_or_else(|| reject("bad_request", "query_id is required"))?;
        let partition_id = root
            .get("partition_id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .ok_or_else(|| reject("bad_request", "partition_id is required"))?;
        let keys_wanted: Vec<String> = root
            .get("keys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let prefix = root
            .get("key_prefix")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let ripe_at_or_before = root.get("ripe_at_or_before").and_then(Value::as_i64);
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let rows = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                if r.streams_query(&tenant, &qid)?.is_none() { return Ok(None); }
                let mut pid = match partition_id.parse::<u64>() {
                    Ok(pid) => r
                        .partition(pid)?
                        .filter(|part| part.tenant == tenant)
                        .map(|_| pid),
                    Err(_) => None,
                };
                if pid.is_none() {
                    let Some(puid) = uuid_string_to_bytes(&partition_id) else { return Ok(None); };
                    r.scan_raw(Keyspace::Partitions, &[], &[], usize::MAX, &mut |k,v| {
                        if let Ok(p) = crate::rsm::store::rows::partition_decode(v) {
                            if p.tenant == tenant && p.uuid == puid { pid = keys::pid_of(k); return false; }
                        }
                        true
                    })?;
                }
                let Some(pid) = pid else { return Ok(None); };
                let sp = keys::streams_state_prefix(&qid, pid);
                let mut out = Vec::new();
                r.scan_raw(Keyspace::StreamsState, &sp, &sp, usize::MAX, &mut |k,v| {
                    if let (Some(key), Ok(row)) = (keys::streams_state_key_of(k), crate::rsm::store::rows::streams_state_decode(v)) {
                        let value = serde_json::from_slice::<Value>(&row.value).unwrap_or(Value::Null);
                        let ripe = ripe_at_or_before.is_none_or(|cutoff| value.get("windowEnd").and_then(Value::as_i64).is_some_and(|end| end <= cutoff));
                        if ripe && (keys_wanted.is_empty() || keys_wanted.contains(&key)) && key.starts_with(&prefix) {
                            out.push(json!({"key":key,"value":value,"updated_at":crate::rsm::planner::timers::iso_us(row.updated_at_us)}));
                        }
                    }
                    true
                })?;
                Ok(Some(out))
            })
        }).await.map_err(|e| RsmError::Internal(format!("streams state read: {e}")))?.map_err(read_error)?;
        match rows {
            Some(rows) => Ok(ApiOut::json(
                200,
                json!({"success":true,"rows":rows}).to_string(),
            )),
            None => Ok(ApiOut::json(404, json!({"error":"not found"}).to_string())),
        }
    }
}

fn map_propose(e: ProposeError) -> RsmError {
    match e {
        ProposeError::NotLeader { hint } => RsmError::Retry {
            leader_hint: hint.map(|h| h.to_string()),
        },
        ProposeError::OutcomeUnknown | ProposeError::Timeout => RsmError::Timeout,
        ProposeError::Refused(m) | ProposeError::Fatal(m) => RsmError::Internal(m),
    }
}

fn read_error(e: crate::rsm::store::StoreError) -> RsmError {
    RsmError::Internal(format!("state read: {e}"))
}

fn reject(code: impl Into<String>, message: impl Into<String>) -> RsmError {
    RsmError::Rejected {
        code: code.into(),
        message: message.into(),
    }
}

fn rejected(code: &'static str, e: impl std::fmt::Display) -> RsmError {
    reject(code, e.to_string())
}

/// A tenant purge or a quota grant that names a reserved tenant
/// ([`crate::config::is_reserved_tenant`]): refused whoever asks, because the
/// owners of those tenants act on them in-process and never through a route.
fn reserved_tenant(tenant: &str) -> ApiOut {
    ApiOut::json(
        403,
        json!({
            "success": false,
            "error": format!("tenant {} is reserved: no route acts on it", tenant.trim()),
            "code": "reserved_tenant",
        })
        .to_string(),
    )
}

fn configured_defaults(now: i64) -> QueueConfig {
    QueueConfig {
        id: crate::util::uuidv7_bytes(),
        namespace: Some(String::new()),
        task: Some(String::new()),
        priority: 0,
        lease_time: 300,
        retry_limit: 3,
        retry_delay: 1000,
        ttl: 3600,
        dead_letter_queue: true,
        dlq_after_max_retries: true,
        delayed_processing: 0,
        window_buffer: 0,
        retention_seconds: 0,
        completed_retention_seconds: 0,
        retention_enabled: false,
        encryption_enabled: false,
        max_wait_time_seconds: 0,
        max_queue_size: 0,
        min_pop_wait_time: 0,
        dedup_window_seconds: 3600,
        retention_sink_hold: String::new(),
        retention_sink_hold_max_seconds: 604800,
        created_at_us: now,
    }
}

fn apply_config_options(cfg: &mut QueueConfig, o: &Map<String, Value>) -> Result<(), RsmError> {
    // A queue created implicitly by a push stores 0 here (no sink hold was ever
    // configured): read it as the default, so a later merge-mode configure of
    // that queue is not refused over a value nobody set. An explicit 0 in the
    // request is still refused by the range check below.
    let defaults = configured_defaults(cfg.created_at_us);
    if cfg.retention_sink_hold_max_seconds == 0 {
        cfg.retention_sink_hold_max_seconds = defaults.retention_sink_hold_max_seconds;
    }
    let s = |k: &str| o.get(k).map(|v| v.as_str().unwrap_or("").to_string());
    let i = |k: &str| -> Result<Option<i32>, RsmError> {
        o.get(k)
            .map(|v| {
                v.as_i64()
                    .and_then(|n| i32::try_from(n).ok())
                    .ok_or_else(|| reject("bad_request", format!("{k} must be an integer")))
            })
            .transpose()
    };
    let b = |k: &str| -> Result<Option<bool>, RsmError> {
        o.get(k)
            .map(|v| {
                v.as_bool()
                    .ok_or_else(|| reject("bad_request", format!("{k} must be a boolean")))
            })
            .transpose()
    };
    if let Some(v) = s("namespace") {
        cfg.namespace = Some(v);
    }
    if let Some(v) = s("task") {
        cfg.task = Some(v);
    }
    // An explicit `null` restores the option's default (the dashboard's reset).
    macro_rules! set_i {
        ($k:literal,$f:ident) => {
            if o.get($k).is_some_and(Value::is_null) {
                cfg.$f = defaults.$f;
            } else if let Some(v) = i($k)? {
                cfg.$f = v;
            }
        };
    }
    macro_rules! set_b {
        ($k:literal,$f:ident) => {
            if o.get($k).is_some_and(Value::is_null) {
                cfg.$f = defaults.$f;
            } else if let Some(v) = b($k)? {
                cfg.$f = v;
            }
        };
    }
    set_i!("priority", priority);
    set_i!("leaseTime", lease_time);
    set_i!("retryLimit", retry_limit);
    set_i!("retryDelay", retry_delay);
    set_i!("maxSize", max_queue_size);
    set_i!("ttl", ttl);
    set_i!("delayedProcessing", delayed_processing);
    set_i!("windowBuffer", window_buffer);
    set_i!("retentionSeconds", retention_seconds);
    set_i!("completedRetentionSeconds", completed_retention_seconds);
    set_i!("maxWaitTimeSeconds", max_wait_time_seconds);
    set_i!("minPopWaitTime", min_pop_wait_time);
    set_i!("dedupWindowSeconds", dedup_window_seconds);
    set_i!(
        "retentionSinkHoldMaxSeconds",
        retention_sink_hold_max_seconds
    );
    set_b!("deadLetterQueue", dead_letter_queue);
    set_b!("dlqAfterMaxRetries", dlq_after_max_retries);
    set_b!("retentionEnabled", retention_enabled);
    set_b!("encryptionEnabled", encryption_enabled);
    cfg.min_pop_wait_time = cfg.min_pop_wait_time.clamp(0, 60000);
    cfg.dedup_window_seconds = cfg.dedup_window_seconds.max(0);
    if let Some(v) = s("retentionSinkHold") {
        cfg.retention_sink_hold = v;
    }
    if !cfg
        .retention_sink_hold
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || ".-_".contains(c))
        || cfg.retention_sink_hold.len() > 64
    {
        return Err(reject(
            "bad_request",
            "retentionSinkHold must match [A-Za-z0-9._-]{0,64}",
        ));
    }
    if !(60..=31_536_000).contains(&cfg.retention_sink_hold_max_seconds) {
        return Err(reject(
            "bad_request",
            "retentionSinkHoldMaxSeconds must be between 60 and 31536000",
        ));
    }
    Ok(())
}

fn config_options(c: &QueueConfig) -> Value {
    json!({"priority":c.priority,"leaseTime":c.lease_time,"retryLimit":c.retry_limit,"retryDelay":c.retry_delay,"maxSize":c.max_queue_size,"maxQueueSize":c.max_queue_size,"ttl":c.ttl,"deadLetterQueue":c.dead_letter_queue,"dlqAfterMaxRetries":c.dlq_after_max_retries,"delayedProcessing":c.delayed_processing,"windowBuffer":c.window_buffer,"retentionSeconds":c.retention_seconds,"completedRetentionSeconds":c.completed_retention_seconds,"retentionEnabled":c.retention_enabled,"encryptionEnabled":c.encryption_enabled,"maxWaitTimeSeconds":c.max_wait_time_seconds,"minPopWaitTime":c.min_pop_wait_time,"dedupWindowSeconds":c.dedup_window_seconds,"retentionSinkHold":c.retention_sink_hold,"retentionSinkHoldMaxSeconds":c.retention_sink_hold_max_seconds})
}
fn configured_json(name: &str, c: &QueueConfig) -> Value {
    json!({"configured":true,"queueId":uuid_bytes_to_string(&c.id),"partitionId":Value::Null,"queue":name,"namespace":c.namespace.clone().unwrap_or_default(),"task":c.task.clone().unwrap_or_default(),"storage":"segments","options":config_options(c)})
}
/// Every tenant's backlog and every backlogged queue's, `(tenant, queue,
/// pending, processing)`: what waits for a consumer and what is leased, as the
/// overview and the queue list count them. A tenant's total comes first with
/// an empty `queue` (written at zero too); a queue follows only when it holds
/// something. The dashboard's backlog sampler reads it once a minute
/// (`dashboard::collector`).
///
/// It walks partitions only within the read budget ([`stats`]): past it, a
/// queue counts its slowest group's pending messages from the group counters
/// and nothing leased. [`tenant_backlogs_of`] also takes the queues past the
/// budget from their last walk, and keeps the sampler that walks them going.
pub(crate) fn tenant_backlogs<R: TypedReads + ?Sized>(
    r: &R,
    now: i64,
) -> Result<Vec<(String, String, i64, i64)>, StoreError> {
    Ok(stats::backlogs(r, now, stats::exact_budget(), None)?.0)
}

/// [`tenant_backlogs`] of `store`, with the figures of its last walks for the
/// queues past the read budget.
pub(crate) fn tenant_backlogs_of(
    store: &std::sync::Arc<crate::rsm::store::heed_store::HeedStore>,
    now: i64,
) -> Result<Vec<(String, String, i64, i64)>, StoreError> {
    let known = stats::stats_for(store);
    let (rows, all) =
        store.read(|r| stats::backlogs(r, now, stats::exact_budget(), Some(&known)))?;
    if !all {
        known.wake(None);
    }
    Ok(rows)
}

/// One queue's figures for the list views, summed over its partitions.
#[derive(Default)]
struct SnapCounts {
    parts: i64,
    segments: i64,
    total: i64,
    /// Unconsumed by the slowest cursor, leased ones included.
    pending: i64,
    /// Inside a live lease (a subset of `pending`).
    processing: i64,
    dead_letter: i64,
    retained: i64,
}

impl SnapCounts {
    fn of(s: &stats::QueueStat) -> SnapCounts {
        SnapCounts {
            parts: s.fig.parts,
            segments: s.fig.segments,
            total: s.fig.total,
            pending: s.fig.pending,
            processing: s.fig.processing,
            dead_letter: s.dead_letter,
            retained: s.retained,
        }
    }
}

/// A queue as the queue listing shows it: `pending` excludes
/// what is leased (`processing`); `completed` = total − pending − DLQ, as the
/// queue detail counts it.
fn queue_json(name: &str, c: &QueueConfig, n: &SnapCounts) -> Value {
    json!({"id":uuid_bytes_to_string(&c.id),"name":name,"queue":name,"namespace":c.namespace.clone().unwrap_or_default(),"task":c.task.clone().unwrap_or_default(),"storage":"segments","partitions":n.parts,"segments":{"segments":n.segments,"messages":n.total},"messages":{"total":n.total,"pending":(n.pending-n.processing).max(0),"processing":n.processing,"completed":(n.total-n.pending-n.dead_letter).max(0),"deadLetter":n.dead_letter},"retainedBytes":n.retained,"options":config_options(c),"createdAt":crate::rsm::planner::timers::iso_us(c.created_at_us)})
}

/// `GET /api/v1/resources/queues/:queue`: the queue, its first `max`
/// partitions by partition id (sorted by name), and its totals — over the
/// listed partitions, or, when there are more (`"partitionsTruncated"`), the
/// queue's figures ([`stats`]).
fn queue_detail<R: TypedReads + ?Sized>(
    r: &R,
    tenant: &str,
    queue: &str,
    now: i64,
    max: usize,
    known: &stats::StoreStats,
) -> Result<Option<Value>, StoreError> {
    let Some(cfg) = r.queue(tenant, queue)? else {
        return Ok(None);
    };
    // At most `QUEEN_DASH_SIZES_MAX` partitions are listed; past it the
    // totals are the queue's figures (`stats`), not the listed ones'.
    let mut listed = 0usize;
    let mut truncated = false;
    let mut dlq_by_pid: BTreeMap<u64, i64> = BTreeMap::new();
    for (_, _, _, row) in reads::scan_dlq_rows(r, tenant, Some(queue), None)? {
        *dlq_by_pid.entry(row.pid).or_default() += 1;
    }
    let mut partitions = Vec::new();
    let mut totals = [0i64; 5]; // total, pending, processing, completed, dlq
    let mut retained = 0i64;
    let mut segments = 0i64;
    r.scan_queue_partitions(tenant, queue, None, usize::MAX, &mut |pid| {
        listed += 1;
        if listed > max {
            truncated = true;
            return false;
        }
        let Ok(Some(p)) = r.partition(pid) else { return true };
        let total = (p.last_offset - p.log_start as i64 + 1).max(0);
        let Backlog {
            floor,
            processing,
            consumed,
            last_activity,
        } = backlog(r, pid, now);
        let pending = p.pending_from(floor) as i64;
        let processing = processing.min(pending);
        let dead_letter = dlq_by_pid.get(&pid).copied().unwrap_or(0);
        let completed = (total - pending - dead_letter).max(0);
        let stats = json!({
            "total":total,"pending":pending,"processing":processing,
            "completed":completed,"failed":Value::Null,"deadLetter":dead_letter
        });
        totals[0] += total;
        totals[1] += pending;
        totals[2] += processing;
        totals[3] += completed;
        totals[4] += dead_letter;
        let partition_bytes = r.partition_counter(pid, Counter::RetainedBytes).unwrap_or(0);
        retained += partition_bytes;
        let mut sealed = 0i64;
        let _ = r.scan_partition_files(pid, usize::MAX, &mut |_| {
            sealed += 1;
            true
        });
        // The current open tail is a live API segment even though it
        // has not been sealed into PartitionFiles yet.
        segments += sealed + i64::from(p.last_offset >= p.log_start as i64);
        partitions.push(json!({
            "id":uuid_bytes_to_string(&p.uuid),
            "name":p.partition,
            "createdAt":crate::rsm::planner::timers::iso_us(p.created_at_us),
            "messages":stats,
            "stats":stats,
            "cursor":{"totalConsumed":consumed,"batchesConsumed":0},
            "lastActivity":last_activity.map(crate::rsm::planner::timers::iso_us),
            "oldestMessage":p.oldest_live_at_us.map(crate::rsm::planner::timers::iso_us),
            "newestMessage":if total > 0 { Some(crate::rsm::planner::timers::iso_us(p.last_created_at_us)) } else { None },
            "retainedBytes":partition_bytes
        }));
        true
    })?;
    partitions.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    if truncated {
        let (f, _) = stats::queue_figures(r, tenant, queue, now, stats::exact_budget(), known)?;
        let dlq = r.queue_counter(tenant, queue, Counter::DlqCount)?;
        totals = [
            f.total,
            f.pending,
            f.processing,
            (f.total - f.pending - dlq).max(0),
            dlq,
        ];
        segments = f.segments;
        retained = r.queue_counter(tenant, queue, Counter::RetainedBytes)?;
    }
    let mut v = json!({
        "id":uuid_bytes_to_string(&cfg.id),"name":queue,
        "namespace":cfg.namespace.clone().unwrap_or_default(),
        "task":cfg.task.clone().unwrap_or_default(),
        "createdAt":crate::rsm::planner::timers::iso_us(cfg.created_at_us),
        "options":config_options(&cfg),"partitions":partitions,
        "segments":{"segments":segments,"messages":totals[0]},
        "totals":{"total":totals[0],"pending":totals[1],"processing":totals[2],"completed":totals[3],"failed":Value::Null,"deadLetter":totals[4]},
        "retainedBytes":retained
    });
    if truncated {
        v["partitionsTruncated"] = Value::Bool(true);
    }
    Ok(Some(v))
}

/// What a partition's cursors say about its backlog. `floor` is the committed
/// offset its pending counts from: the slowest named group's, the queue-mode
/// cursor's when no group reads it, `-1` when nothing does.
struct Backlog {
    floor: i64,
    /// What live leases hold, not yet capped at the pending.
    processing: i64,
    consumed: u64,
    last_activity: Option<i64>,
}

fn backlog<R: TypedReads + ?Sized>(r: &R, pid: crate::rsm::effect::Pid, now: i64) -> Backlog {
    let mut named_min: Option<i64> = None;
    let mut queue_min: Option<i64> = None;
    let mut processing = 0i64;
    let mut consumed = 0u64;
    let mut last_activity: Option<i64> = None;
    let _ = r.scan_cursors(pid, usize::MAX, &mut |group, c| {
        if group == "__QUEUE_MODE__" {
            queue_min = Some(queue_min.map_or(c.committed, |v| v.min(c.committed)));
        } else {
            named_min = Some(named_min.map_or(c.committed, |v| v.min(c.committed)));
        }
        if rows::lease_live(&c, now) {
            processing += c
                .batch_end
                .map(|end| (end as i64 - c.committed).max(0))
                .unwrap_or(0);
        }
        consumed = consumed.saturating_add(c.total_consumed);
        last_activity = match (last_activity, c.lease_acquired_at_us) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (None, Some(b)) => Some(b),
            (a, None) => a,
        };
        true
    });
    Backlog {
        floor: named_min.or(queue_min).unwrap_or(-1),
        processing,
        consumed,
        last_activity,
    }
}

/// `GET /api/v1/resources/partitions`: the `limit` partitions of the tenant
/// (of `queue`, when given) holding the most pending, pending counted as the
/// queue detail counts it. Only the rows answered pay for their lag. The walk
/// stops after `max` partitions and says so; `None` for no such queue.
#[allow(clippy::too_many_arguments)]
fn top_partitions<R: TypedReads + ?Sized>(
    r: &R,
    qlog: Option<&crate::rsm::qlog::set::QLogReader>,
    reader: &crate::rsm::segments::Reader,
    tenant: &str,
    queue: Option<&str>,
    limit: usize,
    max: usize,
    now: i64,
) -> Result<Option<Value>, StoreError> {
    let queues = match queue {
        Some(q) => {
            if r.queue(tenant, q)?.is_none() {
                return Ok(None);
            }
            vec![q.to_string()]
        }
        None => {
            let mut qs = Vec::new();
            r.scan_queues(tenant, usize::MAX, &mut |name, _| {
                qs.push(name.to_string());
                true
            })?;
            qs
        }
    };
    let mut walked = 0usize;
    let mut truncated = false;
    // (pending, processing, floor, queue index, pid, row)
    let mut rows = Vec::new();
    'queues: for (qi, name) in queues.iter().enumerate() {
        let mut pids = Vec::new();
        r.scan_queue_partitions(
            tenant,
            name,
            None,
            (max - walked).saturating_add(1),
            &mut |pid| {
                pids.push(pid);
                true
            },
        )?;
        for pid in pids {
            if walked == max {
                truncated = true;
                break 'queues;
            }
            walked += 1;
            let Some(p) = r.partition(pid)? else { continue };
            let b = backlog(r, pid, now);
            let pending = p.pending_from(b.floor) as i64;
            rows.push((pending, b.processing.min(pending), b.floor, qi, pid, p));
        }
    }
    rows.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| a.3.cmp(&b.3))
            .then_with(|| a.5.partition.cmp(&b.5.partition))
    });
    rows.truncate(limit);
    let partitions: Vec<Value> = rows
        .into_iter()
        .map(|(pending, processing, floor, qi, pid, p)| {
            let oldest = if pending > 0 {
                let sealed = || {
                    let mut files = Vec::new();
                    let _ = r.scan_partition_files(pid, usize::MAX, &mut |f| {
                        files.push(f);
                        true
                    });
                    files
                };
                reads::oldest_unconsumed_us(qlog, reader, tenant, pid, &p, &sealed, floor)
            } else {
                None
            };
            json!({
                "queue":queues[qi],
                "partition":p.partition,
                "id":uuid_bytes_to_string(&p.uuid),
                "pending":pending,
                "processing":processing,
                "lagSeconds":oldest.map(|t| ((now - t) as f64 / 1_000_000.0).round().max(0.0) as i64),
            })
        })
        .collect();
    Ok(Some(json!({
        "partitions":partitions,
        "walked":walked,
        "truncated":truncated,
    })))
}

/// A queue's retained bytes by partition name, and whether it has more
/// partitions than listed.
type Sizes = (Map<String, Value>, bool);

/// The retained bytes of a queue's partitions by partition name, the first
/// `max` in partition-id order, and whether it has more; `None` for no such
/// queue.
fn queue_sizes<R: TypedReads + ?Sized>(
    r: &R,
    tenant: &str,
    queue: &str,
    max: usize,
) -> Result<Option<Sizes>, StoreError> {
    if r.queue(tenant, queue)?.is_none() {
        return Ok(None);
    }
    let mut pids = Vec::new();
    r.scan_queue_partitions(tenant, queue, None, max.saturating_add(1), &mut |pid| {
        pids.push(pid);
        true
    })?;
    let truncated = pids.len() > max;
    pids.truncate(max);
    let mut sizes = Map::new();
    for pid in pids {
        if let Ok(Some(p)) = r.partition(pid) {
            let bytes = r
                .partition_counter(pid, Counter::RetainedBytes)
                .unwrap_or(0);
            sizes.insert(p.partition, Value::from(bytes));
        }
    }
    Ok(Some((sizes, truncated)))
}

/// How long [`RaftFacade::caught_up`] waits for this node to apply the
/// leader's read index before it calls the node behind: an append's round
/// trip and an apply, not a lag.
const CAUGHT_UP_GRACE: std::time::Duration = std::time::Duration::from_millis(20);

/// `QUEEN_DASH_SIZES_MAX` (default 100,000): the most partitions one
/// `/sizes` answer lists.
fn sizes_max() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("QUEEN_DASH_SIZES_MAX")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(100_000)
    })
}

fn query_value(query: Option<&str>, key: &str) -> Option<String> {
    query_map(query).remove(key)
}

fn query_map(query: Option<&str>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for pair in query.unwrap_or("").split('&').filter(|s| !s.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        out.insert(percent_decode(k), percent_decode(v));
    }
    out
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'+' {
            out.push(b' ');
            i += 1;
        } else if b[i] == b'%' && i + 2 < b.len() {
            let h = |c: u8| match c {
                b'0'..=b'9' => Some(c - b'0'),
                b'a'..=b'f' => Some(c - b'a' + 10),
                b'A'..=b'F' => Some(c - b'A' + 10),
                _ => None,
            };
            if let (Some(a), Some(z)) = (h(b[i + 1]), h(b[i + 2])) {
                out.push(a * 16 + z);
                i += 3
            } else {
                out.push(b[i]);
                i += 1
            }
        } else {
            out.push(b[i]);
            i += 1
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub(super) fn depth_json<R: Reads + ?Sized>(
    r: &R,
    tenant: &str,
    queue: &str,
    group: Option<&str>,
    now: i64,
) -> crate::rsm::store::Result<Option<Value>> {
    let Some(_) = r.queue(tenant, queue)? else {
        return Ok(None);
    };
    let mut parts = Vec::new();
    let mut conflation = false;
    if let Some(g) = group {
        conflation = r
            .group(tenant, queue, g)?
            .is_some_and(|x| x.meta.conflation);
    }
    // Queue level: every NAMED group of the queue counts, including one with no
    // cursor on a partition yet (it has consumed nothing there), so `pending`
    // is the worst named cursor's; the queue-mode cursor counts only when the
    // queue has no named group.
    let mut named_groups: Vec<String> = Vec::new();
    if group.is_none() {
        r.scan_groups(tenant, queue, usize::MAX, &mut |g, _| {
            if g != "__QUEUE_MODE__" {
                named_groups.push(g.to_string());
            }
            true
        })?;
    }
    r.scan_queue_partitions(tenant, queue, None, usize::MAX, &mut |pid| {
        if let Ok(Some(p)) = r.partition(pid) {
            let live = |c: &crate::rsm::effect::CursorRow| {
                if c.lease_expires_at_us.is_some_and(|x| x > now) {
                    (c.batch_end.unwrap_or(c.committed.max(0) as u64) as i64 - c.committed).max(0)
                } else {
                    0
                }
            };
            let (committed, processing) = if let Some(g) = group {
                r.cursor(pid, g)
                    .ok()
                    .flatten()
                    .map(|c| (c.committed, live(&c)))
                    .unwrap_or((-1, 0))
            } else {
                let mut named: Option<i64> = None;
                let mut seen = 0usize;
                let mut qm: Option<i64> = None;
                let mut proc = 0;
                let _ = r.scan_cursors(pid, usize::MAX, &mut |g, c| {
                    if g == "__QUEUE_MODE__" {
                        qm = Some(qm.map_or(c.committed, |x| x.min(c.committed)));
                    } else {
                        seen += 1;
                        named = Some(named.map_or(c.committed, |x| x.min(c.committed)));
                    }
                    proc += live(&c);
                    true
                });
                if seen < named_groups.len() {
                    named = Some(-1);
                }
                (named.or(qm).unwrap_or(-1), proc)
            };
            let pending = p.pending_from(committed) as i64;
            let processing = processing.min(pending);
            parts.push(json!({"partition":p.partition,"pending":pending,"processing":processing,"ready":pending-processing}));
        }
        true
    })?;
    parts.sort_by(|a, b| a["partition"].as_str().cmp(&b["partition"].as_str()));
    let pending: i64 = parts.iter().filter_map(|v| v["pending"].as_i64()).sum();
    let processing: i64 = parts.iter().filter_map(|v| v["processing"].as_i64()).sum();
    let ready = pending - processing;
    let pp = parts
        .iter()
        .filter(|v| v["pending"].as_i64().unwrap_or(0) > 0)
        .count() as i64;
    let pr = parts
        .iter()
        .filter(|v| v["ready"].as_i64().unwrap_or(0) > 0)
        .count() as i64;
    Ok(Some(
        json!({"queue":queue,"group":group,"pending":pending,"processing":processing,"ready":ready,"partitionsPending":pp,"partitionsReady":pr,"conflation":conflation,"effectivePending":if conflation{pp}else{pending},"effectiveReady":if conflation{pr}else{ready},"partitions":parts}),
    ))
}

#[cfg(test)]
mod configure_tests {
    use super::*;

    fn opts(v: Value) -> Map<String, Value> {
        v.as_object().cloned().unwrap_or_default()
    }

    /// A queue a push created stores no sink-hold ceiling; merging options into
    /// it must not trip the ceiling's range check (it used to answer 400 to
    /// every SDK `.create()` that followed a push).
    #[test]
    fn a_merge_into_an_implicitly_created_queue_is_accepted() {
        let mut cfg = crate::rsm::planner::timers::implicit_queue_config();
        assert_eq!(cfg.retention_sink_hold_max_seconds, 0);
        apply_config_options(&mut cfg, &opts(json!({"leaseTime": 60}))).expect("merge");
        assert_eq!(cfg.lease_time, 60);
        assert_eq!(cfg.retention_sink_hold_max_seconds, 604_800);
    }

    #[test]
    fn an_explicit_out_of_range_sink_hold_ceiling_is_still_refused() {
        let mut cfg = configured_defaults(0);
        let err = apply_config_options(&mut cfg, &opts(json!({"retentionSinkHoldMaxSeconds": 0})));
        assert!(err.is_err());
    }

    /// `null` restores the default (the dashboard editor's reset).
    #[test]
    fn null_restores_the_default() {
        let mut cfg = configured_defaults(0);
        apply_config_options(
            &mut cfg,
            &opts(json!({"leaseTime": 60, "retentionEnabled": true})),
        )
        .unwrap();
        apply_config_options(
            &mut cfg,
            &opts(json!({"leaseTime": null, "retentionEnabled": null})),
        )
        .unwrap();
        let d = configured_defaults(0);
        assert_eq!(cfg.lease_time, d.lease_time);
        assert_eq!(cfg.retention_enabled, d.retention_enabled);
    }
}
