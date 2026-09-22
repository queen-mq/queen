//! Read-only Phase-2 HTTP surface. Reads are taken from one LMDB transaction;
//! payloads are then read from this node's committed queue/segment log.

use std::collections::{BTreeSet, HashMap};

use serde::Deserialize;
use serde_json::{json, Value};

use super::{query_map, read_error, ApiOut, ApiReq, Effect, RaftFacade, ReqCtx, RsmError};
use crate::frames::{unpack_frames_ref, uuid_bytes_to_string, uuid_string_to_bytes};
use crate::rsm::effect::{Pid, TraceEvent};
use crate::rsm::planner::bucket_of;
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::store::keys;
use crate::rsm::store::rows::{self, DlqRow, PartitionRow};
use crate::rsm::store::{Keyspace, Reads, Store, TypedReads};

#[derive(Clone)]
pub(super) struct Part {
    pub(super) pid: Pid,
    pub(super) row: PartitionRow,
    pub(super) sealed: Vec<u32>,
}

#[derive(Clone)]
pub(super) struct Record {
    pub(super) queue: String,
    pub(super) partition: String,
    pub(super) partition_id: [u8; 16],
    pub(super) offset: u64,
    pub(super) segment_base: u64,
    pub(super) frame_idx: usize,
    pub(super) created_at_us: i64,
    pub(super) id: [u8; 16],
    pub(super) txn: String,
    pub(super) trace_id: Option<[u8; 16]>,
    pub(super) producer_sub: Option<String>,
    pub(super) payload: Vec<u8>,
    pub(super) encrypted: bool,
}

#[derive(Deserialize)]
struct FetchBody {
    #[serde(default)]
    entries: Vec<FetchEntry>,
    #[serde(rename = "maxWaitMs", default)]
    max_wait_ms: Option<u64>,
    #[serde(rename = "minBytes", default)]
    min_bytes: Option<i64>,
}

#[derive(Deserialize)]
struct FetchEntry {
    queue: String,
    #[serde(default)]
    partition: Option<Value>,
    offset: i64,
    #[serde(rename = "maxBytes")]
    max_bytes: Option<i64>,
}

impl RaftFacade {
    /// Keep the legacy diagnostics route wire-compatible in a Postgres-free
    /// deployment. Its categories are inherently PostgreSQL-specific, so Raft
    /// reports an explicit engine marker and empty category collections rather
    /// than dialing the lazy pool or returning a misleading 404.
    pub(super) async fn api_postgres_stats(&self, ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        self.linearizable(&ctx).await?;
        Ok(ApiOut::json(
            200,
            json!({
                "timestamp": crate::rsm::planner::timers::iso_us(super::super::wall_micros()),
                "database": "raft",
                "databaseCache": {},
                "tableCache": [],
                "indexCache": [],
                "cacheSummary": {},
                "deadTuples": [],
                "hotUpdates": [],
                "activeQueries": [],
                "autovacuumStatus": [],
                "bufferConfig": {},
                "bufferUsage": [],
                "tableSizes": []
            })
            .to_string(),
        ))
    }

    pub(super) async fn api_dynamic(
        &self,
        ctx: &ReqCtx,
        req: &ApiReq,
    ) -> Result<Option<ApiOut>, RsmError> {
        let p: Vec<&str> = req.path.trim_matches('/').split('/').collect();
        let out = match p.as_slice() {
            ["api", "v1", "messages", pid, txn] if req.method == "GET" => {
                Some(self.api_message(ctx.clone(), pid, txn).await?)
            }
            ["api", "v1", "messages", pid, txn] if req.method == "DELETE" => {
                Some(self.api_message_delete(ctx.clone(), pid, txn).await?)
            }
            ["api", "v1", "messages", pid, txn, "retry"] if req.method == "POST" => Some(
                self.api_dlq_move(ctx.clone(), None, Some((pid, txn)), &req.body)
                    .await?,
            ),
            ["api", "v1", "dlq", id, "replay"] if req.method == "POST" => Some(
                self.api_dlq_move(ctx.clone(), Some(id), None, &req.body)
                    .await?,
            ),
            ["api", "v1", "consumer-groups", group] if req.method == "GET" => {
                Some(self.api_groups(ctx.clone(), Some(group)).await?)
            }
            ["api", "v1", "consumer-groups", group] if req.method == "DELETE" => Some(
                self.api_group_delete(ctx.clone(), group, None, req.query.as_deref())
                    .await?,
            ),
            ["api", "v1", "consumer-groups", group, "subscription"] if req.method == "POST" => {
                Some(
                    self.api_group_subscription(ctx.clone(), group, &req.body)
                        .await?,
                )
            }
            ["api", "v1", "consumer-groups", group, "queues", queue] if req.method == "DELETE" => {
                Some(
                    self.api_group_delete(ctx.clone(), group, Some(queue), req.query.as_deref())
                        .await?,
                )
            }
            ["api", "v1", "consumer-groups", group, "queues", queue, "seek"]
                if req.method == "POST" =>
            {
                Some(
                    self.api_group_seek(ctx.clone(), group, queue, None, &req.body)
                        .await?,
                )
            }
            ["api", "v1", "consumer-groups", group, "queues", queue, "partitions", part, "seek"]
                if req.method == "POST" =>
            {
                Some(
                    self.api_group_seek(ctx.clone(), group, queue, Some(part), &req.body)
                        .await?,
                )
            }
            ["api", "v1", "traces", "by-name", name] if req.method == "GET" => Some(
                self.api_traces(ctx.clone(), None, None, Some(name), req.query.as_deref())
                    .await?,
            ),
            ["api", "v1", "traces", pid, txn] if req.method == "GET" => Some(
                self.api_traces(
                    ctx.clone(),
                    Some(pid),
                    Some(txn),
                    None,
                    req.query.as_deref(),
                )
                .await?,
            ),
            ["api", "v1", "status", "queues", queue] if req.method == "GET" => Some(
                self.api_status_queue(ctx.clone(), queue, req.query.as_deref())
                    .await?,
            ),
            ["api", "v1", "ephemeral", "queue", queue] if req.method == "DELETE" => {
                Some(self.api_ephemeral_delete(ctx.clone(), queue).await?)
            }
            _ => None,
        };
        Ok(out)
    }

    pub(super) async fn api_fetch(&self, ctx: ReqCtx, body: &[u8]) -> Result<ApiOut, RsmError> {
        let request: FetchBody =
            serde_json::from_slice(body).map_err(|e| super::rejected("bad_body", e))?;
        let max_wait = request.max_wait_ms.unwrap_or(0).min(30_000);
        let min_bytes = request.min_bytes.unwrap_or(1).max(0) as usize;
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(max_wait);
        loop {
            let out = self.api_fetch_once(ctx.clone(), body).await?;
            if out.status != 200 || max_wait == 0 || min_bytes == 0 {
                return Ok(out);
            }
            let parsed: Value = serde_json::from_str(&out.body).unwrap_or(Value::Null);
            let mut bytes = 0usize;
            let mut error = false;
            for entry in parsed
                .get("entries")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                error |= entry.get("error").is_some();
                for record in entry
                    .get("records")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    bytes = bytes.saturating_add(
                        record
                            .get("payload")
                            .map(|v| v.to_string().len())
                            .unwrap_or(0)
                            .max(1),
                    );
                }
            }
            if error || bytes >= min_bytes || std::time::Instant::now() >= deadline {
                return Ok(out);
            }
            let wait = deadline
                .saturating_duration_since(std::time::Instant::now())
                .min(std::time::Duration::from_millis(200));
            self.notifier.wait_any(&ctx.tenant, wait).await;
        }
    }

    async fn api_fetch_once(&self, ctx: ReqCtx, body: &[u8]) -> Result<ApiOut, RsmError> {
        let body: FetchBody =
            serde_json::from_slice(body).map_err(|e| super::rejected("bad_body", e))?;
        if body.entries.len() > 1024 {
            return Ok(ApiOut::json(
                400,
                json!({"error":"too many entries"}).to_string(),
            ));
        }
        if body.entries.iter().any(|e| e.offset < 0) {
            return Ok(ApiOut::json(
                400,
                json!({"error":"offset must be non-negative"}).to_string(),
            ));
        }
        let asks: Vec<(String, String, u64, usize)> = body
            .entries
            .into_iter()
            .map(|e| {
                let partition = match e.partition {
                    Some(Value::String(s)) => s,
                    Some(Value::Number(n)) => n.to_string(),
                    _ => "Default".to_string(),
                };
                (
                    e.queue,
                    partition,
                    e.offset as u64,
                    e.max_bytes.unwrap_or(1 << 20).clamp(1, 8 << 20) as usize,
                )
            })
            .collect();
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let asks2 = asks.clone();
        let parts = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut out = Vec::with_capacity(asks2.len());
                for (queue, part, _, _) in &asks2 {
                    let queue_exists = r.queue(&tenant, queue)?.is_some();
                    let p = match r.pid_of(&tenant, queue, part)? {
                        Some(pid) => {
                            let row = r.partition(pid)?;
                            let mut sealed = Vec::new();
                            r.scan_partition_files(pid, usize::MAX, &mut |f| {
                                sealed.push(f);
                                true
                            })?;
                            row.map(|row| Part { pid, row, sealed })
                        }
                        None => None,
                    };
                    out.push((queue_exists, p));
                }
                Ok(out)
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("fetch read: {e}")))?
        .map_err(read_error)?;

        let qlog = self.qlog_reader.clone();
        let reader = self.reader.clone();
        let encryption = self.encryption.clone();
        let tenant2 = ctx.tenant.clone();
        let entries = tokio::task::spawn_blocking(move || -> Result<Vec<Value>, RsmError> {
            let mut out = Vec::with_capacity(asks.len());
            for ((queue, partition, offset, max_bytes), (queue_exists, part)) in asks.into_iter().zip(parts) {
                let Some(part) = part else {
                    out.push(if queue_exists {
                        json!({"queue":queue,"partition":partition,"records":[],"highWatermark":0,"logStartOffset":0})
                    } else {
                        json!({"queue":queue,"partition":partition,"records":[],"highWatermark":0,"logStartOffset":0,"error":"UNKNOWN_TOPIC_OR_PARTITION"})
                    });
                    continue;
                };
                let high = (part.row.last_offset + 1).max(0) as u64;
                if offset < part.row.log_start || offset > high {
                    out.push(json!({"queue":queue,"partition":partition,"records":[],"highWatermark":high,"logStartOffset":part.row.log_start,"error":"OFFSET_OUT_OF_RANGE"}));
                    continue;
                }
                let mut records = Vec::new();
                let mut used = 0usize;
                walk_records(&reader, qlog.as_ref(), &tenant2, &part, offset, high, 10_000, |mut rec| {
                    decrypt_record(&encryption, &mut rec);
                    let weight = rec.payload.len().max(1);
                    if !records.is_empty() && used.saturating_add(weight) > max_bytes { return false; }
                    used = used.saturating_add(weight);
                    records.push(json!({"offset":rec.offset,"transactionId":rec.txn,"payload":payload_json(&rec.payload),"ts":crate::rsm::planner::timers::iso_us(rec.created_at_us)}));
                    true
                })?;
                out.push(json!({"queue":queue,"partition":partition,"records":records,"highWatermark":high,"logStartOffset":part.row.log_start}));
            }
            Ok(out)
        }).await.map_err(|e| RsmError::Internal(format!("fetch payload task: {e}")))??;
        Ok(ApiOut::json(200, json!({"entries":entries}).to_string()))
    }

    pub(super) async fn api_partitions_changed(
        &self,
        ctx: ReqCtx,
        body: &[u8],
    ) -> Result<ApiOut, RsmError> {
        let root: Value =
            serde_json::from_slice(body).map_err(|e| super::rejected("bad_body", e))?;
        let entries = root
            .get("entries")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if entries.len() > 1024 {
            return Ok(ApiOut::json(
                400,
                json!({"error":"too many entries"}).to_string(),
            ));
        }
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let out = tokio::task::spawn_blocking(move || store.read(|r| {
            let mut answer = Vec::with_capacity(entries.len());
            for e in entries {
                let queue = e.get("queue").and_then(Value::as_str).unwrap_or("");
                if r.queue(&tenant, queue)?.is_none() {
                    answer.push(json!({"queue":queue,"error":"UNKNOWN_TOPIC_OR_PARTITION"}));
                    continue;
                }
                let since = e.get("since").and_then(Value::as_str).and_then(crate::util::parse_iso_ms).map(|v|v*1000);
                let after = e.get("after").and_then(Value::as_str);
                let limit = e.get("limit").and_then(Value::as_u64).unwrap_or(1000).clamp(1,1000) as usize;
                let mode = if since.is_some() { 't' } else { 'n' };
                let cursor = parse_changed_cursor(after, mode);
                if after.is_some() && cursor.is_none() {
                    answer.push(json!({"queue":queue,"error":"BAD_CURSOR"})); continue;
                }
                let mut rows = Vec::new();
                r.scan_queue_partitions(&tenant, queue, None, usize::MAX, &mut |pid| {
                    if let Ok(Some(p)) = r.partition(pid) {
                        let keep = match since { Some(s) => p.last_write_at_us >= s, None => true };
                        if keep { rows.push(p); }
                    }
                    true
                })?;
                if mode == 't' { rows.sort_by(|a,b|(a.last_write_at_us,&a.partition).cmp(&(b.last_write_at_us,&b.partition))); }
                else { rows.sort_by(|a,b|a.partition.cmp(&b.partition)); }
                let rows: Vec<_> = rows.into_iter().filter(|p| match &cursor {
                    Some((_, ts, name)) if mode == 't' => (p.last_write_at_us, &p.partition) > (*ts, name),
                    Some((_, _, name)) => p.partition > *name,
                    None => true,
                }).take(limit).collect();
                let next = if rows.len() == limit { rows.last().map(|p| if mode=='t' { format!("t|{}|{}",p.last_write_at_us,p.partition) } else { format!("n|{}",p.partition) }) } else { None };
                let parts:Vec<Value>=rows.into_iter().map(|p|json!({"name":p.partition,"lastOffset":p.last_offset,"logStart":p.log_start,"lastWriteAt":crate::rsm::planner::timers::iso_us(p.last_write_at_us)})).collect();
                answer.push(json!({"queue":queue,"partitions":parts,"next":next}));
            }
            Ok(answer)
        })).await.map_err(|e| RsmError::Internal(format!("partitions changed: {e}")))?.map_err(read_error)?;
        Ok(ApiOut::json(200, json!({"safeTime":crate::rsm::planner::timers::iso_us(super::super::wall_micros()),"safeTimeDegraded":false,"entries":out}).to_string()))
    }

    pub(super) async fn api_messages(
        &self,
        ctx: ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let q = query_map(query);
        let queue = q.get("queue").cloned();
        let partition = q.get("partition").cloned();
        let limit = q
            .get("limit")
            .and_then(|s| s.parse().ok())
            .unwrap_or(200usize)
            .clamp(1, 1000);
        let offset = q
            .get("offset")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0usize);
        let selected_parts = self
            .parts(&ctx.tenant, queue.as_deref(), partition.as_deref())
            .await?;
        let selected_pids: Vec<Pid> = selected_parts.iter().map(|p| p.pid).collect();
        let store = self.store.clone();
        let mode = tokio::task::spawn_blocking(move || {
            store.read(|r| {
            let mut has_queue_mode = false;
            let mut groups = BTreeSet::new();
            for pid in selected_pids {
                r.scan_cursors(pid, usize::MAX, &mut |group, _| {
                    if group == "__QUEUE_MODE__" {
                        has_queue_mode = true;
                    } else {
                        groups.insert(group.to_string());
                    }
                    true
                })?;
            }
            let bus_groups_count = groups.len();
            let ty = match (has_queue_mode, bus_groups_count > 0) {
                (false, false) => "none",
                (true, false) => "queue",
                (false, true) => "bus",
                (true, true) => "hybrid",
            };
            Ok(json!({"hasQueueMode":has_queue_mode,"busGroupsCount":bus_groups_count,"type":ty}))
        })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("message mode read: {e}")))?
        .map_err(read_error)?;
        let mut records = self
            .collect_records(
                &ctx.tenant,
                queue.as_deref(),
                partition.as_deref(),
                limit.saturating_add(offset),
            )
            .await?;
        records.sort_by(|a, b| {
            b.created_at_us
                .cmp(&a.created_at_us)
                .then_with(|| b.offset.cmp(&a.offset))
        });
        let total = records.len();
        let rows: Vec<Value> = records
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(message_json)
            .collect();
        Ok(ApiOut::json(
            200,
            json!({"messages":rows,"total":total,"pagination":{"limit":limit,"offset":offset},"mode":mode})
                .to_string(),
        ))
    }

    pub(super) async fn api_message(
        &self,
        ctx: ReqCtx,
        partition: &str,
        txn: &str,
    ) -> Result<ApiOut, RsmError> {
        let Some((part, rec)) = self.find_message(&ctx.tenant, partition, txn).await? else {
            return Ok(ApiOut::json(
                404,
                json!({"error":"Message not found"}).to_string(),
            ));
        };
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let txn2 = txn.to_string();
        let pid = part.pid;
        let queue = part.row.queue.clone();
        let rec_for_detail = rec.clone();
        let detail=tokio::task::spawn_blocking(move || store.read(|r| {
            let cfg=r.queue(&tenant,&queue)?;
            let mut groups=Vec::new(); let mut live=false; let mut completed=false; let mut dlq=None;
            r.scan_cursors(pid,usize::MAX,&mut |g,c| { let consumed=rec_for_detail.offset as i64<=c.committed; completed|=consumed; live|=c.worker.is_some()&&c.lease_expires_at_us.is_some_and(|x|x>super::super::wall_micros()); groups.push(json!({"name":g,"committed":c.committed,"consumed":consumed,"leaseExpiresAt":c.lease_expires_at_us.map(crate::rsm::planner::timers::iso_us)})); true })?;
            let pref=keys::dlq_by_pos_pid_prefix(pid); r.scan_raw(Keyspace::DlqByPos,&pref,&pref,usize::MAX,&mut |_k,v| { if let Ok(ids)=rows::dlq_ids_decode(v) { for id in ids { if let Ok(Some(row))=r.dlq(&tenant,&queue,&id) { if row.txn==txn2 { dlq=Some(row); return false; } } } } true })?;
            Ok((cfg,groups,live,completed,dlq))
        })).await.map_err(|e|RsmError::Internal(format!("message detail: {e}")))?.map_err(read_error)?;
        let (cfg, groups, live, completed, dlq) = detail;
        let status = if dlq.is_some() {
            "dead_letter"
        } else if completed {
            "completed"
        } else if live {
            "processing"
        } else {
            "pending"
        };
        let has_queue_mode = groups
            .iter()
            .any(|g| g.get("name").and_then(Value::as_str) == Some("__QUEUE_MODE__"));
        let bus_groups = groups
            .iter()
            .filter(|g| g.get("name").and_then(Value::as_str) != Some("__QUEUE_MODE__"))
            .count();
        Ok(ApiOut::json(200,json!({"id":uuid_bytes_to_string(&rec.id),"transactionId":rec.txn,"data":payload_json(&rec.payload),"payload":payload_json(&rec.payload),"traceId":rec.trace_id.map(|x|uuid_bytes_to_string(&x)),"producerSub":rec.producer_sub,"createdAt":crate::rsm::planner::timers::iso_us(rec.created_at_us),"partitionId":uuid_bytes_to_string(&part.row.uuid),"partition":part.row.partition,"queue":part.row.queue,"queuePath":format!("{}/{}",part.row.queue,part.row.partition),"namespace":cfg.as_ref().and_then(|x|x.namespace.clone()),"task":cfg.as_ref().and_then(|x|x.task.clone()),"status":status,"errorMessage":dlq.as_ref().map(|x|x.error.clone()),"retryCount":dlq.as_ref().map(|x|x.retry_count).unwrap_or(0),"isEncrypted":rec.encrypted,"queueConfig":cfg.as_ref().map(super::config_options),"mode":{"hasQueueMode":has_queue_mode,"busGroupsCount":bus_groups,"type":if bus_groups>0{"bus"}else{"queue"}},"consumerGroups":groups}).to_string()))
    }

    pub(super) async fn api_dlq(
        &self,
        ctx: ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let q = query_map(query);
        let queue = q.get("queue").cloned();
        let group = q.get("consumerGroup").cloned();
        let limit = q
            .get("limit")
            .and_then(|v| v.parse().ok())
            .unwrap_or(100usize)
            .clamp(1, 1000);
        let offset = q
            .get("offset")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0usize);
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let (mut rows, partitions) = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let rows = scan_dlq_rows(r, &tenant, queue.as_deref(), group.as_deref())?;
                let mut partitions = HashMap::new();
                for (_, _, _, row) in &rows {
                    if let Some(part) = r.partition(row.pid)? {
                        partitions.insert(row.pid, (part.partition, part.uuid));
                    }
                }
                Ok((rows, partitions))
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("dlq read: {e}")))?
        .map_err(read_error)?;
        rows.sort_by(|a, b| b.3.failed_at_us.cmp(&a.3.failed_at_us));
        let total = rows.len();
        let messages: Vec<Value> = rows
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|mut row| {
                if let Some(payload) = self.encryption.decrypt_payload_bytes(&row.3.payload) {
                    row.3.payload = payload;
                }
                let partition = partitions.get(&row.3.pid);
                dlq_json(row, partition)
            })
            .collect();
        Ok(ApiOut::json(
            200,
            json!({"messages":messages,"total":total,"pagination":{"limit":limit,"offset":offset}})
                .to_string(),
        ))
    }

    pub(super) async fn api_groups(
        &self,
        ctx: ReqCtx,
        only: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let only = only.map(str::to_string);
        let only_read = only.clone();
        let now = super::super::wall_micros();
        let groups = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                if let Some(group) = only_read.as_deref() {
                    group_detail(r, &tenant, group, now).map(|v| vec![v])
                } else {
                    group_view(r, &tenant, None, now)
                }
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("groups read: {e}")))?
        .map_err(read_error)?;
        if only.is_some()
            && groups
                .first()
                .is_none_or(|v| v.as_object().is_none_or(|o| o.is_empty()))
        {
            return Ok(ApiOut::json(
                404,
                json!({"error":"Consumer group not found"}).to_string(),
            ));
        }
        Ok(ApiOut::json(
            200,
            if only.is_some() {
                groups
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| json!({}))
                    .to_string()
            } else {
                json!({"consumerGroups":groups}).to_string()
            },
        ))
    }

    pub(super) async fn api_lagging_groups(
        &self,
        ctx: ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let min = query_map(query)
            .get("minLagSeconds")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(3600);
        let now = super::super::wall_micros();
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let rows = tokio::task::spawn_blocking(move || {
            store.read(|r| lagging_partitions(r, &tenant, min, now))
        })
        .await
        .map_err(|e| RsmError::Internal(format!("lag read: {e}")))?
        .map_err(read_error)?;
        Ok(ApiOut::json(200, Value::Array(rows).to_string()))
    }

    pub(super) async fn api_trace_record(
        &self,
        ctx: ReqCtx,
        body: &[u8],
    ) -> Result<ApiOut, RsmError> {
        let v: Value = serde_json::from_slice(body).map_err(|e| super::rejected("bad_body", e))?;
        let txn = v
            .get("transactionId")
            .or_else(|| v.get("transaction_id"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if txn.is_empty() {
            return Ok(ApiOut::json(
                400,
                json!({"error":"transactionId is required"}).to_string(),
            ));
        }
        let pid_s = v
            .get("partitionId")
            .or_else(|| v.get("partition_id"))
            .and_then(Value::as_str);
        let pid = self.resolve_pid(&ctx.tenant, pid_s).await?;
        let names = v
            .get("traceNames")
            .or_else(|| v.get("names"))
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let event = TraceEvent {
            trace_id: v
                .get("traceId")
                .and_then(Value::as_str)
                .and_then(uuid_string_to_bytes)
                .unwrap_or_else(crate::util::uuidv7_bytes),
            tenant: ctx.tenant.clone(),
            pid,
            message_id: v
                .get("messageId")
                .and_then(Value::as_str)
                .and_then(uuid_string_to_bytes),
            txn,
            consumer_group: v
                .get("consumerGroup")
                .and_then(Value::as_str)
                .map(str::to_string),
            event_type: v
                .get("eventType")
                .or_else(|| v.get("type"))
                .and_then(Value::as_str)
                .unwrap_or("event")
                .to_string(),
            data: serde_json::to_vec(v.get("data").unwrap_or(&Value::Null)).unwrap(),
            worker: v.get("worker").and_then(Value::as_str).map(str::to_string),
            names,
            created_at_us: super::super::wall_micros(),
        };
        let id = uuid_bytes_to_string(&event.trace_id);
        self.submit_effects(&ctx, vec![Effect::TraceAppend { event }])
            .await?;
        Ok(ApiOut::json(
            200,
            json!({"success":true,"traceId":id}).to_string(),
        ))
    }

    pub(super) async fn api_trace_names(
        &self,
        ctx: ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let params = query_map(query);
        let limit = params
            .get("limit")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100usize)
            .clamp(1, 1000);
        let offset = params
            .get("offset")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0usize);
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let names = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut stats: std::collections::BTreeMap<
                    String,
                    (usize, BTreeSet<(Option<Pid>, String)>, i64),
                > = std::collections::BTreeMap::new();
                r.scan_traces(usize::MAX, &mut |_k, e| {
                    if e.tenant == tenant {
                        for n in &e.names {
                            let stat = stats
                                .entry(n.clone())
                                .or_insert_with(|| (0, BTreeSet::new(), e.created_at_us));
                            stat.0 += 1;
                            stat.1.insert((e.pid, e.txn.clone()));
                            stat.2 = stat.2.max(e.created_at_us);
                        }
                    }
                    true
                })?;
                let total = stats.len();
                let rows = stats
                    .into_iter()
                    .skip(offset)
                    .take(limit)
                    .map(|(name, (trace_count, messages, last_seen))| {
                        json!({
                            "trace_name":name,
                            "trace_count":trace_count,
                            "message_count":messages.len(),
                            "last_seen":crate::rsm::planner::timers::iso_us(last_seen)
                        })
                    })
                    .collect::<Vec<_>>();
                Ok((total, rows))
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("trace names: {e}")))?
        .map_err(read_error)?;
        Ok(ApiOut::json(
            200,
            json!({"trace_names":names.1,"total":names.0,"pagination":{"limit":limit,"offset":offset}})
                .to_string(),
        ))
    }

    pub(super) async fn api_traces(
        &self,
        ctx: ReqCtx,
        pid: Option<&str>,
        txn: Option<&str>,
        name: Option<&str>,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let params = query_map(query);
        let limit = params
            .get("limit")
            .and_then(|s| s.parse().ok())
            .unwrap_or(200usize)
            .clamp(1, 1000);
        let offset = params
            .get("offset")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0usize);
        let wanted_pid = self.resolve_pid(&ctx.tenant, pid).await?;
        let tenant = ctx.tenant.clone();
        let txn = txn.map(str::to_string);
        let name = name.map(str::to_string);
        let store = self.store.clone();
        let mut events = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut out = Vec::new();
                r.scan_traces(usize::MAX, &mut |_k, e| {
                    if e.tenant == tenant
                        && wanted_pid.is_none_or(|p| e.pid == Some(p))
                        && txn.as_ref().is_none_or(|t| &e.txn == t)
                        && name.as_ref().is_none_or(|n| e.names.contains(n))
                    {
                        let location = e
                            .pid
                            .and_then(|pid| r.partition(pid).ok().flatten())
                            .map(|p| (p.queue, p.partition));
                        out.push((e.created_at_us, trace_json(e, location)));
                    }
                    true
                })?;
                Ok(out)
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("traces: {e}")))?
        .map_err(read_error)?;
        events.sort_by(|a, b| b.0.cmp(&a.0));
        let total = events.len();
        let events: Vec<Value> = events
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|(_, event)| event)
            .collect();
        let events2 = events.clone();
        Ok(ApiOut::json(
            200,
            json!({"traces":events,"events":events2,"total":total,"pagination":{"limit":limit,"offset":offset}}).to_string(),
        ))
    }

    pub(super) async fn resolve_pid(
        &self,
        tenant: &str,
        id: Option<&str>,
    ) -> Result<Option<Pid>, RsmError> {
        let Some(id) = id else { return Ok(None) };
        if let Ok(p) = id.parse::<u64>() {
            return Ok(Some(p));
        }
        let uuid = uuid_string_to_bytes(id);
        let store = self.store.clone();
        let tenant = tenant.to_string();
        tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut found = None;
                r.scan_raw(Keyspace::Partitions, &[], &[], usize::MAX, &mut |k, v| {
                    if let (Some(pid), Ok(p)) = (keys::pid_of(k), rows::partition_decode(v)) {
                        if p.tenant == tenant && Some(p.uuid) == uuid {
                            found = Some(pid);
                            return false;
                        }
                    }
                    true
                })?;
                Ok(found)
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("partition lookup: {e}")))?
        .map_err(read_error)
    }

    async fn collect_records(
        &self,
        tenant: &str,
        queue: Option<&str>,
        partition: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Record>, RsmError> {
        let parts = self.parts(tenant, queue, partition).await?;
        let qlog = self.qlog_reader.clone();
        let reader = self.reader.clone();
        let tenant = tenant.to_string();
        let mut records = tokio::task::spawn_blocking(move || {
            let mut out = Vec::new();
            for p in parts {
                let high = (p.row.last_offset + 1).max(0) as u64;
                walk_records(
                    &reader,
                    qlog.as_ref(),
                    &tenant,
                    &p,
                    p.row.log_start,
                    high,
                    limit.saturating_sub(out.len()),
                    |r| {
                        out.push(r);
                        out.len() < limit
                    },
                )?;
                if out.len() >= limit {
                    break;
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| RsmError::Internal(format!("payload read task: {e}")))??;
        for record in &mut records {
            decrypt_record(&self.encryption, record);
        }
        Ok(records)
    }

    pub(super) async fn parts(
        &self,
        tenant: &str,
        queue: Option<&str>,
        partition: Option<&str>,
    ) -> Result<Vec<Part>, RsmError> {
        let store = self.store.clone();
        let tenant = tenant.to_string();
        let queue = queue.map(str::to_string);
        let partition = partition.map(str::to_string);
        tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut out = Vec::new();
                let mut qs = Vec::new();
                if let Some(q) = queue {
                    qs.push(q)
                } else {
                    r.scan_queues(&tenant, usize::MAX, &mut |q, _| {
                        qs.push(q.to_string());
                        true
                    })?;
                }
                for q in qs {
                    r.scan_queue_partitions(&tenant, &q, None, usize::MAX, &mut |pid| {
                        if let Ok(Some(row)) = r.partition(pid) {
                            if partition.as_ref().is_none_or(|x| x == &row.partition) {
                                let mut sealed = Vec::new();
                                let _ = r.scan_partition_files(pid, usize::MAX, &mut |f| {
                                    sealed.push(f);
                                    true
                                });
                                out.push(Part { pid, row, sealed });
                            }
                        }
                        true
                    })?;
                }
                Ok(out)
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("partition scan: {e}")))?
        .map_err(read_error)
    }

    async fn find_message(
        &self,
        tenant: &str,
        partition: &str,
        txn: &str,
    ) -> Result<Option<(Part, Record)>, RsmError> {
        let pid = self.resolve_pid(tenant, Some(partition)).await?;
        let Some(pid) = pid else { return Ok(None) };
        let mut parts = self.parts(tenant, None, None).await?;
        let Some(part) = parts.drain(..).find(|p| p.pid == pid) else {
            return Ok(None);
        };
        let qlog = self.qlog_reader.clone();
        let reader = self.reader.clone();
        let tenant = tenant.to_string();
        let txn = txn.to_string();
        let p2 = part.clone();
        let mut rec = tokio::task::spawn_blocking(move || {
            let mut found = None;
            let high = (p2.row.last_offset + 1).max(0) as u64;
            walk_records(
                &reader,
                qlog.as_ref(),
                &tenant,
                &p2,
                p2.row.log_start,
                high,
                usize::MAX,
                |r| {
                    if r.txn == txn {
                        found = Some(r);
                        false
                    } else {
                        true
                    }
                },
            )?;
            Ok(found)
        })
        .await
        .map_err(|e| RsmError::Internal(format!("message read task: {e}")))??;
        if let Some(record) = &mut rec {
            decrypt_record(&self.encryption, record);
        }
        Ok(rec.map(|r| (part, r)))
    }
}

pub(super) fn walk_records(
    reader: &crate::rsm::segments::Reader,
    qlog: Option<&QLogReader>,
    tenant: &str,
    part: &Part,
    mut off: u64,
    high: u64,
    limit: usize,
    mut cb: impl FnMut(Record) -> bool,
) -> Result<(), RsmError> {
    let qid = qlog.map(|_| QLogReader::queue_id_of(tenant, &part.row.queue));
    let mut n = 0usize;
    while off < high && n < limit {
        let got = match (qlog, qid) {
            (Some(q), Some(id)) => q
                .read_owned(id, part.pid, off)
                .map_err(|e| RsmError::Internal(format!("qlog read: {e}")))?
                .map(|r| (r.base_offset, r.created_at_us, r.count, r.payload)),
            _ => reader
                .read_at_within(
                    bucket_of(tenant, &part.row.queue, &part.row.partition),
                    part.pid,
                    off,
                    &part.sealed,
                    None,
                )
                .map_err(|e| RsmError::Internal(format!("segment read: {e}")))?
                .map(|r| (r.base_offset, r.created_at_us, r.count, r.blob)),
        };
        let Some((base, created, count, blob)) = got else {
            off += 1;
            continue;
        };
        if let Some(frames) = unpack_frames_ref(&blob) {
            for (i, f) in frames.into_iter().enumerate() {
                let pos = base + i as u64;
                if pos < off || pos >= high {
                    continue;
                }
                let rec = Record {
                    queue: part.row.queue.clone(),
                    partition: part.row.partition.clone(),
                    partition_id: part.row.uuid,
                    offset: pos,
                    segment_base: base,
                    frame_idx: i,
                    created_at_us: created,
                    id: f.message_id,
                    txn: f.txn.to_string(),
                    trace_id: f.trace_id,
                    producer_sub: f.producer_sub.map(str::to_string),
                    payload: f.payload.to_vec(),
                    encrypted: f.encrypted,
                };
                n += 1;
                if !cb(rec) {
                    return Ok(());
                }
                if n >= limit {
                    break;
                }
            }
        }
        off = base.saturating_add(count as u64).max(off + 1)
    }
    Ok(())
}

pub(super) fn payload_json(b: &[u8]) -> Value {
    if b.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(b)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(b).into_owned()))
    }
}

fn decrypt_record(encryption: &crate::encryption::Encryption, record: &mut Record) {
    if record.encrypted {
        if let Some(payload) = encryption.decrypt_payload_bytes(&record.payload) {
            record.payload = payload;
        }
    }
}
fn message_json(r: Record) -> Value {
    let txn_hash = hex::encode(crate::util::txn_hash128(&r.txn));
    json!({"id":uuid_bytes_to_string(&r.id),"transactionId":r.txn,"txnHash":txn_hash,"data":payload_json(&r.payload),"payload":payload_json(&r.payload),"traceId":r.trace_id.map(|x|uuid_bytes_to_string(&x)),"producerSub":r.producer_sub,"createdAt":crate::rsm::planner::timers::iso_us(r.created_at_us),"offset":r.offset,"queue":r.queue,"partition":r.partition,"partitionId":uuid_bytes_to_string(&r.partition_id),"status":"pending","segment":{"seq":r.segment_base,"frameIdx":r.frame_idx},"isEncrypted":r.encrypted})
}

pub(super) fn scan_dlq_rows<R: Reads + ?Sized>(
    r: &R,
    tenant: &str,
    queue: Option<&str>,
    group: Option<&str>,
) -> crate::rsm::store::Result<Vec<(String, String, [u8; 16], DlqRow)>> {
    let prefix = keys::queues_prefix(tenant);
    let mut out = Vec::new();
    let mut err = None;
    r.scan_raw(
        Keyspace::Dlq,
        &prefix,
        &prefix,
        usize::MAX,
        &mut |k, v| match (keys::dlq_parts(k), rows::dlq_decode(v)) {
            (Some((t, q, id)), Ok(row))
                if queue.is_none_or(|x| x == q) && group.is_none_or(|x| x == row.group) =>
            {
                out.push((t, q, id, row));
                true
            }
            (Some(_), Ok(_)) => true,
            _ => {
                err = Some(crate::rsm::store::StoreError::corrupt(
                    Keyspace::Dlq,
                    "dlq row",
                ));
                false
            }
        },
    )?;
    match err {
        Some(e) => Err(e),
        None => Ok(out),
    }
}
fn dlq_json(
    (_t, q, id, r): (String, String, [u8; 16], DlqRow),
    partition: Option<&(String, [u8; 16])>,
) -> Value {
    json!({"id":uuid_bytes_to_string(&id),"queue":q,"partition":partition.map(|p|p.0.clone()),"partitionId":r.pid.to_string(),"consumerGroup":r.group,"offset":r.offset,"messageId":r.message_id.map(|x|uuid_bytes_to_string(&x)),"transactionId":r.txn,"data":payload_json(&r.payload),"payload":payload_json(&r.payload),"errorMessage":r.error,"retryCount":r.retry_count,"failedAt":crate::rsm::planner::timers::iso_us(r.failed_at_us)})
}

fn group_detail<R: Reads + ?Sized>(
    r: &R,
    tenant: &str,
    group: &str,
    now: i64,
) -> crate::rsm::store::Result<Value> {
    let mut queues = Vec::new();
    r.scan_queues(tenant, usize::MAX, &mut |q, _| {
        queues.push(q.to_string());
        true
    })?;
    let mut answer = serde_json::Map::new();
    for queue in queues {
        let meta = r.group(tenant, &queue, group)?;
        let mut partitions = Vec::new();
        r.scan_queue_partitions(tenant, &queue, None, usize::MAX, &mut |pid| {
            if let (Ok(Some(p)), Ok(Some(c))) = (r.partition(pid), r.cursor(pid, group)) {
                let lag = p.pending_from(c.committed);
                let lag_seconds = if lag == 0 {
                    0
                } else {
                    (now - p.oldest_live_at_us.unwrap_or(p.last_write_at_us)).max(0) / 1_000_000
                };
                partitions.push(json!({
                    "partition":p.partition,
                    "workerId":c.worker,
                    "lastConsumedAt":Value::Null,
                    "totalConsumed":c.total_consumed,
                    "offsetLag":lag,
                    "timeLagSeconds":lag_seconds,
                    "leaseActive":rows::lease_live(&c, now)
                }));
            }
            true
        })?;
        if !partitions.is_empty() {
            partitions.sort_by(|a, b| a["partition"].as_str().cmp(&b["partition"].as_str()));
            answer.insert(
                queue,
                json!({
                    "conflation":meta.as_ref().is_some_and(|m| m.meta.conflation),
                    "kind":"queen",
                    "partitions":partitions
                }),
            );
        }
    }
    Ok(Value::Object(answer))
}

fn lagging_partitions<R: Reads + ?Sized>(
    r: &R,
    tenant: &str,
    min_lag_seconds: i64,
    now: i64,
) -> crate::rsm::store::Result<Vec<Value>> {
    let mut queues = Vec::new();
    r.scan_queues(tenant, usize::MAX, &mut |q, _| {
        queues.push(q.to_string());
        true
    })?;
    let mut answer = Vec::new();
    for queue in queues {
        r.scan_queue_partitions(tenant, &queue, None, usize::MAX, &mut |pid| {
            if let Ok(Some(p)) = r.partition(pid) {
                let _ = r.scan_cursors(pid, usize::MAX, &mut |group, c| {
                    if group == "__QUEUE_MODE__" {
                        return true;
                    }
                    let offset_lag = p.pending_from(c.committed);
                    if offset_lag == 0 {
                        return true;
                    }
                    let oldest_us = p.oldest_live_at_us.unwrap_or(p.last_write_at_us);
                    // SQL rounds this age to whole seconds and applies a strict
                    // greater-than filter. Keep a newly-created backlog visible
                    // for minLagSeconds=0 even inside its first second.
                    let time_lag_seconds = ((now - oldest_us).max(0) / 1_000_000).max(1);
                    if time_lag_seconds <= min_lag_seconds {
                        return true;
                    }
                    answer.push(json!({
                        "consumer_group":group,
                        "queue_name":queue,
                        "partition_name":p.partition,
                        "partition_id":uuid_bytes_to_string(&p.uuid),
                        "worker_id":c.worker,
                        "kind":"queen",
                        "offset_lag":offset_lag,
                        "time_lag_seconds":time_lag_seconds,
                        "lag_hours":time_lag_seconds as f64 / 3600.0,
                        "oldest_unconsumed_at":crate::rsm::planner::timers::iso_us(oldest_us),
                        "last_consumed_at":Value::Null
                    }));
                    true
                });
            }
            true
        })?;
    }
    answer.sort_by(|a, b| {
        b["time_lag_seconds"]
            .as_i64()
            .cmp(&a["time_lag_seconds"].as_i64())
    });
    Ok(answer)
}

fn group_view<R: Reads + ?Sized>(
    r: &R,
    tenant: &str,
    only: Option<&str>,
    now: i64,
) -> crate::rsm::store::Result<Vec<Value>> {
    let mut out = Vec::new();
    let mut qs = Vec::new();
    r.scan_queues(tenant, usize::MAX, &mut |q, _| {
        qs.push(q.to_string());
        true
    })?;
    for q in qs {
        let mut metas = HashMap::new();
        r.scan_groups(tenant, &q, usize::MAX, &mut |g, row| {
            if only.is_none_or(|x| x == g) {
                metas.insert(g.to_string(), row);
            }
            true
        })?;
        let mut curs: HashMap<String, Vec<(PartitionRow, crate::rsm::effect::CursorRow)>> =
            HashMap::new();
        r.scan_queue_partitions(tenant, &q, None, usize::MAX, &mut |pid| {
            if let Ok(Some(p)) = r.partition(pid) {
                let _ = r.scan_cursors(pid, usize::MAX, &mut |g, c| {
                    if g != "__QUEUE_MODE__" && only.is_none_or(|x| x == g) {
                        curs.entry(g.to_string()).or_default().push((p.clone(), c));
                    }
                    true
                });
            }
            true
        })?;
        for (g, rows) in curs {
            let pending: usize = rows
                .iter()
                .map(|(p, c)| p.pending_from(c.committed) as usize)
                .sum();
            let processing = rows
                .iter()
                .filter(|(_, c)| c.lease_expires_at_us.is_some_and(|x| x > now))
                .count();
            let last = rows
                .iter()
                .map(|(p, _)| p.last_write_at_us)
                .max()
                .unwrap_or(0);
            let m = metas.remove(&g);
            out.push(json!({"name":g,"consumerGroup":g,"queue":q,"kind":"queen","partitions":rows.len(),"pending":pending,"processing":processing,"lag":pending,"lagSeconds":if pending>0{(now-last).max(0)/1_000_000}else{0},"subscriptionMode":m.as_ref().map(|x|format!("{:?}",x.meta.mode).to_lowercase()),"subscriptionTimestamp":m.map(|x|crate::rsm::planner::timers::iso_us(x.meta.subscription_timestamp_us))}));
        }
        for (g, m) in metas {
            out.push(json!({"name":g,"consumerGroup":g,"queue":q,"kind":"queen","partitions":0,"pending":0,"processing":0,"lag":0,"lagSeconds":0,"subscriptionMode":format!("{:?}",m.meta.mode).to_lowercase(),"subscriptionTimestamp":crate::rsm::planner::timers::iso_us(m.meta.subscription_timestamp_us)}));
        }
    }
    out.sort_by(|a, b| {
        (a["consumerGroup"].as_str(), a["queue"].as_str())
            .cmp(&(b["consumerGroup"].as_str(), b["queue"].as_str()))
    });
    Ok(out)
}

fn trace_json(e: TraceEvent, location: Option<(String, String)>) -> Value {
    let partition_id = e.pid.map(|p| p.to_string());
    let message_id = e.message_id.map(|x| uuid_bytes_to_string(&x));
    let created_at = crate::rsm::planner::timers::iso_us(e.created_at_us);
    let data = serde_json::from_slice::<Value>(&e.data).unwrap_or(Value::Null);
    let (queue_name, partition_name) = location
        .map(|(q, p)| (Some(q), Some(p)))
        .unwrap_or((None, None));
    json!({
        "traceId":uuid_bytes_to_string(&e.trace_id),
        "trace_id":uuid_bytes_to_string(&e.trace_id),
        "partitionId":partition_id,
        "partition_id":partition_id,
        "messageId":message_id,
        "message_id":message_id,
        "transactionId":e.txn,
        "transaction_id":e.txn,
        "consumerGroup":e.consumer_group,
        "consumer_group":e.consumer_group,
        "eventType":e.event_type,
        "event_type":e.event_type,
        "data":data,
        "worker":e.worker,
        "worker_id":e.worker,
        "names":e.names,
        "trace_names":e.names,
        "createdAt":created_at,
        "created_at":created_at,
        "queue_name":queue_name,
        "partition_name":partition_name,
        "message_payload":Value::Null
    })
}

fn parse_changed_cursor(raw: Option<&str>, mode: char) -> Option<(char, i64, String)> {
    match raw {
        None => Some((mode, 0, String::new())),
        Some(s) if mode == 'n' => s.strip_prefix("n|").map(|n| ('n', 0, n.to_string())),
        Some(s) => {
            let rest = s.strip_prefix("t|")?;
            let (ts, name) = rest.split_once('|')?;
            Some(('t', ts.parse().ok()?, name.to_string()))
        }
    }
}
