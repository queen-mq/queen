//! Ordered admin mutations and counter-backed status views.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use super::reads::{scan_dlq_rows, walk_records};
use super::{query_map, read_error, ApiOut, Effect, RaftFacade, ReqCtx, RsmError};
use crate::frames::{pack_frames, uuid_bytes_to_string, uuid_string_to_bytes, FrameIn};
use crate::rsm::batcher::Command;
use crate::rsm::effect::{CursorRow, SubscriptionMode};
use crate::rsm::entry::PushVerdict;
use crate::rsm::planner::txn::TxnCommand;
use crate::rsm::planner::{PushCommand, PushItem};
use crate::rsm::replicator::Replicator;
use crate::rsm::store::rows::cursor_fresh;
use crate::rsm::store::{Store, TypedReads};
use crate::util::{txn_hash128, uuidv7_bytes};

impl RaftFacade {
    pub(super) async fn api_group_delete(
        &self,
        ctx: ReqCtx,
        group: &str,
        queue: Option<&str>,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let delete_meta = query_map(query)
            .get("deleteMetadata")
            .is_none_or(|v| v != "false");
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let group_s = group.to_string();
        let queue_s = queue.map(str::to_string);
        let (mut effects, count, queues) = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut effects = Vec::new();
                let mut count = 0;
                let mut queues = Vec::new();
                if let Some(q) = queue_s {
                    queues.push(q)
                } else {
                    r.scan_queues(&tenant, usize::MAX, &mut |q, _| {
                        queues.push(q.to_string());
                        true
                    })?;
                }
                for q in &queues {
                    if delete_meta && r.group(&tenant, q, &group_s)?.is_some() {
                        effects.push(Effect::GroupDelete {
                            tenant: tenant.clone(),
                            queue: q.clone(),
                            group: group_s.clone(),
                        });
                    }
                    r.scan_queue_partitions(&tenant, q, None, usize::MAX, &mut |pid| {
                        if r.cursor(pid, &group_s).ok().flatten().is_some() {
                            effects.push(Effect::CursorDelete {
                                pid,
                                group: group_s.clone(),
                            });
                            count += 1;
                        }
                        true
                    })?;
                }
                Ok((effects, count, queues))
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("group delete read: {e}")))?
        .map_err(read_error)?;
        if effects.is_empty() {
            effects.push(Effect::Noop)
        }
        self.submit_effects(&ctx, effects).await?;
        Ok(ApiOut::json(200,json!({"success":true,"consumerGroup":group,"queueName":queue,"deletedPartitions":count,"metadataDeleted":delete_meta,"queues":queues}).to_string()))
    }

    pub(super) async fn api_group_subscription(
        &self,
        ctx: ReqCtx,
        group: &str,
        body: &[u8],
    ) -> Result<ApiOut, RsmError> {
        let v: Value = serde_json::from_slice(body).map_err(|e| super::rejected("bad_body", e))?;
        let timestamp = v
            .get("subscriptionTimestamp")
            .and_then(Value::as_str)
            .ok_or_else(|| super::reject("bad_request", "subscriptionTimestamp is required"))?
            .to_string();
        let ts = crate::util::parse_iso_ms(&timestamp)
            .ok_or_else(|| super::reject("bad_request", "subscriptionTimestamp is invalid"))?
            * 1000;
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let group_s = group.to_string();
        let now = super::super::wall_micros();
        let effects = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut effects = Vec::new();
                let mut qs = Vec::new();
                r.scan_queues(&tenant, usize::MAX, &mut |q, _| {
                    qs.push(q.to_string());
                    true
                })?;
                for q in qs {
                    if let Some(row) = r.group(&tenant, &q, &group_s)? {
                        let mut meta = row.meta;
                        meta.mode = SubscriptionMode::Timestamp;
                        meta.subscription_timestamp_us = ts;
                        meta.registered_at_us = meta.registered_at_us.min(now);
                        effects.push(Effect::GroupUpsert {
                            tenant: tenant.clone(),
                            queue: q,
                            group: group_s.clone(),
                            meta,
                        });
                    }
                }
                Ok(effects)
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("subscription read: {e}")))?
        .map_err(read_error)?;
        let rows_updated = effects.len();
        if !effects.is_empty() {
            self.submit_effects(&ctx, effects).await?;
        }
        Ok(ApiOut::json(200,json!({"success":true,"consumerGroup":group,"newTimestamp":timestamp,"rowsUpdated":rows_updated}).to_string()))
    }

    pub(super) async fn api_group_seek(
        &self,
        ctx: ReqCtx,
        group: &str,
        queue: &str,
        partition: Option<&str>,
        body: &[u8],
    ) -> Result<ApiOut, RsmError> {
        let v: Value = if body.is_empty() {
            json!({"toEnd":true})
        } else {
            serde_json::from_slice(body).map_err(|e| super::rejected("bad_body", e))?
        };
        let to_end = v.get("toEnd").and_then(Value::as_bool) == Some(true);
        let ts = v
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(crate::util::parse_iso_ms)
            .map(|x| x * 1000);
        if !to_end && ts.is_none() {
            return Ok(ApiOut::json(
                400,
                json!({"success":false,"error":"Must specify toEnd=true or a timestamp"})
                    .to_string(),
            ));
        }
        let parts = self.parts(&ctx.tenant, Some(queue), partition).await?;
        if parts.is_empty() {
            return Ok(ApiOut::json(
                404,
                json!({
                    "success": false,
                    "error": if partition.is_some() { "Partition not found" } else { "Queue not found or has no partitions" },
                    "consumerGroup": group,
                    "queueName": queue,
                    "partitionsUpdated": 0,
                })
                .to_string(),
            ));
        }
        let qlog = self.qlog_reader.clone();
        let reader = self.reader.clone();
        let tenant = ctx.tenant.clone();
        let group_s = group.to_string();
        let store = self.store.clone();
        let now = super::super::wall_micros();
        let effects = tokio::task::spawn_blocking(move || {
            let mut effects = Vec::new();
            for part in parts {
                let committed = if to_end {
                    part.row.last_offset
                } else {
                    let mut first = None;
                    walk_records(
                        &reader,
                        qlog.as_ref(),
                        &tenant,
                        &part,
                        part.row.log_start,
                        (part.row.last_offset + 1).max(0) as u64,
                        usize::MAX,
                        |r| {
                            if r.created_at_us >= ts.unwrap() {
                                first = Some(r.offset);
                                false
                            } else {
                                true
                            }
                        },
                    )?;
                    first.map(|o| o as i64 - 1).unwrap_or(part.row.last_offset)
                };
                let mut row = store
                    .read(|r| r.cursor(part.pid, &group_s))
                    .map_err(read_error)?
                    .unwrap_or_else(|| cursor_fresh(-1, now));
                clear_cursor_lease(&mut row);
                row.committed = committed.max(part.row.floor());
                effects.push(Effect::CursorSet {
                    pid: part.pid,
                    group: group_s.clone(),
                    row,
                });
            }
            Ok::<_, RsmError>(effects)
        })
        .await
        .map_err(|e| RsmError::Internal(format!("seek task: {e}")))??;
        let count = effects.len();
        self.submit_effects(&ctx, effects).await?;
        Ok(ApiOut::json(200,json!({"success":true,"consumerGroup":group,"queueName":queue,"partitionName":partition,"seekToEnd":to_end,"targetTimestamp":ts.map(crate::rsm::planner::timers::iso_us),"partitionsUpdated":count,"updated":true}).to_string()))
    }

    pub(super) async fn api_dlq_purge(
        &self,
        ctx: ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let q = query_map(query);
        let Some(queue) = q.get("queue").filter(|v| !v.is_empty()).cloned() else {
            return Ok(ApiOut::json(400,json!({"success":false,"error":"queue is required","message":"Bulk DLQ purge requires an exact queue name"}).to_string()));
        };
        let group = q.get("consumerGroup").cloned();
        let group_read = group.clone();
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let queue2 = queue.clone();
        let effects = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                Ok(
                    scan_dlq_rows(r, &tenant, Some(&queue2), group_read.as_deref())?
                        .into_iter()
                        .map(|(_, queue, id, _)| Effect::DlqDelete {
                            dlq_id: id,
                            tenant: tenant.clone(),
                            queue,
                        })
                        .collect::<Vec<_>>(),
                )
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("dlq purge read: {e}")))?
        .map_err(read_error)?;
        let n = effects.len();
        if !effects.is_empty() {
            self.submit_effects(&ctx, effects).await?;
        }
        Ok(ApiOut::json(
            200,
            json!({"success":true,"queue":queue,"consumerGroup":group,"deleted":n}).to_string(),
        ))
    }

    pub(super) async fn api_message_delete(
        &self,
        ctx: ReqCtx,
        partition: &str,
        txn: &str,
    ) -> Result<ApiOut, RsmError> {
        let Some(pid) = self.resolve_pid(&ctx.tenant, Some(partition)).await? else {
            return Ok(message_delete_miss(partition, txn));
        };
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let txn_s = txn.to_string();
        let effects = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let Some(p) = r.partition(pid)? else {
                    return Ok(Vec::new());
                };
                Ok(scan_dlq_rows(r, &tenant, Some(&p.queue), None)?
                    .into_iter()
                    .filter(|(_, _, _, d)| d.pid == pid && d.txn == txn_s)
                    .map(|(_, queue, id, _)| Effect::DlqDelete {
                        dlq_id: id,
                        tenant: tenant.clone(),
                        queue,
                    })
                    .collect::<Vec<_>>())
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("message delete read: {e}")))?
        .map_err(read_error)?;
        if effects.is_empty() {
            return Ok(message_delete_miss(partition, txn));
        }
        let n = effects.len();
        self.submit_effects(&ctx, effects).await?;
        Ok(ApiOut::json(
            200,
            json!({"success":true,"partitionId":partition,"transactionId":txn,"deleted":n})
                .to_string(),
        ))
    }

    pub(super) async fn api_dlq_move(
        &self,
        ctx: ReqCtx,
        id: Option<&str>,
        address: Option<(&str, &str)>,
        body: &[u8],
    ) -> Result<ApiOut, RsmError> {
        let store = self.store.clone();
        let maintenance = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                Ok(r.flag("maintenance_mode")?
                    .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                    .and_then(|value| value.get("enabled").and_then(Value::as_bool))
                    .unwrap_or(false))
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("maintenance read: {e}")))?
        .map_err(read_error)?;
        if maintenance {
            return Ok(ApiOut::json(
                503,
                json!({"success":false,"error":"Maintenance mode is enabled; replay is paused"})
                    .to_string(),
            ));
        }
        let target: Value = if body.iter().all(u8::is_ascii_whitespace) {
            json!({})
        } else {
            serde_json::from_slice(body).map_err(|e| super::rejected("bad_body", e))?
        };
        let queue_override = target.get("queue").and_then(Value::as_str).map(str::trim);
        let part_override = target
            .get("partition")
            .and_then(Value::as_str)
            .map(str::trim);
        if queue_override.is_some_and(str::is_empty) || part_override.is_some_and(str::is_empty) {
            return Ok(ApiOut::json(
                400,
                json!({"success":false,"error":"queue and partition overrides must be non-empty"})
                    .to_string(),
            ));
        }

        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let idb = id.and_then(uuid_string_to_bytes);
        let addr_pid = address.and_then(|(p, _)| p.parse::<u64>().ok());
        let addr_uuid = address.and_then(|(p, _)| uuid_string_to_bytes(p));
        let addr_txn = address.map(|(_, t)| t.to_string());
        let found = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                for (_, source_queue, row_id, row) in scan_dlq_rows(r, &tenant, None, None)? {
                    let Some(source_part) = r.partition(row.pid)? else {
                        continue;
                    };
                    let id_match = idb.is_some_and(|x| x == row_id);
                    let address_match = addr_txn.as_ref().is_some_and(|t| t == &row.txn)
                        && (addr_pid.is_some_and(|p| p == row.pid)
                            || addr_uuid.is_some_and(|p| p == source_part.uuid));
                    if id_match || address_match {
                        return Ok(Some((source_queue, source_part.partition, row_id, row)));
                    }
                }
                Ok(None)
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("dlq move read: {e}")))?
        .map_err(read_error)?;
        let Some((source_queue, source_partition, row_id, row)) = found else {
            return Ok(ApiOut::json(
                404,
                json!({"success":false,"result":"gone","error":"Message not found"}).to_string(),
            ));
        };

        let queue = queue_override.unwrap_or(&source_queue).to_string();
        let partition = part_override.unwrap_or(&source_partition).to_string();
        crate::rsm::facade::check_message_key_names(&ctx.tenant, &queue, None, Some(&partition))?;
        let message_id = uuidv7_bytes();
        let transaction_id = format!("dlq:{}", uuid_bytes_to_string(&row_id));
        let encrypted_queues = self
            .encrypted_queues(&ctx.tenant, std::iter::once(queue.clone()).collect())
            .await?;
        // New rows already hold plaintext snapshots; sniffing also makes rows
        // written by an older raft build replay safely after this upgrade.
        let plain = self
            .encryption
            .decrypt_payload_bytes(&row.payload)
            .unwrap_or(row.payload);
        let (payload, encrypted) =
            self.encode_payload(encrypted_queues.contains(&queue), &plain, &queue);
        let frame = pack_frames(&[FrameIn {
            message_id,
            txn: &transaction_id,
            trace_id: None,
            producer_sub: None,
            payload: &payload,
            encrypted,
        }]);
        let push = PushCommand {
            request_id: ctx.request_id,
            tenant: ctx.tenant.clone(),
            queue: queue.clone(),
            partition: partition.clone(),
            items: vec![PushItem {
                hash: txn_hash128(&transaction_id),
                frame,
            }],
            create_cfg: super::super::default_queue_config(&queue),
        };
        let cmd = Command::Transaction(TxnCommand {
            request_id: ctx.request_id,
            tenant: ctx.tenant.clone(),
            pushes: vec![push],
            acks: Vec::new(),
            positional_acks: Vec::new(),
            kv: Vec::new(),
            timers: Vec::new(),
            extra_effects: vec![Effect::DlqDelete {
                dlq_id: row_id,
                tenant: ctx.tenant.clone(),
                queue: source_queue,
            }],
            allow_duplicate: true,
        });
        let out = match self.submit(&ctx, cmd).await? {
            super::Reply::Done { outcome, .. } => {
                crate::rsm::batcher::TxnOutcome::from_outcome(&outcome).ok_or_else(|| {
                    RsmError::Internal("DLQ move got non-transaction outcome".into())
                })?
            }
            super::Reply::Refused(r) => return Ok(ApiOut::json(
                409,
                json!({"success":false,"error":r.message,"reason":r.code,"dlqRowRemoved":false})
                    .to_string(),
            )),
            super::Reply::Retry { hint } => {
                return Err(RsmError::Retry {
                    leader_hint: hint.map(|h| h.to_string()),
                })
            }
        };
        let verdict = out
            .pushes
            .first()
            .and_then(|p| p.items.first())
            .ok_or_else(|| RsmError::Internal("DLQ move returned no push verdict".into()))?;
        let (result, offset, moved) =
            match verdict {
                PushVerdict::Created { offset, .. } => ("moved", *offset, true),
                PushVerdict::Duplicate { offset, .. } => ("duplicate", *offset, false),
                PushVerdict::Refused { code, message } => return Ok(ApiOut::json(
                    409,
                    json!({"success":false,"error":message,"reason":code,"dlqRowRemoved":false})
                        .to_string(),
                )),
            };
        Ok(ApiOut::json(200, json!({
            "success":true,
            "result":result,
            "queue":queue,
            "partition":partition,
            "consumerGroup":row.group,
            "dlqId":uuid_bytes_to_string(&row_id),
            "originalTransactionId":row.txn,
            "replayedAs":{
                "index":0,
                "message_id":if moved { uuid_bytes_to_string(&message_id) } else { uuid_bytes_to_string(&[0;16]) },
                "transaction_id":transaction_id,
                "queueName":queue,
                "status":if moved { "queued" } else { "duplicate" },
                "offset":offset,
            },
            "dlqRowRemoved":moved,
        }).to_string()))
    }

    pub(super) async fn api_flag_get(&self, key: &str) -> Result<ApiOut, RsmError> {
        let store = self.store.clone();
        let key = key.to_string();
        let values = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let get = |key: &str, default: bool| -> crate::rsm::store::Result<bool> {
                    Ok(r.flag(key)?
                        .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
                        .and_then(|v| v.get("enabled").and_then(Value::as_bool))
                        .unwrap_or(default))
                };
                Ok((
                    get("maintenance_mode", false)?,
                    get("pop_maintenance_mode", false)?,
                    get(crate::switches::Switches::KEY_KV, true)?,
                    get(crate::switches::Switches::KEY_TIMERS_SCHEDULE, true)?,
                    get(crate::switches::Switches::KEY_TIMERS_FIRE, true)?,
                    get(crate::switches::Switches::KEY_EPHEMERAL, true)?,
                ))
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("flag read: {e}")))?
        .map_err(read_error)?;
        let out = match key.as_str() {
            "maintenance_mode" => {
                json!({"maintenanceMode":values.0,"popMaintenanceMode":values.1,"bufferedMessages":0,"bufferHealthy":true})
            }
            "pop_maintenance_mode" => json!({"popMaintenanceMode":values.1}),
            "kv_timers_enabled" => {
                json!({"kvEnabled":values.2,"timersScheduleEnabled":values.3,"timersFireEnabled":values.4,"quotaTenants":0,"quotaAgeMs":Value::Null,"quotaHot":0})
            }
            "ephemeral_enabled" => json!({"ephemeralEnabled":values.5}),
            _ => json!({"enabled":true}),
        };
        Ok(ApiOut::json(200, out.to_string()))
    }

    pub(super) async fn api_flag_set(
        &self,
        ctx: ReqCtx,
        key: &str,
        body: &[u8],
    ) -> Result<ApiOut, RsmError> {
        let mut v: Value =
            serde_json::from_slice(body).map_err(|e| super::rejected("bad_body", e))?;
        let mut effects = Vec::new();
        if key == "kv_timers_enabled" {
            let Some(o) = v.as_object() else {
                return Ok(ApiOut::json(
                    400,
                    json!({"error":"object required"}).to_string(),
                ));
            };
            for (field, stored) in [
                ("kv", crate::switches::Switches::KEY_KV),
                (
                    "timersSchedule",
                    crate::switches::Switches::KEY_TIMERS_SCHEDULE,
                ),
                ("timersFire", crate::switches::Switches::KEY_TIMERS_FIRE),
            ] {
                if let Some(value) = o.get(field) {
                    let Some(enabled) = value.as_bool() else {
                        return Ok(ApiOut::json(
                            400,
                            json!({"error":format!("{field} must be a boolean")}).to_string(),
                        ));
                    };
                    effects.push(Effect::FlagSet {
                        key: stored.to_string(),
                        value: serde_json::to_vec(&json!({"enabled":enabled})).unwrap(),
                    });
                }
            }
            if effects.is_empty() {
                return Ok(ApiOut::json(400,json!({"error":"at least one of kv, timersSchedule, timersFire (boolean) is required"}).to_string()));
            }
        } else {
            let Some(enabled) = v.get("enabled").and_then(Value::as_bool) else {
                return Ok(ApiOut::json(
                    400,
                    json!({"error":"enabled (boolean) is required"}).to_string(),
                ));
            };
            effects.push(Effect::FlagSet {
                key: key.to_string(),
                value: serde_json::to_vec(&json!({"enabled":enabled})).unwrap(),
            });
        }
        self.submit_effects(&ctx, effects).await?;
        if key == "maintenance_mode" {
            v = json!({"maintenanceMode":v["enabled"],"bufferedMessages":0,"bufferHealthy":true,"mirrored":true})
        } else if key == "pop_maintenance_mode" {
            v = json!({"popMaintenanceMode":v["enabled"],"mirrored":true})
        } else if key == "kv_timers_enabled" {
            return self.api_flag_get(key).await;
        } else if key == "ephemeral_enabled" {
            v = json!({"ephemeralEnabled":v["enabled"],"mirrored":true})
        }
        Ok(ApiOut::json(200, v.to_string()))
    }

    pub(super) async fn api_ephemeral_configure(
        &self,
        ctx: ReqCtx,
        body: &[u8],
    ) -> Result<ApiOut, RsmError> {
        let v: Value = serde_json::from_slice(body).map_err(|e| super::rejected("bad_body", e))?;
        let queue = v
            .get("queue")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| super::reject("bad_request", "queue is required"))?;
        let opts = v.get("options").cloned().unwrap_or_else(|| json!({}));
        self.submit_effects(
            &ctx,
            vec![Effect::EphemeralConfigSet {
                tenant: ctx.tenant.clone(),
                queue: queue.to_string(),
                options: serde_json::to_vec(&opts).unwrap(),
                updated_at_us: super::super::wall_micros(),
            }],
        )
        .await?;
        Ok(ApiOut::json(
            201,
            json!({"queue":queue,"options":opts,"declared":true}).to_string(),
        ))
    }
    pub(super) async fn api_ephemeral_delete(
        &self,
        ctx: ReqCtx,
        queue: &str,
    ) -> Result<ApiOut, RsmError> {
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let q = queue.to_string();
        let declared = tokio::task::spawn_blocking(move || {
            store.read(|r| r.eph_config(&tenant, &q).map(|x| x.is_some()))
        })
        .await
        .map_err(|e| RsmError::Internal(format!("ephemeral config read: {e}")))?
        .map_err(read_error)?;
        if declared {
            self.submit_effects(
                &ctx,
                vec![Effect::EphemeralConfigDelete {
                    tenant: ctx.tenant.clone(),
                    queue: queue.to_string(),
                }],
            )
            .await?;
        }
        Ok(ApiOut::json(
            200,
            json!({"queue":queue,"deleted":declared,"declared":declared}).to_string(),
        ))
    }

    pub(super) async fn api_shared_state(&self, ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let applied = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                Ok((
                    r.applied_index()?,
                    r.durable_index()?,
                    r.scan_queues(&tenant, usize::MAX, &mut |_, _| true)?,
                ))
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("shared state: {e}")))?
        .map_err(read_error)?;
        Ok(ApiOut::json(200,json!({"enabled":true,"reason":"raft_state_machine","appliedIndex":applied.0,"durableIndex":applied.1,"queues":applied.2}).to_string()))
    }

    /// `GET /api/v1/raft/status` — this node's block of the Raft view plus the
    /// cluster's name, leader, size (§14.6).
    pub(super) async fn api_raft_status(&self, _ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let mut v = local_node_json(&self.repl, &*self.store);
        let view = self.repl.cluster_view();
        let (cluster, voters) = cluster_identity(view.as_ref(), &v);
        if let Some(o) = v.as_object_mut() {
            o.insert("engine".into(), json!("raft"));
            o.insert("clusterId".into(), json!(cluster));
            o.insert("self".into(), o.get("nodeId").cloned().unwrap_or(Value::Null));
            o.insert("voters".into(), json!(voters));
            o.insert("singleNode".into(), json!(voters <= 1));
        }
        Ok(ApiOut::json(200, v.to_string()))
    }

    /// `GET /api/v1/raft/members` — every member of the cluster: this node's
    /// block, and each peer's own block gathered over the Raft RPC port
    /// (`/raft/v1/local`); a peer that does not answer within
    /// [`PEER_GATHER_TTL`] is listed as unreachable. The leader's view of each
    /// follower's replication (`matchIndex`, `lagEntries`, `heartbeatAgeMs`)
    /// is folded in from whichever block is the leader's.
    pub(super) async fn api_raft_members(&self, _ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let local = local_node_json(&self.repl, &*self.store);
        let view = self.repl.cluster_view();
        let (cluster, voters) = cluster_identity(view.as_ref(), &local);
        let mut members: Vec<Value> = match &view {
            None => vec![local.clone()],
            Some(v) => {
                let calls = v.members.iter().map(|m| {
                    let repl = self.repl.clone();
                    let me = m.node_id == v.node_id;
                    let m = m.clone();
                    let local = local.clone();
                    async move {
                        if me {
                            return local;
                        }
                        let body = bytes::Bytes::from_static(br#"{"kind":"node"}"#);
                        match repl
                            .call_peer(&m.raft_addr, "/raft/v1/local", body, PEER_GATHER_TTL)
                            .await
                            .and_then(|b| {
                                serde_json::from_slice::<Value>(&b).map_err(|e| e.to_string())
                            }) {
                            Ok(mut peer) => {
                                if let Some(o) = peer.as_object_mut() {
                                    o.insert("local".into(), json!(false));
                                    o.insert("reachable".into(), json!(true));
                                }
                                peer
                            }
                            Err(e) => json!({
                                "nodeId":m.node_id,
                                "role":if m.voter { "voter" } else { "learner" },
                                "state":"unreachable",
                                "local":false,
                                "reachable":false,
                                "raftAddr":m.raft_addr,
                                "httpAddr":m.http_addr,
                                "hostname":Value::Null,
                                "error":e
                            }),
                        }
                    }
                });
                futures_util::future::join_all(calls).await
            }
        };
        // The leader's replication view, from the leader's own block.
        let leader_view: Option<(u64, Vec<Value>)> = members.iter().find_map(|m| {
            (m.get("state").and_then(Value::as_str) == Some("leader")).then(|| {
                (
                    m.get("lastLogIndex").and_then(Value::as_u64).unwrap_or(0),
                    m.get("replication")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default(),
                )
            })
        });
        for m in &mut members {
            let id = m.get("nodeId").and_then(Value::as_u64);
            let is_leader = m.get("state").and_then(Value::as_str) == Some("leader");
            let (mut matched, mut hb) = (Value::Null, Value::Null);
            let mut lag = Value::Null;
            if let Some((last, rep)) = &leader_view {
                if is_leader {
                    matched = json!(last);
                    lag = json!(0);
                } else if let Some(r) = rep
                    .iter()
                    .find(|r| r.get("nodeId").and_then(Value::as_u64) == id)
                {
                    matched = r.get("matchIndex").cloned().unwrap_or(Value::Null);
                    hb = r.get("heartbeatAgeMs").cloned().unwrap_or(Value::Null);
                    if let Some(mi) = matched.as_u64() {
                        lag = json!(last.saturating_sub(mi));
                    }
                }
            }
            if let Some(o) = m.as_object_mut() {
                o.insert("matchIndex".into(), matched);
                o.insert("heartbeatAgeMs".into(), hb);
                o.insert("lagEntries".into(), lag);
                o.remove("replication");
            }
        }
        members.sort_by_key(|m| m.get("nodeId").and_then(Value::as_u64).unwrap_or(0));
        let leader = view
            .as_ref()
            .and_then(|v| v.leader)
            .or_else(|| {
                local
                    .get("state")
                    .and_then(Value::as_str)
                    .filter(|s| *s == "leader")
                    .and(local.get("nodeId").and_then(Value::as_u64))
            });
        Ok(ApiOut::json(
            200,
            json!({
                "engine":"raft",
                "clusterId":cluster,
                "leaderId":leader,
                "term":local.get("term").cloned().unwrap_or(Value::Null),
                "self":local.get("nodeId").cloned().unwrap_or(Value::Null),
                "voters":voters,
                "members":members
            })
            .to_string(),
        ))
    }

    /// `GET /api/v1/raft/liveness`: the raft cluster's members and how long ago
    /// the LEADER last heard from each (an acknowledged heartbeat or append) —
    /// the liveness a client of any node can judge by, since every node serves
    /// the leader's own observations (a follower a copy it fetched over the
    /// Raft RPC port, `viewAgeMs` old). `lastAckMs` is as of now: the leader's
    /// figure plus the copy's age. Always answered by the node asked (never
    /// forwarded): `nodeId` is that node.
    pub(super) async fn api_raft_liveness(&self, _ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let cm = self.repl.members();
        Ok(ApiOut::json(200, raft_liveness_json(&cm).to_string()))
    }
    pub(super) async fn api_status_queues(&self, ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        Ok(ApiOut::json(
            200,
            json!({"queues":self.queue_snapshots(&ctx.tenant).await?}).to_string(),
        ))
    }
    pub(super) async fn api_status_queue(
        &self,
        ctx: ReqCtx,
        queue: &str,
        _query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        match self.queue_resource_detail(&ctx.tenant, queue).await? {
            Some(v) => {
                let totals = v.get("totals").cloned().unwrap_or_else(|| json!({}));
                let options = v.get("options").cloned().unwrap_or_else(|| json!({}));
                let queue_info = json!({
                    "id":v.get("id"),"name":v.get("name"),
                    "namespace":v.get("namespace"),"task":v.get("task"),
                    "priority":options.get("priority"),"createdAt":v.get("createdAt"),
                    "config":options
                });
                Ok(ApiOut::json(
                    200,
                    json!({
                        "queue":queue_info,
                        "partitions":v.get("partitions").cloned().unwrap_or_else(|| json!([])),
                        "totals":{"messages":totals}
                    })
                    .to_string(),
                ))
            }
            None => Ok(ApiOut::json(
                404,
                json!({"error":"Queue not found"}).to_string(),
            )),
        }
    }
    pub(super) async fn api_status_analytics(&self, ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let queues = self.queue_snapshots(&ctx.tenant).await?;
        let total: i64 = queues
            .iter()
            .filter_map(|x| x.pointer("/messages/total")?.as_i64())
            .sum();
        let now = super::super::wall_micros();
        Ok(ApiOut::json(
            200,
            json!({
                "dataPoints":[{"timestamp":crate::rsm::planner::timers::iso_us(now),"messages":total}],
                "interval":"hour",
                "from":crate::rsm::planner::timers::iso_us(now-86_400_000_000),
                "to":crate::rsm::planner::timers::iso_us(now)
            })
                .to_string(),
        ))
    }
    pub(super) async fn api_local_snapshot(
        &self,
        _ctx: &ReqCtx,
        key: &str,
    ) -> Result<ApiOut, RsmError> {
        let value = match key {
            // §14.6: no database, no spool (D19 refuses a push instead of
            // buffering it). `pending` is the log's appended-not-applied tail.
            "buffers" => {
                let m = self.repl.metrics();
                json!({"pending":m.inflight,"failed":0,"worker":0,"engine":"raft","spool":false})
            }
            _ => json!({key:[]}),
        };
        Ok(ApiOut::json(200, value.to_string()))
    }
    /// `GET /api/v1/analytics/dlq-signatures?queue=` — `get_dlq_signatures_v1`
    /// (010): why rows sit in one queue's DLQ, folded into error signatures
    /// over the newest `limit` rows (default 200, 1..=1000). 400 without a
    /// queue, as the Postgres handler answers.
    pub(super) async fn api_dlq_signatures(
        &self,
        ctx: ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let q = query_map(query);
        let Some(queue) = q.get("queue").filter(|v| !v.is_empty()).cloned() else {
            return Ok(ApiOut::json(400, json!({"error":"queue required"}).to_string()));
        };
        let limit = q
            .get("limit")
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(200)
            .clamp(1, 1000) as usize;
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let q2 = queue.clone();
        let mut rows = tokio::task::spawn_blocking(move || {
            store.read(|r| scan_dlq_rows(r, &tenant, Some(&q2), None))
        })
        .await
        .map_err(|e| RsmError::Internal(format!("dlq signatures: {e}")))?
        .map_err(read_error)?;
        let rows_now = rows.len();
        rows.sort_by(|a, b| b.3.failed_at_us.cmp(&a.3.failed_at_us));
        rows.truncate(limit);
        let n = rows.len();
        let mut sigs: BTreeMap<String, i64> = BTreeMap::new();
        let mut days: BTreeMap<String, i64> = BTreeMap::new();
        let mut retries = std::collections::BTreeSet::new();
        let mut groups = std::collections::BTreeSet::new();
        let mut bytes = 0i64;
        for (_, _, _, d) in &rows {
            *sigs.entry(error_signature(&d.error)).or_default() += 1;
            let day = crate::rsm::planner::timers::iso_us(d.failed_at_us)[..10].to_string();
            *days.entry(day).or_default() += 1;
            retries.insert(d.retry_count);
            groups.insert(d.group.clone());
            bytes += d.payload.len() as i64;
        }
        let mut top: Vec<(String, i64)> = sigs.into_iter().collect();
        top.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        top.truncate(8);
        let at = |us: Option<i64>| us.map(super::reads::iso_ms);
        Ok(ApiOut::json(
            200,
            json!({
                "queue":queue,
                "rowsNow":rows_now,
                "sample":n,
                "retryCounts":retries.into_iter().collect::<Vec<_>>(),
                "groups":groups.into_iter().collect::<Vec<_>>(),
                "oldest":at(rows.iter().map(|r| r.3.failed_at_us).min()),
                "newest":at(rows.iter().map(|r| r.3.failed_at_us).max()),
                "avgBytes":if n > 0 { ((bytes as f64) / n as f64).round() as i64 } else { 0 },
                "signatures":top.iter().map(|(text, k)| json!({
                    "text":text,"n":k,
                    "share":((*k as f64 / n as f64) * 1000.0).round() / 1000.0
                })).collect::<Vec<_>>(),
                "byDay":days.into_iter().map(|(day, k)| json!({"day":day,"n":k})).collect::<Vec<_>>()
            })
            .to_string(),
        ))
    }

    /// `GET /api/v1/analytics/partition-liveness` — `get_partition_liveness_v1`
    /// (011): per queue, how many partitions were written in the last hour,
    /// day and week, created in the last day, and the write-time span; the
    /// `limit` (default 20, 1..=200) queues with the most partitions. An empty
    /// `namespace=` / `task=` filters on the empty label, as in Postgres.
    pub(super) async fn api_partition_liveness(
        &self,
        ctx: ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let q = query_map(query);
        let queue = q.get("queue").filter(|v| !v.is_empty()).cloned();
        let namespace = q.get("namespace").cloned();
        let task = q.get("task").cloned();
        let limit = q
            .get("limit")
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(20)
            .clamp(1, 200) as usize;
        let pending: std::collections::HashMap<String, i64> = self
            .queue_snapshots(&ctx.tenant)
            .await?
            .iter()
            .filter_map(|v| {
                let name = v.get("name")?.as_str()?.to_string();
                let p = v.pointer("/messages/pending")?.as_i64()?
                    + v.pointer("/messages/processing").and_then(Value::as_i64).unwrap_or(0);
                Some((name, p))
            })
            .collect();
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let now = super::super::wall_micros();
        const H: i64 = 3_600_000_000;
        let mut rows = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut qs = Vec::new();
                r.scan_queues(&tenant, usize::MAX, &mut |name, cfg| {
                    qs.push((name.to_string(), cfg));
                    true
                })?;
                let mut out = Vec::new();
                for (name, cfg) in qs {
                    let ns = cfg.namespace.clone().unwrap_or_default();
                    let tk = cfg.task.clone().unwrap_or_default();
                    if queue.as_ref().is_some_and(|x| x != &name)
                        || namespace.as_ref().is_some_and(|x| x != &ns)
                        || task.as_ref().is_some_and(|x| x != &tk)
                    {
                        continue;
                    }
                    let mut pids = Vec::new();
                    r.scan_queue_partitions(&tenant, &name, None, usize::MAX, &mut |pid| {
                        pids.push(pid);
                        true
                    })?;
                    let (mut parts, mut l1, mut l24, mut l7, mut c24) = (0i64, 0i64, 0i64, 0i64, 0i64);
                    let (mut oldest, mut newest): (Option<i64>, Option<i64>) = (None, None);
                    for pid in pids {
                        let Some(p) = r.partition(pid)? else { continue };
                        parts += 1;
                        let w = p.last_write_at_us;
                        l1 += i64::from(w > now - H);
                        l24 += i64::from(w > now - 24 * H);
                        l7 += i64::from(w > now - 168 * H);
                        c24 += i64::from(p.created_at_us > now - 24 * H);
                        oldest = Some(oldest.map_or(w, |o| o.min(w)));
                        newest = Some(newest.map_or(w, |o| o.max(w)));
                    }
                    out.push((name, ns, tk, parts, l1, l24, l7, c24, oldest, newest));
                }
                Ok(out)
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("partition liveness: {e}")))?
        .map_err(read_error)?;
        rows.sort_by(|a, b| b.3.cmp(&a.3).then_with(|| a.0.cmp(&b.0)));
        rows.truncate(limit);
        let rows: Vec<Value> = rows
            .into_iter()
            .map(|(name, ns, tk, parts, l1, l24, l7, c24, oldest, newest)| {
                json!({
                    "queue":name,"namespace":ns,"task":tk,"partitions":parts,
                    "live1h":l1,"live24h":l24,"live7d":l7,"created24h":c24,
                    "oldestWriteAt":oldest.map(super::reads::iso_ms),
                    "newestWriteAt":newest.map(super::reads::iso_ms),
                    "pending":pending.get(&name).copied().unwrap_or(0)
                })
            })
            .collect();
        Ok(ApiOut::json(
            200,
            json!({"capturedAt":super::reads::iso_ms(now),"rows":rows}).to_string(),
        ))
    }
}

/// `get_dlq_signatures_v1`'s fold of an error text (010): empty → `(no
/// message)`; then, in order, UUIDs and whole words of 20+ hex digits →
/// `<id>`, `YYYY-MM-DD[T…[Z]]` → `<date>`, whole words of digits → `<n>`,
/// whitespace runs → one space; trimmed, first 120 characters.
pub(super) fn error_signature(error: &str) -> String {
    let t = error.trim();
    let s = if t.is_empty() { "(no message)" } else { t };
    let s = replace_uuids(s);
    let s = replace_words(&s, |w| w.len() >= 20 && w.chars().all(|c| c.is_ascii_hexdigit()), "<id>");
    let s = replace_dates(&s);
    let s = replace_words(&s, |w| w.chars().all(|c| c.is_ascii_digit()), "<n>");
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    s.chars().take(120).collect()
}

fn replace_uuids(s: &str) -> String {
    let b = s.as_bytes();
    let is_uuid = |i: usize| -> bool {
        if i + 36 > b.len() {
            return false;
        }
        (0..36).all(|k| match k {
            8 | 13 | 18 | 23 => b[i + k] == b'-',
            _ => b[i + k].is_ascii_hexdigit(),
        })
    };
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < b.len() {
        if is_uuid(i) {
            out.push_str("<id>");
            i += 36;
        } else {
            let ch = s[i..].chars().next().unwrap_or(' ');
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

/// Replace every whole word (a run of `[A-Za-z0-9_]`) matching `hit`.
fn replace_words(s: &str, hit: impl Fn(&str) -> bool, with: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut word = String::new();
    let flush = |word: &mut String, out: &mut String| {
        if !word.is_empty() {
            if hit(word) {
                out.push_str(with);
            } else {
                out.push_str(word);
            }
            word.clear();
        }
    };
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            word.push(ch);
        } else {
            flush(&mut word, &mut out);
            out.push(ch);
        }
    }
    flush(&mut word, &mut out);
    out
}

fn replace_dates(s: &str) -> String {
    let b = s.as_bytes();
    let d = |i: usize| i < b.len() && b[i].is_ascii_digit();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < b.len() {
        let date = i + 10 <= b.len()
            && (0..4).all(|k| d(i + k))
            && b[i + 4] == b'-'
            && d(i + 5)
            && d(i + 6)
            && b[i + 7] == b'-'
            && d(i + 8)
            && d(i + 9);
        if date {
            let mut j = i + 10;
            if j < b.len() && b[j] == b'T' && j + 1 < b.len() && (d(j + 1) || b[j + 1] == b':' || b[j + 1] == b'.') {
                j += 1;
                while j < b.len() && (d(j) || b[j] == b':' || b[j] == b'.') {
                    j += 1;
                }
                if j < b.len() && b[j] == b'Z' {
                    j += 1;
                }
            }
            out.push_str("<date>");
            i = j;
        } else {
            let ch = s[i..].chars().next().unwrap_or(' ');
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

#[cfg(test)]
mod signature_tests {
    use super::error_signature;

    #[test]
    fn folds_like_the_stored_procedure() {
        assert_eq!(error_signature("  "), "(no message)");
        assert_eq!(
            error_signature("order 01a0d2c7-e81f-702c-8734-c88401672347 failed at 2026-09-24T09:57:29.004Z after 3 tries"),
            "order <id> failed at <date> after <n> tries"
        );
        assert_eq!(
            error_signature("hash deadbeefdeadbeefdeadbeef\n\tretry   7"),
            "hash <id> retry <n>"
        );
        assert_eq!(error_signature("v2 abc123 x1"), "v2 abc123 x1");
        assert_eq!(error_signature(&"x".repeat(200)).len(), 120);
    }
}

fn clear_cursor_lease(c: &mut CursorRow) {
    c.batch_end = None;
    c.worker = None;
    c.lease_expires_at_us = None;
    c.lease_acquired_at_us = None;
    c.batch_retry_count = 0;
    c.attempt_offset = None;
    c.attempt_count = 0;
    c.lease_conflated = false;
    c.delivered.clear();
}
fn message_delete_miss(partition: &str, txn: &str) -> ApiOut {
    ApiOut::json(404,json!({"success":false,"partitionId":partition,"transactionId":txn,"error":"Message not found","message":"No dead-letter row for this address. Live messages live in immutable segments and cannot be deleted"}).to_string())
}

/// How long a dashboard read waits for a peer's `/raft/v1/local` answer.
pub(super) const PEER_GATHER_TTL: std::time::Duration = std::time::Duration::from_millis(1500);

/// This node's block of the Raft view: identity, role and state, term, the
/// log's indexes, its queue-log files and bytes, the store's map usage,
/// version and uptime — and, on the leader, its view of every other member's
/// replication (`replication`). What `/raft/v1/local` answers a peer, and what
/// `/api/v1/raft/status` answers a client.
pub(crate) fn local_node_json(
    repl: &crate::rsm::replicator::node::NodeReplicator<crate::rsm::store::heed_store::HeedStore>,
    store: &crate::rsm::store::heed_store::HeedStore,
) -> Value {
    use crate::rsm::replicator::{Replicator, Role};
    let m = repl.metrics();
    let view = repl.cluster_view();
    let map = store.map_usage();
    let node_id = view.as_ref().map(|v| v.node_id).unwrap_or(1);
    let me = view
        .as_ref()
        .and_then(|v| v.members.iter().find(|x| x.node_id == node_id));
    let state = match repl.role() {
        Role::Leader { .. } => "leader",
        Role::Follower { .. } => "follower",
        Role::Candidate => "candidate",
        Role::Learner => "learner",
        Role::Stopped => "shutdown",
    };
    let replication: Value = match &view {
        Some(v) if state == "leader" => Value::Array(
            v.members
                .iter()
                .filter(|x| x.node_id != node_id)
                .map(|x| json!({"nodeId":x.node_id,"matchIndex":x.matched,"heartbeatAgeMs":x.heartbeat_age_ms}))
                .collect(),
        ),
        _ => Value::Null,
    };
    json!({
        "nodeId":node_id,
        "hostname":crate::rsm::dashboard::node_label(node_id),
        "raftAddr":me.map(|x| x.raft_addr.clone()),
        "httpAddr":me.map(|x| x.http_addr.clone()),
        "role":if me.is_none_or(|x| x.voter) { "voter" } else { "learner" },
        "state":state,
        "local":true,
        "reachable":true,
        "leaderId":view.as_ref().and_then(|v| v.leader).or(m.leader),
        "term":m.term,
        "lastLogIndex":m.last_log_index,
        "committedIndex":m.committed_index,
        "appliedIndex":m.applied_index,
        "durableIndex":m.durable_index,
        "inflight":m.inflight,
        "proposals":m.proposals,
        "log":{"files":m.log_files,"bytes":m.log_bytes},
        "store":{"mapBytes":map.map_bytes,"usedBytes":map.used_bytes,"mapUsedPct":map.pct(),"readersInUse":map.readers_in_use,"maxReaders":map.max_readers},
        "version":crate::VERSION,
        "uptimeSeconds":crate::rsm::dashboard::started().elapsed().as_secs(),
        "replication":replication,
        "error":Value::Null
    })
}

/// The cluster's dashboard id and voter count, from the membership (or, on a
/// replicator without one, from this node alone).
fn cluster_identity(
    view: Option<&crate::rsm::replicator::raft::ClusterView>,
    local: &Value,
) -> (String, usize) {
    match view {
        Some(v) if !v.members.is_empty() => {
            let members: Vec<(u64, String)> = v
                .members
                .iter()
                .filter(|m| m.voter)
                .map(|m| (m.node_id, m.raft_addr.clone()))
                .collect();
            let voters = members.len();
            (crate::rsm::dashboard::cluster_id(&members), voters)
        }
        _ => {
            let id = local.get("nodeId").and_then(Value::as_u64).unwrap_or(1);
            (crate::rsm::dashboard::cluster_id(&[(id, String::new())]), 1)
        }
    }
}

/// The body of `GET /api/v1/raft/liveness` for what one node knows. Pure, so
/// the shape is testable without a cluster.
pub(crate) fn raft_liveness_json(cm: &crate::rsm::replicator::ClusterMembers) -> serde_json::Value {
    let age_ms = cm.view_age.map(|a| a.as_millis() as u64);
    let members: Vec<serde_json::Value> = cm
        .view
        .as_ref()
        .map(|v| {
            v.members
                .iter()
                .map(|m| {
                    let state = if !m.voter {
                        "learner"
                    } else if m.id == v.leader {
                        "leader"
                    } else {
                        "follower"
                    };
                    json!({
                        "nodeId": m.id,
                        "role": if m.voter { "voter" } else { "learner" },
                        "state": state,
                        "http": m.http,
                        "raft": m.raft,
                        "lastAckMs": m.last_ack_ms.map(|ms| ms + age_ms.unwrap_or(0)),
                        "matchIndex": m.matched,
                        "local": m.id == cm.node_id,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    json!({
        "engine": "raft",
        "nodeId": cm.node_id,
        "leaderId": cm.leader,
        "term": cm.term,
        "viewAgeMs": age_ms,
        "viewLeaderId": cm.view.as_ref().map(|v| v.leader),
        "members": members,
    })
}
