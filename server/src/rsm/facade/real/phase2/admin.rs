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
            return Ok(ApiOut::json(404,json!({"success":false,"error":if partition.is_some(){"Partition not found"}else{"Queue not found or has no partitions"}}).to_string()));
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

    pub(super) async fn api_status(&self, ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let queues = self.queue_snapshots(&ctx.tenant).await?;
        let total: i64 = queues
            .iter()
            .filter_map(|x| x.pointer("/messages/total")?.as_i64())
            .sum();
        let pending: i64 = queues
            .iter()
            .filter_map(|x| x.pointer("/messages/pending")?.as_i64())
            .sum();
        let completed: i64 = queues
            .iter()
            .filter_map(|x| x.pointer("/messages/completed")?.as_i64())
            .sum();
        let now = super::super::wall_micros();
        Ok(ApiOut::json(200,json!({
            "timeRange":{"from":crate::rsm::planner::timers::iso_us(now-3_600_000_000),"to":crate::rsm::planner::timers::iso_us(now)},
            "bucketMinutes":1,"pointCount":0,"throughput":[],"queues":queues,
            "messages":{"total":total,"pending":pending,"processing":0,"completed":completed,"failed":0,"deadLetter":0,"requests":{"push":0,"pop":0,"ack":0}},
            "leases":{"active":0,"partitionsWithLeases":0,"totalBatchSize":0,"totalAcked":0},
            "deadLetterQueue":{"totalMessages":0,"currentMessages":0,"affectedPartitions":0,"topErrors":[]},
            "workers":[],"errors":{"dbErrors":0,"ackFailed":0,"dlqMessages":0},"statsAge":0,
            "engine":"raft"
        }).to_string()))
    }

    pub(super) async fn api_raft_status(&self, _ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let repl = self.repl.metrics();
        let map = self.store.map_usage();
        Ok(ApiOut::json(
            200,
            json!({
                "engine":"raft",
                "nodeId":1,
                "role":if repl.is_leader { "leader" } else { "follower" },
                "leaderId":if repl.is_leader { Some(1u64) } else { None },
                "term":repl.term,
                "lastLogIndex":repl.last_log_index,
                "committedIndex":repl.committed_index,
                "appliedIndex":repl.applied_index,
                "durableIndex":repl.durable_index,
                "inflight":repl.inflight,
                "proposals":repl.proposals,
                "log":{"files":repl.log_files,"bytes":repl.log_bytes},
                "store":{"mapBytes":map.map_bytes,"usedBytes":map.used_bytes,"mapUsedPct":map.pct(),"readersInUse":map.readers_in_use,"maxReaders":map.max_readers},
                "singleNode":true
            })
            .to_string(),
        ))
    }

    pub(super) async fn api_raft_members(&self, _ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let repl = self.repl.metrics();
        Ok(ApiOut::json(
            200,
            json!({
                "leaderId":if repl.is_leader { Some(1u64) } else { None },
                "members":[{
                    "nodeId":1,
                    "role":"voter",
                    "state":if repl.is_leader { "leader" } else { "follower" },
                    "term":repl.term,
                    "matchIndex":repl.committed_index,
                    "appliedIndex":repl.applied_index,
                    "local":true
                }]
            })
            .to_string(),
        ))
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
        ctx: &ReqCtx,
        key: &str,
    ) -> Result<ApiOut, RsmError> {
        let value = match key {
            "metrics" => self.local_system_metrics(),
            "workers" => self.local_worker_metrics(),
            "buffers" => {
                let m = self.repl.metrics();
                json!({"pending":m.inflight,"failed":0,"dbHealthy":true,"worker":0,"engine":"raft"})
            }
            "queues" => json!({
                "timeRange":{},
                "bucketMinutes":1,
                "series":self.queue_snapshots(&ctx.tenant).await?,
                "replicas":[{"hostname":"local","workerId":0}]
            }),
            "retention" => self.local_metrics.retention_json(&ctx.tenant),
            _ => json!({key:[]}),
        };
        Ok(ApiOut::json(200, value.to_string()))
    }

    fn local_system_metrics(&self) -> Value {
        let repl = self.repl.metrics();
        let map = self.store.map_usage();
        let counters: serde_json::Map<String, Value> = self
            .store
            .metrics()
            .snapshot()
            .into_iter()
            .map(|(k, v)| (k.to_string(), json!(v)))
            .collect();
        let now = super::super::wall_micros();
        json!({
            "engine":"raft",
            "timeRange":{"from":crate::rsm::planner::timers::iso_us(now-3_600_000_000),"to":crate::rsm::planner::timers::iso_us(now)},
            "replicaCount":1,
            "bucketMinutes":1,
            "pointCount":1,
            "replicas":[{
                "hostname":"local","port":0,"workerId":"0",
                "timeSeries":[{
                    "raft":{"term":repl.term,"leader":repl.is_leader,"lastLogIndex":repl.last_log_index,"committedIndex":repl.committed_index,"appliedIndex":repl.applied_index,"durableIndex":repl.durable_index,"inflight":repl.inflight,"proposals":repl.proposals,"logFiles":repl.log_files,"logBytes":repl.log_bytes},
                    "store":{"mapBytes":map.map_bytes,"usedBytes":map.used_bytes,"mapUsedPct":map.pct(),"readersInUse":map.readers_in_use,"maxReaders":map.max_readers,"counters":counters}
                }]
            }]
        })
    }

    fn local_worker_metrics(&self) -> Value {
        use std::sync::atomic::Ordering;
        let m = crate::rsm::timing::metrics();
        let now = super::super::wall_micros();
        json!({
            "timeRange":{"from":crate::rsm::planner::timers::iso_us(now-3_600_000_000),"to":crate::rsm::planner::timers::iso_us(now)},
            "bucketMinutes":1,
            "pointCount":1,
            "timeSeries":[{
                "pushMessages":m.apply_stats.messages.load(Ordering::Relaxed),
                "jobsDone":m.apply_stats.entries.load(Ordering::Relaxed),
                "dlqCount":0,
                "dbErrors":self.store.metrics().commit_failed.load(Ordering::Relaxed),
                "slowCommands":m.slow_commands.load(Ordering::Relaxed),
                "applyReceives":m.apply_receives.load(Ordering::Relaxed)
            }],
            "workers":[{"hostname":"local","workerId":0}],
            "queues":[],
            "summary":{"engine":"raft"}
        })
    }
    pub(super) async fn api_queue_lag(&self, ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let q = self.queue_snapshots(&ctx.tenant).await?;
        Ok(ApiOut::json(200, json!({"queues":q}).to_string()))
    }
    pub(super) async fn api_queue_ops(&self, ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let q = self.queue_snapshots(&ctx.tenant).await?;
        Ok(ApiOut::json(200, json!({"queues":q}).to_string()))
    }
    pub(super) async fn api_workload(&self, ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let q = self.queue_snapshots(&ctx.tenant).await?;
        Ok(ApiOut::json(200, json!({"workload":q}).to_string()))
    }
    pub(super) async fn api_dlq_signatures(&self, ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let rows=tokio::task::spawn_blocking(move||store.read(|r|{let mut m:BTreeMap<(String,String),i64>=BTreeMap::new();for(_,q,_,d)in scan_dlq_rows(r,&tenant,None,None)?{*m.entry((q,d.error)).or_default()+=1;}Ok(m.into_iter().map(|((queue,error),count)|json!({"queue":queue,"error":error,"count":count})).collect::<Vec<_>>())})).await.map_err(|e|RsmError::Internal(format!("dlq signatures: {e}")))?.map_err(read_error)?;
        Ok(ApiOut::json(200, json!({"signatures":rows}).to_string()))
    }
    pub(super) async fn api_partition_liveness(&self, ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let parts = self.parts(&ctx.tenant, None, None).await?;
        let rows:Vec<Value>=parts.into_iter().map(|p|json!({"queue":p.row.queue,"partition":p.row.partition,"partitionId":uuid_bytes_to_string(&p.row.uuid),"lastOffset":p.row.last_offset,"logStart":p.row.log_start,"lastWriteAt":crate::rsm::planner::timers::iso_us(p.row.last_write_at_us)})).collect();
        Ok(ApiOut::json(200, json!({"partitions":rows}).to_string()))
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
