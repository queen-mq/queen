//! Streams cycle: source acknowledgement, sink appends, and state cells share
//! one `Transaction` command and therefore one committed entry.

use serde_json::{json, Value};

use super::{ApiOut, Effect, RaftFacade, Reply, ReqCtx, RsmError};
use crate::frames::{pack_frames, uuid_bytes_to_string, uuid_string_to_bytes, FrameIn};
use crate::rsm::batcher::Command;
use crate::rsm::entry::PushVerdict;
use crate::rsm::planner::txn::TxnCommand;
use crate::rsm::planner::{AckItem, AckStatus, AckTarget, PushCommand, PushItem};
use crate::rsm::store::{Reads, Store, TypedReads};
use crate::util::{txn_hash128, uuidv7_bytes};

impl RaftFacade {
    pub(super) async fn api_streams_cycle(
        &self,
        ctx: ReqCtx,
        body: &[u8],
    ) -> Result<ApiOut, RsmError> {
        let root: Value =
            serde_json::from_slice(body).map_err(|e| super::rejected("bad_body", e))?;
        let qid = root
            .get("query_id")
            .and_then(Value::as_str)
            .and_then(uuid_string_to_bytes)
            .ok_or_else(|| super::reject("bad_request", "query_id is required"))?;
        let partition_id = root
            .get("partition_id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .ok_or_else(|| super::reject("bad_request", "partition_id is required"))?;
        let group = root
            .get("consumer_group")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or("__QUEUE_MODE__")
            .to_string();
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let partition_lookup = partition_id.clone();
        let resolved = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                if r.streams_query(&tenant, &qid)?.is_none() {
                    return Ok(None);
                }
                if let Ok(pid) = partition_lookup.parse::<u64>() {
                    return Ok(r
                        .partition(pid)?
                        .filter(|p| p.tenant == tenant)
                        .map(|p| (pid, p)));
                }
                let Some(puid) = uuid_string_to_bytes(&partition_lookup) else {
                    return Ok(None);
                };
                let mut found = None;
                r.scan_raw(
                    crate::rsm::store::Keyspace::Partitions,
                    &[],
                    &[],
                    usize::MAX,
                    &mut |k, v| {
                        if let (Some(pid), Ok(p)) = (
                            crate::rsm::store::keys::pid_of(k),
                            crate::rsm::store::rows::partition_decode(v),
                        ) {
                            if p.tenant == tenant && p.uuid == puid {
                                found = Some((pid, p));
                                return false;
                            }
                        }
                        true
                    },
                )?;
                Ok(found)
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("streams cycle lookup: {e}")))?
        .map_err(super::read_error)?;
        let Some((pid, source)) = resolved else {
            return Ok(ApiOut::json(
                404,
                json!({"success":false,"error":"query or partition not found"}).to_string(),
            ));
        };

        let mut pushes = Vec::new();
        let mut push_meta = Vec::new();
        for (i, item) in root
            .get("push_items")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let queue = item.get("queue").and_then(Value::as_str).unwrap_or("");
            if queue.is_empty() {
                continue;
            }
            let partition = item
                .get("partition")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or("Default");
            super::super::super::check_message_key_names(
                &ctx.tenant,
                queue,
                None,
                Some(partition),
            )?;
            let mid = item
                .get("messageId")
                .and_then(Value::as_str)
                .and_then(uuid_string_to_bytes)
                .unwrap_or_else(uuidv7_bytes);
            let mid_s = uuid_bytes_to_string(&mid);
            let txn = item
                .get("transactionId")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| mid_s.clone());
            let payload = item
                .get("payload")
                .or_else(|| item.get("data"))
                .cloned()
                .unwrap_or(json!({}));
            let bytes = serde_json::to_vec(&payload).unwrap_or_default();
            let frame = pack_frames(&[FrameIn {
                message_id: mid,
                txn: &txn,
                trace_id: None,
                producer_sub: None,
                payload: &bytes,
                encrypted: false,
            }]);
            pushes.push(PushCommand {
                request_id: super::super::derived_request_id(ctx.request_id, i as u32),
                tenant: ctx.tenant.clone(),
                queue: queue.to_string(),
                partition: partition.to_string(),
                items: vec![PushItem {
                    hash: txn_hash128(&txn),
                    frame,
                }],
                create_cfg: super::super::default_queue_config(),
            });
            push_meta.push((queue.to_string(), mid_s, txn));
        }

        let mut acks = Vec::new();
        if let Some(a) = root.get("ack").filter(|a| !a.is_null()) {
            let txn = a.get("transactionId").and_then(Value::as_str).unwrap_or("");
            if txn.is_empty() {
                return Ok(ApiOut::json(
                    400,
                    json!({"success":false,"error":"ack.transactionId is required"}).to_string(),
                ));
            }
            let status = match a
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("completed")
                .to_ascii_lowercase()
                .as_str()
            {
                "completed" | "success" | "acked" | "ok" | "" => AckStatus::Ok,
                "failed" => AckStatus::Failed,
                "dlq" | "dead_letter" => AckStatus::Dlq,
                _ => AckStatus::Retry,
            };
            acks.push(AckTarget {
                pid,
                tenant: ctx.tenant.clone(),
                queue: source.queue.clone(),
                group: group.clone(),
                worker: a
                    .get("leaseId")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                items: vec![AckItem {
                    hash: txn_hash128(txn),
                    status,
                    error: a.get("error").and_then(Value::as_str).map(str::to_string),
                    snapshot: None,
                }],
            });
        }

        let now = super::super::wall_micros();
        let mut extra = Vec::new();
        let mut state_count = 0usize;
        for op in root
            .get("state_ops")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(key) = op.get("key").and_then(Value::as_str) else {
                continue;
            };
            match op.get("type").and_then(Value::as_str).unwrap_or("upsert") {
                "delete" => extra.push(Effect::StreamsStateDelete {
                    query_id: qid,
                    pid,
                    key: key.to_string(),
                }),
                _ => {
                    let value =
                        serde_json::to_vec(op.get("value").unwrap_or(&Value::Null)).unwrap();
                    extra.push(Effect::StreamsStatePut {
                        query_id: qid,
                        pid,
                        key: key.to_string(),
                        value,
                        updated_at_us: now,
                    });
                }
            }
            state_count += 1;
        }
        let cmd = Command::Transaction(TxnCommand {
            request_id: ctx.request_id,
            tenant: ctx.tenant.clone(),
            pushes,
            acks,
            kv: Vec::new(),
            timers: Vec::new(),
            extra_effects: extra,
            allow_duplicate: false,
        });
        let out = match self.submit(&ctx, cmd).await? {
            Reply::Done { outcome, .. } => crate::rsm::batcher::TxnOutcome::from_outcome(&outcome)
                .ok_or_else(|| {
                    RsmError::Internal("streams cycle got non-transaction outcome".into())
                })?,
            Reply::Refused(r) => {
                return Ok(ApiOut::json(
                    200,
                    json!({"success":false,"error":r.message,"reason":r.code}).to_string(),
                ))
            }
            Reply::Retry { hint } => {
                return Err(RsmError::Retry {
                    leader_hint: hint.map(|h| h.to_string()),
                })
            }
        };
        let push_results: Vec<Value> = push_meta
            .into_iter()
            .zip(out.pushes.iter())
            .map(|((queue, mid, txn), o)| {
                let status = o
                    .items
                    .first()
                    .map(|v| match v {
                        PushVerdict::Created { .. } => "created",
                        PushVerdict::Duplicate { .. } => "duplicate",
                        PushVerdict::Refused { .. } => "refused",
                    })
                    .unwrap_or("created");
                json!({"queueName":queue,"messageId":mid,"transactionId":txn,"status":status})
            })
            .collect();
        let ack_result=out.acks.results.first().map(|a|json!({"success":a.stale_hashes.is_empty(),"count":a.acked,"lease_released":a.lease_released,"dlq":a.dlq>0}));
        Ok(ApiOut::json(200,json!({"success":true,"query_id":uuid_bytes_to_string(&qid),"partition_id":partition_id,"queueName":source.queue,"state_ops_applied":state_count,"push_results":push_results,"ack_result":ack_result}).to_string()))
    }
}
