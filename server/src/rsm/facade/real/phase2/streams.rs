//! Streams cycle: source acknowledgement, sink appends, and state cells share
//! one `Transaction` command and therefore one committed entry.

use std::borrow::Cow;

use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;
use serde_json::{json, Value};

use super::{ApiOut, Effect, RaftFacade, Reply, ReqCtx, RsmError};
use crate::frames::{pack_frames, uuid_bytes_to_string, uuid_string_to_bytes, FrameIn};
use crate::rsm::batcher::Command;
use crate::rsm::entry::PushVerdict;
use crate::rsm::planner::txn::TxnCommand;
use crate::rsm::planner::{AckPositionalCommand, PushCommand, PushItem};
use crate::rsm::store::{Reads, Store, TypedReads};
use crate::util::{txn_hash128, uuidv7_bytes};

/// A cycle body, read once: its small fields as values (`None` when absent or
/// `null`), and each push payload and state value as the JSON text it is
/// stored as.
struct CycleIn<'a> {
    query_id: Option<Value>,
    partition_id: Option<Value>,
    consumer_group: Option<Value>,
    release_lease: Option<Value>,
    ack: Option<Value>,
    pushes: Vec<PushIn<'a>>,
    ops: Vec<OpIn<'a>>,
}

/// One object of `push_items`.
struct PushIn<'a> {
    /// Its place in `push_items`: its request id is derived from it.
    index: usize,
    queue: Option<Value>,
    partition: Option<Value>,
    message_id: Option<Value>,
    transaction_id: Option<Value>,
    /// `payload`, else `data`, else `{}`.
    payload: Cow<'a, [u8]>,
}

/// One object of `state_ops`.
struct OpIn<'a> {
    /// Its place in `state_ops`, named when it is refused.
    index: usize,
    key: Option<Value>,
    ty: Option<Value>,
    /// `value`, else `null`.
    value: Cow<'a, [u8]>,
}

/// A field that is present, `null` included, as its text; an absent one is
/// `None` (with `#[serde(default)]`). serde reads a `null` `Option` as absent,
/// and the tree read did not: `"payload": null` stores `null`.
fn present<'de, D: Deserializer<'de>>(d: D) -> Result<Option<&'de RawValue>, D::Error> {
    <&RawValue>::deserialize(d).map(Some)
}

/// The elements of an array field, as text: none for a field that is absent,
/// `null` or not an array, as the tree read's `as_array`.
fn elements(field: Option<&RawValue>) -> Option<Vec<&RawValue>> {
    match field {
        Some(raw) if raw.get().starts_with('[') => serde_json::from_str(raw.get()).ok(),
        _ => Some(Vec::new()),
    }
}

/// The body read without a tree. It is checked first with the rules
/// `serde_json::Value` parses by (`json_out::Check`), so what is stored is a
/// document the tree read takes too; then each payload and state value is kept
/// as its own text, borrowed from the body, where the tree read held about ten
/// times its size. `None` for a body the tree read may answer otherwise: not a
/// JSON object, a repeated key, a field of an unexpected kind. The caller then
/// reads it as before.
fn lean_cycle(body: &[u8]) -> Option<CycleIn<'_>> {
    #[derive(Deserialize)]
    struct Body<'a> {
        #[serde(default)]
        query_id: Option<Value>,
        #[serde(default)]
        partition_id: Option<Value>,
        #[serde(default)]
        consumer_group: Option<Value>,
        #[serde(default)]
        release_lease: Option<Value>,
        #[serde(default)]
        ack: Option<Value>,
        #[serde(borrow, default)]
        push_items: Option<&'a RawValue>,
        #[serde(borrow, default)]
        state_ops: Option<&'a RawValue>,
    }
    #[derive(Deserialize)]
    struct Item<'a> {
        #[serde(default)]
        queue: Option<Value>,
        #[serde(default)]
        partition: Option<Value>,
        #[serde(default, rename = "messageId")]
        message_id: Option<Value>,
        #[serde(default, rename = "transactionId")]
        transaction_id: Option<Value>,
        #[serde(borrow, default, deserialize_with = "present")]
        payload: Option<&'a RawValue>,
        #[serde(borrow, default, deserialize_with = "present")]
        data: Option<&'a RawValue>,
    }
    #[derive(Deserialize)]
    struct Op<'a> {
        #[serde(default)]
        key: Option<Value>,
        #[serde(default, rename = "type")]
        ty: Option<Value>,
        #[serde(borrow, default, deserialize_with = "present")]
        value: Option<&'a RawValue>,
    }
    let text = super::json_out::checked(body)?;
    // A struct also reads from an array, by position; only an object is read
    // here.
    if !text.starts_with('{') {
        return None;
    }
    let b: Body = serde_json::from_str(text).ok()?;
    let mut pushes = Vec::new();
    for (index, item) in elements(b.push_items)?.into_iter().enumerate() {
        // The tree read finds no queue in anything but an object.
        if !item.get().starts_with('{') {
            continue;
        }
        let it: Item = serde_json::from_str(item.get()).ok()?;
        let payload = it
            .payload
            .or(it.data)
            .map_or(&b"{}"[..], |p| p.get().as_bytes());
        pushes.push(PushIn {
            index,
            queue: it.queue,
            partition: it.partition,
            message_id: it.message_id,
            transaction_id: it.transaction_id,
            payload: Cow::Borrowed(payload),
        });
    }
    let mut ops = Vec::new();
    for (index, op) in elements(b.state_ops)?.into_iter().enumerate() {
        // The tree read finds no key in anything but an object.
        if !op.get().starts_with('{') {
            continue;
        }
        let o: Op = serde_json::from_str(op.get()).ok()?;
        ops.push(OpIn {
            index,
            key: o.key,
            ty: o.ty,
            value: Cow::Borrowed(o.value.map_or(&b"null"[..], |v| v.get().as_bytes())),
        });
    }
    Some(CycleIn {
        query_id: b.query_id,
        partition_id: b.partition_id,
        consumer_group: b.consumer_group,
        release_lease: b.release_lease,
        ack: b.ack,
        pushes,
        ops,
    })
}

/// A small field of the tree read: `None` for a field that is absent or
/// `null`, as the lean read's `Option<Value>` has it. Every use reads it with
/// `as_str`, `as_bool` or `get`, which find nothing in either.
fn field(v: &Value, key: &str) -> Option<Value> {
    v.get(key).filter(|x| !x.is_null()).cloned()
}

/// The body as the route always read it, through one tree: for the bodies
/// [`lean_cycle`] leaves to it. Payloads and state values are stored as the
/// tree prints them.
fn tree_cycle(root: &Value) -> CycleIn<'static> {
    let pushes = root
        .get("push_items")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .filter(|(_, item)| item.is_object())
        .map(|(index, item)| PushIn {
            index,
            queue: field(item, "queue"),
            partition: field(item, "partition"),
            message_id: field(item, "messageId"),
            transaction_id: field(item, "transactionId"),
            payload: Cow::Owned(match item.get("payload").or_else(|| item.get("data")) {
                Some(p) => serde_json::to_vec(p).unwrap_or_default(),
                None => b"{}".to_vec(),
            }),
        })
        .collect();
    let ops = root
        .get("state_ops")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .filter(|(_, op)| op.is_object())
        .map(|(index, op)| OpIn {
            index,
            key: field(op, "key"),
            ty: field(op, "type"),
            value: Cow::Owned(
                serde_json::to_vec(op.get("value").unwrap_or(&Value::Null))
                    .unwrap_or_else(|_| b"null".to_vec()),
            ),
        })
        .collect();
    CycleIn {
        query_id: field(root, "query_id"),
        partition_id: field(root, "partition_id"),
        consumer_group: field(root, "consumer_group"),
        release_lease: field(root, "release_lease"),
        ack: field(root, "ack"),
        pushes,
        ops,
    }
}

impl RaftFacade {
    pub(super) async fn api_streams_cycle(
        &self,
        ctx: ReqCtx,
        body: &[u8],
    ) -> Result<ApiOut, RsmError> {
        let cycle = match lean_cycle(body) {
            Some(cycle) => cycle,
            None => tree_cycle(
                &serde_json::from_slice::<Value>(body)
                    .map_err(|e| super::rejected("bad_body", e))?,
            ),
        };
        let qid = cycle
            .query_id
            .as_ref()
            .and_then(Value::as_str)
            .and_then(uuid_string_to_bytes)
            .ok_or_else(|| super::reject("bad_request", "query_id is required"))?;
        let partition_id = cycle
            .partition_id
            .as_ref()
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .ok_or_else(|| super::reject("bad_request", "partition_id is required"))?;
        let group = cycle
            .consumer_group
            .as_ref()
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
        for item in &cycle.pushes {
            let queue = item.queue.as_ref().and_then(Value::as_str).unwrap_or("");
            if queue.is_empty() {
                continue;
            }
            let partition = item
                .partition
                .as_ref()
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
                .message_id
                .as_ref()
                .and_then(Value::as_str)
                .and_then(uuid_string_to_bytes)
                .unwrap_or_else(uuidv7_bytes);
            let mid_s = uuid_bytes_to_string(&mid);
            let txn = item
                .transaction_id
                .as_ref()
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| mid_s.clone());
            let frame = pack_frames(&[FrameIn {
                message_id: mid,
                txn: &txn,
                trace_id: None,
                producer_sub: None,
                payload: &item.payload,
                encrypted: false,
            }]);
            pushes.push(PushCommand {
                request_id: super::super::derived_request_id(ctx.request_id, item.index as u32),
                tenant: ctx.tenant.clone(),
                queue: queue.to_string(),
                partition: partition.to_string(),
                items: vec![PushItem {
                    hash: txn_hash128(&txn),
                    frame: frame.into(),
                }],
                create_cfg: super::super::default_queue_config(queue),
            });
            push_meta.push((queue.to_string(), mid_s, txn));
        }

        let mut positional_acks = Vec::new();
        let mut ack_ok = true;
        if let Some(a) = cycle.ack.as_ref() {
            let ok = matches!(
                a.get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("completed")
                    .to_ascii_lowercase()
                    .as_str(),
                "completed" | "success" | "acked" | "ok" | ""
            );
            ack_ok = ok;
            positional_acks.push(AckPositionalCommand {
                request_id: super::super::derived_request_id(ctx.request_id, 0x7fff_fffe),
                pid,
                tenant: ctx.tenant.clone(),
                queue: source.queue.clone(),
                group: group.clone(),
                worker: a
                    .get("leaseId")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                upto: None,
                ok,
                release_lease: cycle
                    .release_lease
                    .as_ref()
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                acked_count: a
                    .get("count")
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
                    .clamp(0, i32::MAX as i64) as i32,
            });
        }

        let now = super::super::wall_micros();
        let max_state = crate::rsm::facade::streams_max_state_bytes();
        let mut extra = Vec::new();
        let mut state_count = 0usize;
        for op in cycle.ops {
            let Some(key) = op.key.as_ref().and_then(Value::as_str) else {
                continue;
            };
            match op.ty.as_ref().and_then(Value::as_str).unwrap_or("upsert") {
                "delete" => extra.push(Effect::StreamsStateDelete {
                    query_id: qid,
                    pid,
                    key: key.to_string(),
                }),
                _ => {
                    if op.value.len() > max_state {
                        return Ok(ApiOut::json(
                            413,
                            json!({
                                "success": false,
                                "error": format!(
                                    "state op at index {}: value is {} bytes, the ceiling is \
                                     {max_state} (QUEEN_STREAMS_MAX_STATE_BYTES)",
                                    op.index,
                                    op.value.len()
                                ),
                                "reason": "state_too_large",
                            })
                            .to_string(),
                        ));
                    }
                    extra.push(Effect::StreamsStatePut {
                        query_id: qid,
                        pid,
                        key: key.to_string(),
                        value: op.value.into_owned(),
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
            acks: Vec::new(),
            positional_acks,
            kv: Vec::new(),
            timers: Vec::new(),
            extra_effects: extra,
            allow_duplicate: false,
            positions: Vec::new(),
            engine_acks: Default::default(),
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
        // Dashboard counters: the sink messages the cycle stored and the source
        // messages it settled, as a push and an ack call would count them.
        if let Some(m) = crate::metrics::global() {
            for r in &push_results {
                if r["status"] == "created" {
                    if let Some(q) = r["queueName"].as_str() {
                        m.per_queue.add_pushed(&ctx.tenant, q, 1);
                    }
                }
            }
            if let Some(a) = out.acks.results.first() {
                let (ok, failed) = if ack_ok { (a.acked, 0) } else { (0, a.acked) };
                m.per_queue.add_settled(
                    &ctx.tenant,
                    &source.queue,
                    u64::from(ok),
                    u64::from(failed),
                );
            }
        }
        let ack_result=out.acks.results.first().map(|a|json!({"success":a.stale_hashes.is_empty(),"count":a.acked,"lease_released":a.lease_released,"dlq":a.dlq>0}));
        Ok(ApiOut::json(200,json!({"success":true,"query_id":uuid_bytes_to_string(&qid),"partition_id":partition_id,"queueName":source.queue,"state_ops_applied":state_count,"push_results":push_results,"ack_result":ack_result}).to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(body: &[u8]) -> CycleIn<'static> {
        tree_cycle(&serde_json::from_slice::<Value>(body).expect("JSON"))
    }

    fn doc(b: &[u8]) -> Value {
        serde_json::from_slice(b).expect("a stored document")
    }

    /// Both reads of `body` agree: every small field, each push item and
    /// state op with its place, each payload and value the same JSON.
    fn assert_same(body: &[u8]) {
        let lean = lean_cycle(body)
            .unwrap_or_else(|| panic!("left to the tree: {}", String::from_utf8_lossy(body)));
        let tree = tree(body);
        assert_eq!(lean.query_id, tree.query_id);
        assert_eq!(lean.partition_id, tree.partition_id);
        assert_eq!(lean.consumer_group, tree.consumer_group);
        assert_eq!(lean.release_lease, tree.release_lease);
        assert_eq!(lean.ack, tree.ack);
        assert_eq!(lean.pushes.len(), tree.pushes.len());
        for (l, t) in lean.pushes.iter().zip(&tree.pushes) {
            assert_eq!(
                (l.index, &l.queue, &l.partition, &l.message_id, &l.transaction_id),
                (t.index, &t.queue, &t.partition, &t.message_id, &t.transaction_id)
            );
            assert_eq!(doc(&l.payload), doc(&t.payload));
        }
        assert_eq!(lean.ops.len(), tree.ops.len());
        for (l, t) in lean.ops.iter().zip(&tree.ops) {
            assert_eq!((l.index, &l.key, &l.ty), (t.index, &t.key, &t.ty));
            assert_eq!(doc(&l.value), doc(&t.value));
        }
    }

    #[test]
    fn the_lean_read_is_the_tree_read_for_sdk_bodies() {
        // As the Rust SDK's runner commits a window (runner.rs `commit`).
        assert_same(
            br#"{"query_id":"0194e3a6-1111-7000-8000-000000000001","partition_id":"42",
                "consumer_group":"streams.traces",
                "state_ops":[{"type":"upsert","key":"sess:60\u001fopen\u001f42",
                              "value":{"acc":{"z":1,"a":[1,2]},"windowEnd":5}},
                             {"type":"delete","key":"old"}],
                "push_items":[{"queue":"traces","partition":"t1","payload":{"b":1,"a":2},
                               "transactionId":"x","messageId":"0194e3a6-2222-7000-8000-000000000002"}],
                "ack":{"transactionId":"m","leaseId":"l","status":"completed","count":3},
                "release_lease":true}"#,
        );
        // A flush: no ack, nothing to push.
        assert_same(br#"{"query_id":"q","partition_id":"1","state_ops":[],"push_items":[],"ack":null,"release_lease":null}"#);
        // Spaces after the separators, as Python's json.dumps writes them.
        assert_same(
            br#"{"query_id": "q", "partition_id": "1", "state_ops": [{"type": "upsert", "key": "k", "value": {"n": 1, "m": [1, 2]}}], "push_items": [{"queue": "s", "data": {"x": 1}}]}"#,
        );
    }

    #[test]
    fn missing_and_null_fields_read_as_the_tree_read_them() {
        let body = br#"{"query_id":"q","partition_id":"1",
            "push_items":[{"queue":"s","payload":null,"data":{"x":1}},{"queue":"s","data":{"x":1}},
                          {"queue":"s"},7,"str",{"queue":"s","payload":{"y":2}}],
            "state_ops":[{"key":"a","value":null},{"key":"b"},{"type":"delete"},[1],
                         {"key":"c","type":"upsert","value":[1]}]}"#;
        assert_same(body);
        let lean = lean_cycle(body).expect("lean");
        let payloads: Vec<(usize, &[u8])> = lean.pushes.iter().map(|p| (p.index, &*p.payload)).collect();
        assert_eq!(
            payloads,
            [(0, &b"null"[..]), (1, &br#"{"x":1}"#[..]), (2, &b"{}"[..]), (5, &br#"{"y":2}"#[..])]
        );
        let values: Vec<(usize, &[u8])> = lean.ops.iter().map(|o| (o.index, &*o.value)).collect();
        assert_eq!(
            values,
            [(0, &b"null"[..]), (1, &b"null"[..]), (2, &b"null"[..]), (4, &b"[1]"[..])]
        );
    }

    #[test]
    fn bodies_the_tree_reads_otherwise_are_left_to_it() {
        for body in [
            // A repeated key: the tree keeps the last one.
            &br#"{"query_id":"a","query_id":"b","partition_id":"1"}"#[..],
            &br#"{"query_id":"q","partition_id":"1","push_items":[{"queue":"s","queue":"t"}]}"#[..],
            // Not an object.
            &br#"[{"query_id":"q"}]"#[..],
            // Documents `Value` refuses, which `RawValue` alone would store.
            &br#"{"query_id":"q","partition_id":"1","state_ops":[{"key":"k","value":1e400}]}"#[..],
            &br#"{"query_id":"q","partition_id":"1","state_ops":[{"key":"k","value":"\ud800"}]}"#[..],
            &b"not json"[..],
        ] {
            assert!(lean_cycle(body).is_none(), "{}", String::from_utf8_lossy(body));
        }
        let last = tree(br#"{"query_id":"a","query_id":"b","partition_id":"1"}"#);
        assert_eq!(last.query_id, Some(Value::from("b")));
        // Fields that are not arrays hold no items, in both reads.
        assert_same(br#"{"query_id":"q","partition_id":"1","push_items":{"queue":"s"},"state_ops":"x"}"#);
    }

    #[test]
    fn a_large_cycle_body_is_read_without_a_tree() {
        let state = serde_json::to_string(&super::super::json_out::trace_state(20_000)).unwrap();
        let body = format!(
            r#"{{"query_id":"q","partition_id":"1","state_ops":[{{"type":"upsert","key":"k","value":{state}}}],"push_items":[{{"queue":"s","payload":{state}}}]}}"#
        );
        let (read, peak) = crate::test_alloc::peak_during(|| {
            lean_cycle(body.as_bytes()).map(|c| (c.ops[0].value.len(), c.pushes[0].payload.len()))
        });
        assert_eq!(read, Some((state.len(), state.len())));
        // The tree read took about ten times the body.
        if let Some(peak) = peak {
            eprintln!("cycle: {peak} bytes at the peak for a {} byte body", body.len());
            assert!(peak < 1 << 20, "{peak} bytes at the peak for a {} byte body", body.len());
        }
    }
}
