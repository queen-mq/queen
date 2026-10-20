//! Produce → push.
//!
//! Partition `p` of topic `T` is partition `"p"` of queue `T`; each record is
//! one message, its key, value, headers and timestamp in the envelope of
//! [`crate::records`]. A request is ONE ordered push: once it is in the raft
//! log the connection reads its next request, so batches of one connection
//! land in the order they were sent.
//!
//! An idempotent producer's record carries `k<producer id>.<epoch>.<sequence>`
//! as its Queen transaction id, so a retry of a batch whose outcome the
//! producer never learned is a duplicate in the queue's dedup window: nothing
//! is written twice and the answer carries the original offsets. Order across
//! the batches of one connection is the ordered push's (`in_log`).

use std::sync::Arc;

use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ApiKey, ProduceRequest, ProduceResponse, TopicName};

use crate::conn::{encode, Conn, Step};
use crate::core::{LocalAnswer, Pushed, RawPush};
use crate::decompress::{self, Budget, Refusal};
use crate::errors::*;
use crate::records;
use crate::server::Ctx;
use crate::wire;

/// What one request may decompress to, and how many records it may carry.
const MAX_DECOMPRESSED_BYTES: usize = 256 << 20;
const MAX_RECORDS: usize = 2_000_000;

struct Slot {
    index: i32,
    error: i16,
    first: usize,
    count: usize,
}

pub async fn produce(
    ctx: &Arc<Ctx>,
    conn: &Conn,
    req: ProduceRequest,
    version: i16,
    correlation: i32,
) -> Step {
    if let Some(tid) = req.transactional_id.as_ref().filter(|t| !t.0.is_empty()) {
        let tid = tid.0.to_string();
        let out = produce_txn(ctx, conn, &tid, req).await;
        return crate::conn::ready(encode(ApiKey::Produce, version, correlation, &out));
    }
    let acks = req.acks;
    let bearer = conn.bearer.clone();
    let leads = ctx.broker.cluster().leads();
    let snap = if leads {
        let names = req.topic_data.iter().map(|t| t.name.0.as_str());
        ctx.topics.covering(bearer.as_deref(), names).await.ok()
    } else {
        None
    };
    let budget = Budget::new(MAX_DECOMPRESSED_BYTES, MAX_RECORDS);
    let mut items: Vec<RawPush> = Vec::new();
    let mut shape: Vec<(TopicName, Vec<Slot>)> = Vec::with_capacity(req.topic_data.len());
    // `acks=0` has no answer to carry an error: the connection closes
    // instead, as Kafka does, and the client refreshes its metadata.
    let mut any_error = false;
    for t in req.topic_data {
        let name = t.name.0.to_string();
        // A topic the cache does not know yet (created a moment ago on
        // another node) is not refused: the push creates its queue, as a
        // native push does, at the default width.
        let width = snap.as_ref().map(|s| {
            s.widths
                .get(&name)
                .copied()
                .unwrap_or(ctx.topics.default_partitions())
        });
        let mut slots = Vec::with_capacity(t.partition_data.len());
        for p in t.partition_data {
            let mut slot = Slot {
                index: p.index,
                error: NONE,
                first: items.len(),
                count: 0,
            };
            if !leads {
                slot.error = NOT_LEADER_OR_FOLLOWER;
            } else if snap.is_none() {
                slot.error = REQUEST_TIMED_OUT;
            } else if !width.is_some_and(|w| (0..w).contains(&p.index)) {
                slot.error = UNKNOWN_TOPIC_OR_PARTITION;
            } else if let Some(raw) = p.records {
                match decode(raw, &budget, &name, p.index) {
                    Ok((records, _)) => items.extend(records),
                    Err(code) => slot.error = code,
                }
            }
            if slot.error != NONE {
                items.truncate(slot.first);
                any_error = true;
            }
            slot.count = items.len() - slot.first;
            slots.push(slot);
        }
        shape.push((t.name, slots));
    }

    // Every topic Kafka writes to is a Kafka topic for the console.
    if let Some(snap) = snap.as_ref() {
        let mut topics: Vec<&str> = items.iter().map(|i| i.queue.as_str()).collect();
        topics.dedup();
        for topic in topics {
            let width = snap
                .widths
                .get(topic)
                .copied()
                .unwrap_or(ctx.topics.default_partitions());
            ctx.topics
                .ensure_marked(bearer.as_deref(), snap, topic, width)
                .await;
        }
    }
    // A topic an idempotent producer writes to needs a dedup window for its
    // transaction ids to mean anything (checked once per topic and process).
    let mut numbered: Vec<&str> = items
        .iter()
        .filter(|i| i.txn.is_some())
        .map(|i| i.queue.as_str())
        .collect();
    numbered.dedup();
    for topic in numbered {
        ctx.topics.ensure_dedup(bearer.as_deref(), topic).await;
    }
    let n = items.len();
    let answer = move |pushed: Result<Vec<Pushed>, i16>| -> ProduceResponse {
        let responses = shape
            .into_iter()
            .map(|(name, slots)| {
                let partition_responses = slots
                    .into_iter()
                    .map(|s| {
                        let (error, base) = if s.error != NONE {
                            (s.error, -1)
                        } else if s.count == 0 {
                            (NONE, -1)
                        } else {
                            match &pushed {
                                Err(code) => (*code, -1),
                                Ok(v) => outcome(&v[s.first..s.first + s.count]),
                            }
                        };
                        PartitionProduceResponse::default()
                            .with_index(s.index)
                            .with_error_code(error)
                            .with_base_offset(base)
                            .with_log_append_time_ms(-1)
                            .with_log_start_offset(-1)
                    })
                    .collect();
                TopicProduceResponse::default()
                    .with_name(name)
                    .with_partition_responses(partition_responses)
            })
            .collect();
        ProduceResponse::default().with_responses(responses)
    };

    if n == 0 {
        if acks == 0 {
            return if any_error { Step::Close } else { Step::Silent };
        }
        return crate::conn::ready(encode(
            ApiKey::Produce,
            version,
            correlation,
            &answer(Ok(Vec::new())),
        ));
    }

    let ordered = ctx.broker.push(bearer, items);
    let in_log = ordered.in_log.await;
    let done = ordered.done;
    let finish = async move {
        let pushed = match done.await {
            Ok(LocalAnswer::Done(v)) if v.len() == n => Ok(v),
            Ok(LocalAnswer::Done(v)) => {
                tracing::error!(target: "kafka", answered = v.len(), sent = n, "push answered a different number of records");
                Err(UNKNOWN_SERVER_ERROR)
            }
            Ok(LocalAnswer::Refused(r)) => {
                tracing::debug!(target: "kafka", status = r.status, body = %r.body, "push refused");
                Err(of_refusal(&r))
            }
            Err(e) => {
                tracing::warn!(target: "kafka", error = %e, "push failed");
                Err(REQUEST_TIMED_OUT)
            }
        };
        encode(ApiKey::Produce, version, correlation, &answer(pushed))
    };
    if acks == 0 {
        // Nothing goes back; the push still has to finish before the next
        // one when it is not yet in the log.
        if in_log {
            tokio::spawn(finish);
        } else {
            finish.await;
        }
        return if any_error { Step::Close } else { Step::Silent };
    }
    if in_log {
        Step::Reply(Box::pin(finish))
    } else {
        // Not in the log: the next request waits for this one's outcome.
        crate::conn::ready(finish.await)
    }
}

/// One partition's record set as Queen pushes, and the producer id and epoch
/// that wrote it (`-1` for a producer that numbers nothing).
fn decode(
    mut raw: bytes::Bytes,
    budget: &Budget,
    topic: &str,
    index: i32,
) -> Result<(Vec<RawPush>, (i64, i16)), i16> {
    match decompress::decode_all(&mut raw, budget) {
        Ok(batches) => {
            let partition = index.to_string();
            let mut out = Vec::new();
            let mut writer = (-1i64, -1i16);
            for b in batches {
                let lists = wire::header_lists(&b.records, &b.set.records);
                for (i, r) in b.set.records.iter().enumerate() {
                    if writer.0 < 0 {
                        writer = (r.producer_id, r.producer_epoch);
                    }
                    let headers = lists.as_ref().and_then(|l| l.get(i)).map(Vec::as_slice);
                    out.push(RawPush {
                        queue: topic.to_string(),
                        partition: partition.clone(),
                        payload: records::encode_bytes(r, headers),
                        txn: writer_txn(r.producer_id, r.producer_epoch, r.sequence),
                    });
                }
            }
            Ok((out, writer))
        }
        Err(Refusal::Corrupt(why)) => {
            tracing::warn!(target: "kafka", %topic, partition = index, %why, "corrupt batch");
            Err(CORRUPT_MESSAGE)
        }
        Err(Refusal::TooLarge(why)) => {
            tracing::warn!(target: "kafka", %topic, partition = index, %why, "batch too large");
            Err(MESSAGE_TOO_LARGE)
        }
    }
}

/// A transactional produce: the records are held by the coordinator until
/// EndTxn and nothing is pushed. No offset exists before the commit, so every
/// partition answers base offset -1.
async fn produce_txn(
    ctx: &Arc<Ctx>,
    conn: &Conn,
    tid: &str,
    req: ProduceRequest,
) -> ProduceResponse {
    let bearer = conn.bearer.clone();
    let leads = ctx.broker.cluster().leads();
    let snap = if leads {
        let names = req.topic_data.iter().map(|t| t.name.0.as_str());
        ctx.topics.covering(bearer.as_deref(), names).await.ok()
    } else {
        None
    };
    let budget = Budget::new(MAX_DECOMPRESSED_BYTES, MAX_RECORDS);
    let mut responses = Vec::with_capacity(req.topic_data.len());
    for t in req.topic_data {
        let name = t.name.0.to_string();
        let width = snap.as_ref().map(|s| {
            s.widths
                .get(&name)
                .copied()
                .unwrap_or(ctx.topics.default_partitions())
        });
        let mut partition_responses = Vec::with_capacity(t.partition_data.len());
        for p in t.partition_data {
            let code = if !leads {
                NOT_LEADER_OR_FOLLOWER
            } else if snap.is_none() {
                REQUEST_TIMED_OUT
            } else if !width.is_some_and(|w| (0..w).contains(&p.index)) {
                UNKNOWN_TOPIC_OR_PARTITION
            } else {
                match p.records.map(|raw| decode(raw, &budget, &name, p.index)) {
                    None => NONE,
                    Some(Err(code)) => code,
                    Some(Ok((records, (pid, epoch)))) if !records.is_empty() => {
                        ctx.txns
                            .stage_records(
                                bearer.as_deref(),
                                tid,
                                pid,
                                epoch,
                                (&name, p.index),
                                records,
                            )
                            .await
                    }
                    Some(Ok(_)) => NONE,
                }
            };
            partition_responses.push(
                PartitionProduceResponse::default()
                    .with_index(p.index)
                    .with_error_code(code)
                    .with_base_offset(-1)
                    .with_log_append_time_ms(-1)
                    .with_log_start_offset(-1),
            );
        }
        responses.push(
            TopicProduceResponse::default()
                .with_name(t.name)
                .with_partition_responses(partition_responses),
        );
    }
    ProduceResponse::default().with_responses(responses)
}

/// One partition's records: all written (or already there) → their first
/// offset; anything else → an error for the whole batch.
fn outcome(pushed: &[Pushed]) -> (i16, i64) {
    let ok = pushed
        .iter()
        .all(|p| matches!(p.status.as_str(), "queued" | "duplicate") && p.offset.is_some());
    if ok {
        (NONE, pushed[0].offset.unwrap_or(-1))
    } else {
        let reason = pushed
            .iter()
            .find(|p| !matches!(p.status.as_str(), "queued" | "duplicate"))
            .map(|p| p.status.clone())
            .unwrap_or_default();
        tracing::warn!(target: "kafka", %reason, "a record of the batch was not written");
        (KAFKA_STORAGE_ERROR, -1)
    }
}

/// The Queen transaction id of one record of an idempotent producer: its
/// producer id, epoch and sequence, which a retry repeats exactly. `None` for a
/// producer that numbers nothing (`producer_id` -1).
pub fn writer_txn(producer_id: i64, epoch: i16, sequence: i32) -> Option<String> {
    (producer_id >= 0 && sequence >= 0).then(|| format!("k{producer_id}.{epoch}.{sequence}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_numbered_record_has_a_transaction_id_and_a_plain_one_has_none() {
        assert_eq!(writer_txn(42, 3, 7).as_deref(), Some("k42.3.7"));
        assert_eq!(writer_txn(-1, -1, -1), None);
        assert_eq!(writer_txn(42, 0, -1), None);
    }
}
