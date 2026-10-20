//! Produce → push.
//!
//! Partition `p` of topic `T` is partition `"p"` of queue `T`; each record is
//! one message, its key, value, headers and timestamp in the envelope of
//! [`crate::records`]. A request is ONE ordered push: once it is in the raft
//! log the connection reads its next request, so batches of one connection
//! land in the order they were sent.
//!
//! The idempotent producer's id and sequences are accepted and not checked:
//! a retry after an unknown outcome can write a batch twice (phase 2: writer
//! dedup in the broker, REDESIGN.md).

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
    let acks = req.acks;
    let bearer = conn.bearer.clone();
    let leads = ctx.broker.cluster().leads();
    let transactional = req.transactional_id.is_some();
    let snap = if leads && !transactional {
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
            if transactional {
                slot.error = TRANSACTIONAL_ID_AUTHORIZATION_FAILED;
            } else if !leads {
                slot.error = NOT_LEADER_OR_FOLLOWER;
            } else if snap.is_none() {
                slot.error = REQUEST_TIMED_OUT;
            } else if !width.is_some_and(|w| (0..w).contains(&p.index)) {
                slot.error = UNKNOWN_TOPIC_OR_PARTITION;
            } else if let Some(mut raw) = p.records {
                match decompress::decode_all(&mut raw, &budget) {
                    Ok(batches) => {
                        let partition = p.index.to_string();
                        for b in batches {
                            let lists = wire::header_lists(&b.records, &b.set.records);
                            for (i, r) in b.set.records.iter().enumerate() {
                                let headers =
                                    lists.as_ref().and_then(|l| l.get(i)).map(Vec::as_slice);
                                items.push(RawPush {
                                    queue: name.clone(),
                                    partition: partition.clone(),
                                    payload: records::encode_bytes(r, headers),
                                });
                            }
                        }
                    }
                    Err(Refusal::Corrupt(why)) => {
                        tracing::warn!(target: "kafka", topic = %name, partition = p.index, %why, "corrupt batch");
                        slot.error = CORRUPT_MESSAGE;
                    }
                    Err(Refusal::TooLarge(why)) => {
                        tracing::warn!(target: "kafka", topic = %name, partition = p.index, %why, "batch too large");
                        slot.error = MESSAGE_TOO_LARGE;
                    }
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
