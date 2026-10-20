//! Fetch and ListOffsets → reads at an offset.
//!
//! A Kafka Fetch names no consumer group and reads ahead of its commits, so it
//! is not a pop: it is the broker's read of a partition from an offset, with
//! its long poll. Served by the raft leader only: a follower's applied state
//! can be behind the offsets a consumer already read from the leader, and
//! answering from it would read as OFFSET_OUT_OF_RANGE.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::{
    FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse, TopicName,
};
use kafka_protocol::records::{Record, TimestampType, NO_PRODUCER_EPOCH, NO_PRODUCER_ID};

use crate::conn::Conn;
use crate::core::{FetchEntry, LocalAnswer, RawFetched, RawRecord, TimeLookup};
use crate::errors::*;
use crate::records;
use crate::server::Ctx;
use crate::wire;

struct Want {
    index: i32,
    offset: i64,
    error: i16,
    entry: Option<usize>,
}

pub async fn fetch(ctx: &Arc<Ctx>, conn: &Conn, req: FetchRequest, _version: i16) -> FetchResponse {
    let bearer = conn.bearer.clone();
    let leads = ctx.broker.cluster().leads();
    let snap = if leads {
        let names = req.topics.iter().map(|t| t.topic.0.as_str());
        ctx.topics.covering(bearer.as_deref(), names).await.ok()
    } else {
        None
    };
    let mut entries: Vec<FetchEntry> = Vec::new();
    let mut shape: Vec<(TopicName, String, Vec<Want>)> = Vec::with_capacity(req.topics.len());
    let mut any_error = false;
    for t in req.topics {
        let name = t.topic.0.to_string();
        let width = snap.as_ref().and_then(|s| s.widths.get(&name).copied());
        let mut wants = Vec::with_capacity(t.partitions.len());
        for p in t.partitions {
            let mut w = Want {
                index: p.partition,
                offset: p.fetch_offset,
                error: NONE,
                entry: None,
            };
            if !leads || snap.is_none() {
                w.error = NOT_LEADER_OR_FOLLOWER;
            } else if !width.is_some_and(|wd| (0..wd).contains(&p.partition)) {
                w.error = UNKNOWN_TOPIC_OR_PARTITION;
            } else if p.fetch_offset < 0 {
                w.error = OFFSET_OUT_OF_RANGE;
            } else {
                w.entry = Some(entries.len());
                entries.push(FetchEntry {
                    queue: name.clone(),
                    partition: p.partition.to_string(),
                    offset: p.fetch_offset,
                    max_bytes: i64::from(p.partition_max_bytes.max(1)),
                });
            }
            any_error |= w.error != NONE;
            wants.push(w);
        }
        shape.push((t.topic, name, wants));
    }

    let read: Result<Vec<RawFetched>, i16> = if entries.is_empty() {
        Ok(Vec::new())
    } else {
        let asked = entries.len();
        // An error in the answer is news for the client: do not park on it.
        let wait = if any_error {
            0
        } else {
            i64::from(req.max_wait_ms.max(0))
        };
        let max_bytes = (req.max_bytes > 0).then_some(i64::from(req.max_bytes));
        match ctx
            .broker
            .fetch(
                bearer.clone(),
                entries,
                wait,
                i64::from(req.min_bytes.max(0)),
                max_bytes,
            )
            .await
        {
            Ok(LocalAnswer::Done(v)) if v.len() == asked => Ok(v),
            Ok(LocalAnswer::Done(_)) => Err(UNKNOWN_SERVER_ERROR),
            Ok(LocalAnswer::Refused(r)) => {
                tracing::debug!(target: "kafka", status = r.status, body = %r.body, "fetch refused");
                Err(match r.status {
                    401 | 403 => TOPIC_AUTHORIZATION_FAILED,
                    _ => NOT_LEADER_OR_FOLLOWER,
                })
            }
            Err(e) => {
                tracing::warn!(target: "kafka", error = %e, "fetch failed");
                Err(NOT_LEADER_OR_FOLLOWER)
            }
        }
    };

    let mut counts: HashMap<String, u64> = HashMap::new();
    let mut read = read.map(|v| v.into_iter().map(Some).collect::<Vec<_>>());
    let responses = shape
        .into_iter()
        .map(|(topic, name, wants)| {
            let partitions = wants
                .into_iter()
                .map(|w| {
                    let base = PartitionData::default()
                        .with_partition_index(w.index)
                        .with_high_watermark(-1)
                        .with_last_stable_offset(-1)
                        .with_log_start_offset(-1)
                        .with_aborted_transactions(Some(Vec::new()))
                        // An empty record set, never a null one: librdkafka
                        // refuses a MessageSetSize of -1.
                        .with_records(Some(Bytes::new()));
                    if w.error != NONE {
                        return base.with_error_code(w.error);
                    }
                    let got = match (&mut read, w.entry) {
                        (Err(code), _) => return base.with_error_code(*code),
                        (Ok(v), Some(i)) => v.get_mut(i).and_then(Option::take),
                        (Ok(_), None) => None,
                    };
                    let Some(got) = got else {
                        return base.with_error_code(UNKNOWN_SERVER_ERROR);
                    };
                    let (error, hwm, start) = match got.error.as_deref() {
                        None => (NONE, got.high_watermark, got.log_start_offset),
                        // A partition of the topic's width nobody wrote yet:
                        // empty, from 0.
                        Some("UNKNOWN_TOPIC_OR_PARTITION") if w.offset == 0 => (NONE, 0, 0),
                        Some("UNKNOWN_TOPIC_OR_PARTITION") => (OFFSET_OUT_OF_RANGE, 0, 0),
                        Some("OFFSET_OUT_OF_RANGE") => {
                            (OFFSET_OUT_OF_RANGE, got.high_watermark, got.log_start_offset)
                        }
                        Some(other) => {
                            tracing::warn!(target: "kafka", topic = %name, partition = w.index, error = other, "fetch error");
                            (UNKNOWN_SERVER_ERROR, -1, -1)
                        }
                    };
                    let (batch, n) = batch(&got.records);
                    if n > 0 {
                        *counts.entry(name.clone()).or_default() += n as u64;
                    }
                    base.with_error_code(error)
                        .with_high_watermark(hwm)
                        .with_last_stable_offset(hwm)
                        .with_log_start_offset(start)
                        .with_records(Some(batch))
                })
                .collect();
            FetchableTopicResponse::default()
                .with_topic(topic)
                .with_partitions(partitions)
        })
        .collect();
    if !counts.is_empty() {
        let counts: Vec<(String, u64)> = counts.into_iter().collect();
        ctx.broker.note_fetched(bearer.as_deref(), &counts);
    }
    FetchResponse::default().with_responses(responses)
}

/// The records as one v2 batch. Offsets may have holes (the batch carries
/// per-record deltas); the `sequence` is set so the encoder keeps one batch.
fn batch(read: &[RawRecord]) -> (Bytes, usize) {
    let Some(base) = read.iter().map(|r| r.offset).min() else {
        return (Bytes::new(), 0);
    };
    let mut out: Vec<Record> = Vec::with_capacity(read.len());
    let mut headers: Vec<Vec<wire::Header>> = Vec::with_capacity(read.len());
    for r in read {
        if r.offset - base > i64::from(i32::MAX) {
            break;
        }
        let d = records::decode_bytes(&r.payload, r.timestamp_ms);
        headers.push(d.headers);
        out.push(Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: r.offset,
            sequence: (-1i64 + (r.offset - base)) as i32,
            timestamp: d.timestamp,
            key: d.key,
            value: d.value,
            headers: IndexMap::new(),
        });
    }
    match wire::encode(&out, &headers) {
        Ok(bytes) => (bytes, out.len()),
        Err(e) => {
            tracing::error!(target: "kafka", error = %e, "cannot encode a record batch");
            (Bytes::new(), 0)
        }
    }
}

const LATEST: i64 = -1;
const EARLIEST: i64 = -2;

pub async fn list_offsets(
    ctx: &Arc<Ctx>,
    conn: &Conn,
    req: ListOffsetsRequest,
    _version: i16,
) -> ListOffsetsResponse {
    let bearer = conn.bearer.clone();
    let leads = ctx.broker.cluster().leads();
    let snap = if leads {
        ctx.topics.snapshot(bearer.as_deref()).await.ok()
    } else {
        None
    };
    // (topic, partition index, timestamp, error)
    let mut asks: Vec<(String, i32, i64, i16)> = Vec::new();
    let mut shape: Vec<(TopicName, Vec<usize>)> = Vec::new();
    for t in req.topics {
        let name = t.name.0.to_string();
        let width = snap.as_ref().and_then(|s| s.widths.get(&name).copied());
        let mut idx = Vec::new();
        for p in t.partitions {
            let error = if !leads || snap.is_none() {
                NOT_LEADER_OR_FOLLOWER
            } else if !width.is_some_and(|w| (0..w).contains(&p.partition_index)) {
                UNKNOWN_TOPIC_OR_PARTITION
            } else {
                NONE
            };
            idx.push(asks.len());
            asks.push((name.clone(), p.partition_index, p.timestamp, error));
        }
        shape.push((t.name, idx));
    }

    // Bounds for latest/earliest: one read of a byte from offset 0 answers
    // the high watermark and the log start of every partition at once.
    let bounds_of: Vec<usize> = (0..asks.len())
        .filter(|&i| asks[i].3 == NONE && (asks[i].2 == LATEST || asks[i].2 == EARLIEST))
        .collect();
    let mut bounds: HashMap<usize, Result<(i64, i64), i16>> = HashMap::new();
    if !bounds_of.is_empty() {
        let entries: Vec<FetchEntry> = bounds_of
            .iter()
            .map(|&i| FetchEntry {
                queue: asks[i].0.clone(),
                partition: asks[i].1.to_string(),
                offset: 0,
                max_bytes: 1,
            })
            .collect();
        let n = entries.len();
        let got = ctx
            .broker
            .fetch(bearer.clone(), entries, 0, 0, Some(1))
            .await;
        for (k, &i) in bounds_of.iter().enumerate() {
            let r = match &got {
                Ok(LocalAnswer::Done(v)) if v.len() == n => {
                    let f = &v[k];
                    match f.error.as_deref() {
                        Some("UNKNOWN_TOPIC_OR_PARTITION") => Ok((0, 0)),
                        _ => Ok((f.high_watermark.max(0), f.log_start_offset.max(0))),
                    }
                }
                Ok(LocalAnswer::Refused(r)) => Err(of_refusal(r)),
                _ => Err(NOT_LEADER_OR_FOLLOWER),
            };
            bounds.insert(i, r);
        }
    }
    // Times.
    let times_of: Vec<usize> = (0..asks.len())
        .filter(|&i| asks[i].3 == NONE && asks[i].2 >= 0)
        .collect();
    let mut times: HashMap<usize, Result<(i64, i64), i16>> = HashMap::new();
    if !times_of.is_empty() {
        let lookups: Vec<TimeLookup> = times_of
            .iter()
            .map(|&i| TimeLookup {
                queue: asks[i].0.clone(),
                partition: asks[i].1.to_string(),
                timestamp_ms: asks[i].2,
            })
            .collect();
        let got = ctx.queen.times(bearer.as_deref(), &lookups).await;
        for (k, &i) in times_of.iter().enumerate() {
            let r = match &got {
                Ok(v) => match (&v[k].offset, v[k].timestamp_ms) {
                    (Some(o), Some(ts)) => Ok((*o, ts)),
                    (Some(o), None) => Ok((*o, asks[i].2)),
                    _ => Ok((-1, -1)),
                },
                Err(e) => Err(of_call(e)),
            };
            times.insert(i, r);
        }
    }

    let topics = shape
        .into_iter()
        .map(|(name, idx)| {
            let partitions = idx
                .into_iter()
                .map(|i| {
                    let (_, index, ts, error) = &asks[i];
                    let base = ListOffsetsPartitionResponse::default()
                        .with_partition_index(*index)
                        .with_timestamp(-1)
                        .with_offset(-1);
                    if *error != NONE {
                        return base.with_error_code(*error);
                    }
                    let found = if *ts == LATEST || *ts == EARLIEST {
                        bounds
                            .get(&i)
                            .cloned()
                            .unwrap_or(Err(UNKNOWN_SERVER_ERROR))
                            .map(|(hwm, start)| {
                                if *ts == LATEST {
                                    (hwm, -1)
                                } else {
                                    (start, -1)
                                }
                            })
                    } else if *ts >= 0 {
                        times.get(&i).cloned().unwrap_or(Err(UNKNOWN_SERVER_ERROR))
                    } else {
                        Err(INVALID_REQUEST)
                    };
                    match found {
                        Ok((offset, timestamp)) => {
                            base.with_offset(offset).with_timestamp(timestamp)
                        }
                        Err(code) => base.with_error_code(code),
                    }
                })
                .collect();
            ListOffsetsTopicResponse::default()
                .with_name(name)
                .with_partitions(partitions)
        })
        .collect();
    ListOffsetsResponse::default().with_topics(topics)
}
