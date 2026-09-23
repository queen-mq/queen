//! The typed Kafka record path over the real [`RaftFacade`] (phase 2 of the
//! Kafka-on-raft plan, [`crate::rsm::kafka_batch`]).
//!
//! Real RecordBatch v2 bytes, built by `kafka-protocol` the way a client builds
//! them, appended verbatim and read back — through the typed read a Kafka Fetch
//! uses, through a NATIVE pop and its ack, and through the JSON fetch — so both
//! halves of "Kafka in, Queen out" are pinned on the new storage shape.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use serde_json::Value;

use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{
    AckReq, ApiReq, Deadline, KafkaAppendReq, KafkaChunk, KafkaReadReq, PopReq, PushReq, ReqCtx,
    Rsm, RsmBuildCtx,
};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-kafka-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn build_ctx(dir: &Path) -> RsmBuildCtx {
    RsmBuildCtx {
        data_dir: dir.display().to_string(),
        notifier: crate::notify::Notifier::new(false),
    }
}

fn ctx() -> ReqCtx {
    ReqCtx::new(
        crate::config::DEFAULT_TENANT,
        Deadline::after(Duration::from_secs(5)),
    )
}

/// A client's batch of `values`, keys `k0..`, one header, offsets from 0.
fn batch(values: &[&str], compression: Compression) -> Bytes {
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            // `offset - sequence` constant: the encoder's condition for ONE
            // batch (it starts a new one whenever the difference changes), and
            // a base sequence of -1, "no idempotent producer".
            sequence: i as i32 - 1,
            timestamp: 1_756_000_000_000 + i as i64,
            key: Some(Bytes::from(format!("k{i}"))),
            value: Some(Bytes::copy_from_slice(v.as_bytes())),
            headers: {
                let mut h = IndexMap::new();
                h.insert(
                    StrBytes::from_static_str("h"),
                    Some(Bytes::from_static(b"x")),
                );
                h
            },
        })
        .collect();
    let mut out = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut out,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression,
        },
    )
    .expect("encode");
    out.freeze()
}

/// `(offset, value)` of every record in `bytes`, as a Kafka client decodes it.
fn records_of(bytes: &Bytes) -> Vec<(i64, String)> {
    let mut b = bytes.clone();
    let mut out = Vec::new();
    while !b.is_empty() {
        let set = RecordBatchDecoder::decode(&mut b).expect("decode");
        for r in set.records {
            out.push((
                r.offset,
                String::from_utf8(r.value.unwrap_or_default().to_vec()).unwrap(),
            ));
        }
    }
    out
}

fn part(queue: &str, partition: &str, batches: Bytes) -> KafkaAppendReq {
    KafkaAppendReq {
        queue: queue.into(),
        partition: partition.into(),
        batches,
    }
}

fn ask(queue: &str, partition: &str, offset: i64) -> KafkaReadReq {
    KafkaReadReq {
        queue: queue.into(),
        partition: partition.into(),
        offset,
        max_bytes: 1 << 20,
    }
}

fn all_records(chunks: &[KafkaChunk]) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    for c in chunks {
        match c {
            KafkaChunk::Batches(b) => out.extend(records_of(b)),
            KafkaChunk::Messages(m) => out.extend(
                m.iter()
                    .map(|(o, _, p)| (*o as i64, String::from_utf8_lossy(p).into_owned())),
            ),
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn batches_are_numbered_stored_verbatim_and_read_back() {
    let dir = scratch("verbatim");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    // Two batches in one part, lz4; a second partition; then a third batch.
    let mut two = BytesMut::new();
    two.extend_from_slice(&batch(&["a", "b", "c"], Compression::Lz4));
    two.extend_from_slice(&batch(&["d", "e"], Compression::Lz4));
    let got = facade
        .kafka_append(
            ctx(),
            vec![
                part("k", "0", two.freeze()),
                part("k", "1", batch(&["x"], Compression::Zstd)),
            ],
        )
        .await
        .expect("append");
    assert_eq!(got.len(), 2);
    assert_eq!(*got[0].as_ref().unwrap(), 0);
    assert_eq!(*got[1].as_ref().unwrap(), 0);
    let got = facade
        .kafka_append(
            ctx(),
            vec![part("k", "0", batch(&["f", "g"], Compression::None))],
        )
        .await
        .expect("append");
    assert_eq!(*got[0].as_ref().unwrap(), 5, "numbered from the tail");

    // The typed read: every record at its stamped offset, bounds exact.
    let out = facade
        .kafka_read(ctx(), vec![ask("k", "0", 0), ask("k", "1", 0)], 0, 1)
        .await
        .expect("read");
    assert_eq!(out[0].high_watermark, 7);
    assert_eq!(out[0].log_start, 0);
    assert_eq!(out[0].error, None);
    assert!(out[0]
        .chunks
        .iter()
        .all(|c| matches!(c, KafkaChunk::Batches(_))));
    let values: Vec<(i64, String)> = all_records(&out[0].chunks);
    assert_eq!(
        values,
        vec![
            (0, "a".into()),
            (1, "b".into()),
            (2, "c".into()),
            (3, "d".into()),
            (4, "e".into()),
            (5, "f".into()),
            (6, "g".into()),
        ]
    );
    assert_eq!(all_records(&out[1].chunks), vec![(0, "x".to_string())]);

    // From the middle: the answer starts at the BATCH holding the offset.
    let out = facade
        .kafka_read(ctx(), vec![ask("k", "0", 4)], 0, 1)
        .await
        .unwrap();
    assert_eq!(all_records(&out[0].chunks)[0].0, 3);

    // Out of range, and a topic that is not there.
    let out = facade
        .kafka_read(ctx(), vec![ask("k", "0", 99), ask("nope", "0", 0)], 0, 1)
        .await
        .unwrap();
    assert_eq!(out[0].error, Some("OFFSET_OUT_OF_RANGE"));
    assert_eq!(out[0].high_watermark, 7);
    assert_eq!(out[1].error, Some("UNKNOWN_TOPIC_OR_PARTITION"));

    // A clean restart reads back the same bytes.
    facade.shutdown().await;
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("reopen facade");
    let out = facade
        .kafka_read(ctx(), vec![ask("k", "0", 0)], 0, 1)
        .await
        .unwrap();
    assert_eq!(all_records(&out[0].chunks).len(), 7);
    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_native_consumer_pops_and_acks_kafka_records() {
    let dir = scratch("native");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    facade
        .kafka_append(
            ctx(),
            vec![part(
                "mix",
                "0",
                batch(&["one", "two"], Compression::Snappy),
            )],
        )
        .await
        .unwrap()[0]
        .as_ref()
        .unwrap();
    // A Queen producer writes to the same partition, after the Kafka one.
    let pushed = facade
        .push(
            ctx(),
            PushReq {
                raw: br#"{"items":[{"queue":"mix","partition":"0","payload":{"n":3},"transactionId":"t3"}]}"#
                    .to_vec(),
            },
        )
        .await
        .expect("push");
    let pushed: Value = serde_json::from_str(&pushed.body).unwrap();
    assert_eq!(pushed[0]["offset"].as_u64(), Some(2));

    // The typed read interleaves them in offset order.
    let out = facade
        .kafka_read(ctx(), vec![ask("mix", "0", 0)], 0, 1)
        .await
        .unwrap();
    assert!(matches!(out[0].chunks[0], KafkaChunk::Batches(_)));
    assert!(matches!(out[0].chunks[1], KafkaChunk::Messages(_)));
    assert_eq!(
        all_records(&out[0].chunks)
            .iter()
            .map(|(o, _)| *o)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );

    // A native pop sees three messages: the Kafka records as their envelopes.
    let popped = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "mix".into(),
                group: None,
                batch: 10,
                auto_ack: false,
                wait: false,
                timeout_ms: 1000,
                options: Default::default(),
            },
        )
        .await
        .expect("pop");
    let pop: Value = serde_json::from_str(&popped.body).unwrap();
    let msgs = pop["messages"].as_array().expect("messages");
    assert_eq!(msgs.len(), 3, "{}", popped.body);
    assert_eq!(msgs[0]["transactionId"], "kafka:0");
    assert_eq!(msgs[1]["transactionId"], "kafka:1");
    assert_eq!(msgs[2]["transactionId"], "t3");
    let env = crate::rsm::kafka_batch::queen_view(
        &crate::rsm::kafka_batch::wrap(&batch(&["one"], Compression::None)),
        0,
    )
    .unwrap();
    let want: Value = serde_json::from_slice(&env[0].payload).unwrap();
    assert_eq!(
        msgs[0]["data"]["v"], want["v"],
        "the envelope, base64 value"
    );
    assert_eq!(msgs[0]["data"]["k"], want["k"]);
    assert_eq!(msgs[2]["data"]["n"], 3);
    assert_ne!(msgs[0]["id"], msgs[1]["id"]);

    // Ack all three by the ids the pop rendered: the Kafka ones resolve by
    // their synthetic hashes like any message.
    let partition_id = pop["partitionId"].as_str().unwrap().to_string();
    let lease_id = pop["leaseId"].as_str().unwrap().to_string();
    let acks: Vec<String> = msgs
        .iter()
        .map(|m| {
            format!(
                r#"{{"transactionId":"{}","partitionId":"{partition_id}","status":"completed","leaseId":"{lease_id}"}}"#,
                m["transactionId"].as_str().unwrap()
            )
        })
        .collect();
    let acked = facade
        .ack(
            ctx(),
            AckReq {
                queue: Some("mix".into()),
                group: "__QUEUE_MODE__".into(),
                raw: format!(
                    r#"{{"consumerGroup":"__QUEUE_MODE__","acknowledgments":[{}]}}"#,
                    acks.join(",")
                )
                .into_bytes(),
            },
        )
        .await
        .expect("ack");
    let ack: Value = serde_json::from_str(&acked.body).unwrap();
    for r in ack.as_array().unwrap() {
        assert_eq!(r["success"], true, "{r}");
    }
    let empty = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "mix".into(),
                group: None,
                batch: 10,
                auto_ack: false,
                wait: false,
                timeout_ms: 1000,
                options: Default::default(),
            },
        )
        .await
        .unwrap();
    assert!(empty.empty, "everything acked: {}", empty.body);

    // The JSON fetch reads the same envelopes, at the same offsets.
    let fetched = facade
        .api(
            ctx(),
            ApiReq {
                method: "POST".into(),
                path: "/api/v1/fetch".into(),
                query: None,
                body: br#"{"entries":[{"queue":"mix","partition":"0","offset":0}]}"#.to_vec(),
            },
        )
        .await
        .expect("json fetch");
    let f: Value = serde_json::from_str(&fetched.body).unwrap();
    let recs = f["entries"][0]["records"].as_array().expect("records");
    assert_eq!(recs.len(), 3, "{}", fetched.body);
    assert_eq!(recs[0]["offset"], 0);
    assert_eq!(recs[0]["transactionId"], "kafka:0");
    assert_eq!(recs[0]["payload"]["v"], want["v"]);
    assert_eq!(recs[2]["payload"]["n"], 3);

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn what_cannot_be_numbered_is_refused_per_part() {
    let dir = scratch("refused");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let mut broken = batch(&["a", "b"], Compression::None).to_vec();
    broken[16] = 1; // magic 1
    let got = facade
        .kafka_append(
            ctx(),
            vec![
                part("r", "0", Bytes::from(broken)),
                part("r", "1", batch(&["ok"], Compression::None)),
            ],
        )
        .await
        .expect("append call");
    assert!(got[0].is_err(), "{:?}", got[0]);
    assert_eq!(*got[1].as_ref().unwrap(), 0, "the other part landed");
    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A batch from an idempotent producer: `(producer_id, epoch)` and sequences
/// from `base_seq`, one batch (offset - sequence constant).
fn idem_batch(values: &[&str], producer_id: i64, epoch: i16, base_seq: i32) -> Bytes {
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id,
            producer_epoch: epoch,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            sequence: base_seq + i as i32,
            timestamp: 1_756_000_000_000,
            key: None,
            value: Some(Bytes::copy_from_slice(v.as_bytes())),
            headers: IndexMap::new(),
        })
        .collect();
    let mut out = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut out,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .expect("encode");
    out.freeze()
}

async fn append(f: &RaftFacade, b: Bytes) -> Result<u64, crate::rsm::facade::RsmError> {
    f.kafka_append(ctx(), vec![part("idem", "0", b)])
        .await
        .unwrap()
        .remove(0)
}

async fn hwm(f: &RaftFacade) -> i64 {
    f.kafka_read(ctx(), vec![ask("idem", "0", 0)], 0, 1)
        .await
        .unwrap()[0]
        .high_watermark
}

/// The idempotent producer's window is the LOG's: a resend is answered with
/// the original offsets and writes nothing, a gap is refused with Kafka's own
/// code — and all of it survives a restart, which the facade's in-memory
/// window never did.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn an_idempotent_producers_window_is_durable() {
    let dir = scratch("idem");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let first = idem_batch(&["a", "b"], 77, 0, 0);
    let second = idem_batch(&["c", "d", "e"], 77, 0, 2);
    assert_eq!(append(&facade, first.clone()).await.unwrap(), 0);
    assert_eq!(append(&facade, first.clone()).await.unwrap(), 0, "a resend");
    assert_eq!(append(&facade, second.clone()).await.unwrap(), 2);
    assert_eq!(hwm(&facade).await, 5, "the resend wrote nothing");
    let gap = append(&facade, idem_batch(&["z"], 77, 0, 9)).await;
    match gap {
        Err(crate::rsm::facade::RsmError::Rejected { code, .. }) => {
            assert_eq!(code, "OUT_OF_ORDER_SEQUENCE_NUMBER")
        }
        other => panic!("a gap was not refused: {other:?}"),
    }
    // A non-idempotent append beside it is untouched by the window.
    assert_eq!(
        append(&facade, batch(&["n"], Compression::None))
            .await
            .unwrap(),
        5
    );

    // Restart: the window is still there.
    facade.shutdown().await;
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("reopen facade");
    assert_eq!(
        append(&facade, second.clone()).await.unwrap(),
        2,
        "the resend after a restart is still a duplicate"
    );
    assert_eq!(hwm(&facade).await, 6);
    assert_eq!(
        append(&facade, idem_batch(&["f"], 77, 0, 5)).await.unwrap(),
        6,
        "and the next sequence still lands"
    );
    // A bumped epoch resets the sequences.
    assert_eq!(
        append(&facade, idem_batch(&["g"], 77, 1, 0)).await.unwrap(),
        7
    );
    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
