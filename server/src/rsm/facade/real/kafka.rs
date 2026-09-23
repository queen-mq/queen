//! The typed Kafka record path of the raft facade (phase 2 of the Kafka-on-raft
//! plan): a Produce's batches appended VERBATIM, a Fetch answered with the
//! stored bytes. Reached only in-process, through `src/kafka_inproc.rs`, which
//! has already authenticated the caller and taken push admission.

use super::{
    default_queue_config, derived_request_id, read_error, reply_error, RaftFacade, ReqCtx, RsmError,
};
use crate::rsm::batcher::{Command, Reply, Submission};
use crate::rsm::entry::{Outcome, PushVerdict};
use crate::rsm::facade::{KafkaAppendReq, KafkaChunk, KafkaReadOut, KafkaReadReq};
use crate::rsm::kafka_batch;
use crate::rsm::planner::{bucket_of, PushCommand, PushItem};
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::store::rows::PartitionRow;
use crate::rsm::store::{Store, TypedReads};

/// Longest a Kafka Fetch may long-poll, as `POST /api/v1/fetch` caps it.
const MAX_WAIT_MS: u64 = 30_000;

/// Most Queen-pushed messages one partition's answer carries, as the JSON
/// fetch caps them.
const MAX_MESSAGES: usize = 10_000;

/// One partition resolved for a read: its row and its sealed segment files.
struct ReadPart {
    pid: u64,
    row: PartitionRow,
    sealed: Vec<u32>,
}

impl RaftFacade {
    /// [`crate::rsm::facade::Rsm::kafka_append`].
    pub(super) async fn kafka_append_impl(
        &self,
        ctx: ReqCtx,
        parts: Vec<KafkaAppendReq>,
    ) -> Result<Vec<Result<u64, RsmError>>, RsmError> {
        if parts.is_empty() {
            return Ok(Vec::new());
        }
        if self.storage_pressure() {
            return Err(RsmError::StorageFull);
        }
        // Push admission (crate::rsm::admit), as `push` takes it: sized by the
        // batch bytes, held until every part's reply is in.
        let bytes: usize = parts.iter().map(|p| p.batches.len()).sum();
        let _admitted = match self.admit.filter(|_| !crate::rsm::admit::pre_admitted()) {
            Some(gate) => Some(gate.admit(bytes).await.map_err(|o| RsmError::Overloaded {
                retry_after_s: o.retry_after_s,
            })?),
            None => None,
        };
        // A queue encrypted at rest cannot hold a plaintext batch: its parts
        // are stored as encrypted envelope frames, the pre-verbatim shape.
        let encrypted = self
            .encrypted_queues(&ctx.tenant, parts.iter().map(|p| p.queue.clone()).collect())
            .await?;

        let mut pending: Vec<Result<tokio::sync::oneshot::Receiver<Reply>, RsmError>> =
            Vec::with_capacity(parts.len());
        for (ordinal, part) in parts.into_iter().enumerate() {
            if let Err(e) = crate::rsm::facade::check_message_key_names(
                &ctx.tenant,
                &part.queue,
                None,
                Some(&part.partition),
            ) {
                pending.push(Err(e));
                continue;
            }
            let items = if encrypted.contains(&part.queue) {
                match self.kafka_envelope_items(&ctx, &part) {
                    Ok(items) => items,
                    Err(e) => {
                        pending.push(Err(e));
                        continue;
                    }
                }
            } else {
                vec![PushItem {
                    hash: [0; 16],
                    frame: kafka_batch::wrap(&part.batches),
                }]
            };
            let cmd = Command::Push(PushCommand {
                request_id: derived_request_id(ctx.request_id, ordinal as u32),
                tenant: ctx.tenant.clone(),
                create_cfg: default_queue_config(&part.queue),
                queue: part.queue,
                partition: part.partition,
                items,
            });
            let (sub, rx) = Submission::new(cmd);
            match tokio::time::timeout(ctx.deadline.remaining(), self.cmd_tx.send(sub)).await {
                Ok(Ok(())) => pending.push(Ok(rx)),
                Ok(Err(_)) => return Err(RsmError::Internal("planner channel closed".into())),
                Err(_) => return Err(RsmError::Timeout),
            }
        }

        let mut out = Vec::with_capacity(pending.len());
        for p in pending {
            let rx = match p {
                Ok(rx) => rx,
                Err(e) => {
                    out.push(Err(e));
                    continue;
                }
            };
            let reply = match tokio::time::timeout(ctx.deadline.remaining(), rx).await {
                Ok(Ok(r)) => r,
                Ok(Err(_)) => return Err(RsmError::Internal("planner dropped the reply".into())),
                Err(_) => return Err(RsmError::Timeout),
            };
            out.push(match reply {
                Reply::Done {
                    outcome: Outcome::Push(p),
                    ..
                } => match p.items.first() {
                    Some(PushVerdict::Created { offset, .. }) => Ok(*offset),
                    // An idempotent resend: the base the original got.
                    Some(PushVerdict::Duplicate { offset, .. }) => Ok(*offset),
                    other => Err(RsmError::Internal(format!(
                        "a Kafka append answered {other:?}"
                    ))),
                },
                Reply::Done { outcome, .. } => Err(RsmError::Internal(format!(
                    "a Kafka append got a non-push outcome: {outcome:?}"
                ))),
                other => Err(reply_error(other)),
            });
        }
        Ok(out)
    }

    /// The parts of an encrypted queue, as the envelope frames the JSON path
    /// stores: one frame per record, each with its own minted id, encrypted.
    #[cfg(feature = "kafka")]
    fn kafka_envelope_items(
        &self,
        ctx: &ReqCtx,
        part: &KafkaAppendReq,
    ) -> Result<Vec<PushItem>, RsmError> {
        let records =
            queen_kafka::stored::decode(&part.batches).map_err(|e| RsmError::Rejected {
                code: "bad_batch".into(),
                message: e,
            })?;
        let mut items = Vec::with_capacity(records.len());
        for r in records {
            let mid = crate::util::uuidv7_bytes();
            let txn = crate::frames::uuid_bytes_to_string(&mid);
            let (payload, encrypted) = self.encode_payload(true, &r.envelope, &part.queue);
            items.push(PushItem {
                hash: crate::util::txn_hash128(&txn),
                frame: crate::frames::pack_frames(&[crate::frames::FrameIn {
                    message_id: mid,
                    txn: &txn,
                    trace_id: None,
                    producer_sub: ctx.producer_sub.as_deref(),
                    payload: &payload,
                    encrypted,
                }]),
            });
        }
        if items.is_empty() {
            return Err(RsmError::Rejected {
                code: "bad_batch".into(),
                message: "a Kafka append with no records".into(),
            });
        }
        Ok(items)
    }

    #[cfg(not(feature = "kafka"))]
    fn kafka_envelope_items(
        &self,
        _ctx: &ReqCtx,
        _part: &KafkaAppendReq,
    ) -> Result<Vec<PushItem>, RsmError> {
        Err(RsmError::Unsupported)
    }

    /// [`crate::rsm::facade::Rsm::kafka_read`]: one read, then — when it found
    /// fewer than `min_bytes` and there is time left — wait for a push and read
    /// again, exactly as `POST /api/v1/fetch` long-polls.
    pub(super) async fn kafka_read_impl(
        &self,
        ctx: ReqCtx,
        asks: Vec<KafkaReadReq>,
        max_wait_ms: u64,
        min_bytes: usize,
    ) -> Result<Vec<KafkaReadOut>, RsmError> {
        let max_wait = max_wait_ms.min(MAX_WAIT_MS);
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(max_wait);
        // One topic — every consumer of one subscription — parks on that
        // queue's gate, which apply wakes on each of its appends; a fetch
        // spanning topics parks on the tenant's, which every append wakes.
        let one_queue = asks
            .first()
            .map(|a| a.queue.as_str())
            .filter(|q| asks.iter().all(|a| a.queue == *q))
            .map(|q| crate::handlers::tenant_queue_key(&ctx.tenant, q));
        loop {
            let out = self.kafka_read_once(&ctx, &asks).await?;
            let bytes: usize = out
                .iter()
                .flat_map(|o| o.chunks.iter())
                .map(|c| match c {
                    KafkaChunk::Batches(b) => b.len(),
                    KafkaChunk::Messages(m) => m.iter().map(|(_, _, p)| p.len().max(1)).sum(),
                })
                .sum();
            let error = out.iter().any(|o| o.error.is_some());
            if max_wait == 0
                || min_bytes == 0
                || error
                || bytes >= min_bytes
                || std::time::Instant::now() >= deadline
            {
                return Ok(out);
            }
            let wait = deadline
                .saturating_duration_since(std::time::Instant::now())
                .min(std::time::Duration::from_millis(200));
            match &one_queue {
                Some(qkey) => self.notifier.wait_queue(qkey, wait).await,
                None => self.notifier.wait_any(&ctx.tenant, wait).await,
            };
        }
    }

    async fn kafka_read_once(
        &self,
        ctx: &ReqCtx,
        asks: &[KafkaReadReq],
    ) -> Result<Vec<KafkaReadOut>, RsmError> {
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let wanted: Vec<(String, String)> = asks
            .iter()
            .map(|a| (a.queue.clone(), a.partition.clone()))
            .collect();
        let parts = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut out = Vec::with_capacity(wanted.len());
                for (queue, partition) in &wanted {
                    let queue_exists = r.queue(&tenant, queue)?.is_some();
                    let part = match r.pid_of(&tenant, queue, partition)? {
                        Some(pid) => {
                            let mut sealed = Vec::new();
                            r.scan_partition_files(pid, usize::MAX, &mut |f| {
                                sealed.push(f);
                                true
                            })?;
                            r.partition(pid)?.map(|row| ReadPart { pid, row, sealed })
                        }
                        None => None,
                    };
                    out.push((queue_exists, part));
                }
                Ok(out)
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("kafka read: {e}")))?
        .map_err(read_error)?;

        let qlog = self.qlog_reader.clone();
        let reader = self.reader.clone();
        let encryption = self.encryption.clone();
        let tenant = ctx.tenant.clone();
        let asks: Vec<KafkaReadReq> = asks.to_vec();
        tokio::task::spawn_blocking(move || -> Result<Vec<KafkaReadOut>, RsmError> {
            let mut out = Vec::with_capacity(asks.len());
            for (ask, (queue_exists, part)) in asks.iter().zip(parts) {
                let Some(part) = part else {
                    out.push(KafkaReadOut {
                        error: (!queue_exists).then_some("UNKNOWN_TOPIC_OR_PARTITION"),
                        ..KafkaReadOut::default()
                    });
                    continue;
                };
                let high = (part.row.last_offset + 1).max(0);
                let log_start = part.row.log_start as i64;
                if ask.offset < log_start || ask.offset > high {
                    out.push(KafkaReadOut {
                        high_watermark: high,
                        log_start,
                        error: Some("OFFSET_OUT_OF_RANGE"),
                        chunks: Vec::new(),
                    });
                    continue;
                }
                let chunks = read_chunks(
                    &reader,
                    qlog.as_ref(),
                    &encryption,
                    &tenant,
                    &part,
                    ask.offset as u64,
                    high as u64,
                    ask.max_bytes.max(1),
                )?;
                out.push(KafkaReadOut {
                    high_watermark: high,
                    log_start,
                    error: None,
                    chunks,
                });
            }
            Ok(out)
        })
        .await
        .map_err(|e| RsmError::Internal(format!("kafka read task: {e}")))?
    }
}

/// Walk one partition's log from `from` to `high`, until `max_bytes` (at least
/// one chunk): stored Kafka appends as bytes, Queen-pushed frames as messages.
#[allow(clippy::too_many_arguments)]
fn read_chunks(
    reader: &crate::rsm::segments::Reader,
    qlog: Option<&QLogReader>,
    encryption: &crate::encryption::Encryption,
    tenant: &str,
    part: &ReadPart,
    from: u64,
    high: u64,
    max_bytes: usize,
) -> Result<Vec<KafkaChunk>, RsmError> {
    let qid = qlog.map(|_| QLogReader::queue_id_of(tenant, &part.row.queue));
    let mut out: Vec<KafkaChunk> = Vec::new();
    let mut used = 0usize;
    let mut messages = 0usize;
    let mut off = from;
    while off < high && (out.is_empty() || used < max_bytes) {
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
            // A gap (a rolled-back transaction, retention passing): step over.
            off += 1;
            continue;
        };
        if let Some(batches) = kafka_batch::batches(&blob) {
            // From the batch that holds `off`: a client skips records below
            // its position, but there is no reason to send it a whole batch of
            // them.
            let heads = kafka_batch::scan(batches)
                .map_err(|e| RsmError::Internal(format!("stored Kafka batch at {base}: {e}")))?;
            let mut first = base;
            let mut start = 0usize;
            for h in &heads {
                if first + h.records as u64 > off {
                    start = h.start;
                    break;
                }
                first += h.records as u64;
            }
            let bytes = bytes::Bytes::copy_from_slice(&batches[start..]);
            used += bytes.len();
            out.push(KafkaChunk::Batches(bytes));
        } else if let Some(frames) = crate::frames::unpack_frames_ref(&blob) {
            let mut run: Vec<(u64, i64, Vec<u8>)> = Vec::new();
            for (i, f) in frames.into_iter().enumerate() {
                let pos = base + i as u64;
                if pos < off || pos >= high {
                    continue;
                }
                let payload = if f.encrypted {
                    encryption
                        .decrypt_payload_bytes(f.payload)
                        .unwrap_or_else(|| f.payload.to_vec())
                } else {
                    f.payload.to_vec()
                };
                used += payload.len().max(1);
                messages += 1;
                run.push((pos, created, payload));
                // Pushed AFTER the check that would stop it, so the first
                // message of the answer always goes, whatever its size.
                if used >= max_bytes || messages >= MAX_MESSAGES {
                    break;
                }
            }
            if !run.is_empty() {
                // Adjacent Queen-pushed segments merge into one run, so the
                // handler encodes them as one batch.
                match out.last_mut() {
                    Some(KafkaChunk::Messages(prev)) => prev.extend(run),
                    _ => out.push(KafkaChunk::Messages(run)),
                }
            }
            if messages >= MAX_MESSAGES {
                break;
            }
        }
        off = base.saturating_add(count as u64).max(off + 1);
    }
    Ok(out)
}
