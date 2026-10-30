//! Read-only Phase-2 HTTP surface. Reads are taken from one LMDB transaction;
//! payloads are then read from this node's committed queue/segment log.

use std::collections::{BTreeSet, HashMap};

use serde::Deserialize;
use serde_json::{json, Value};

use super::json_out::{push_object, Field};
use super::{query_map, read_error, ApiOut, ApiReq, Effect, RaftFacade, ReqCtx, RsmError};
use crate::frames::{unpack_frames_ref, uuid_bytes_to_string, uuid_string_to_bytes};
use crate::rsm::effect::{Pid, TraceEvent};
use crate::rsm::facade::{ChangedAnswer, ChangedAsk, ChangedPartition, PartitionsChanged};
use crate::rsm::planner::bucket_of;
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::store::keys;
use crate::rsm::store::rows::{self, DlqRow, PartitionRow};
use crate::rsm::store::{Keyspace, Reads, Store, TypedReads};

mod kafka_groups;
mod messages;

pub(super) use kafka_groups::{is_kafka_topic, kafka_topics};

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

/// The newest message stamp a partition can hold: no message in it is newer.
fn newest_stamp(p: &PartitionRow) -> i64 {
    p.last_created_at_us.max(p.last_write_at_us)
}

/// The most partition rows a lookup by partition uuid reads
/// ([`RaftFacade::resolve_pid`]).
const UUID_SCAN_MAX_ROWS: usize = 2_000_000;

/// Offsets read per backward step of the message list.
const MESSAGE_CHUNK: u64 = 256;

/// One partition the message list may read, with what a status needs.
struct MsgPart {
    part: Part,
    cursors: Vec<(String, crate::rsm::effect::CursorRow)>,
    /// Offsets with a DLQ row (any group).
    dlq: std::collections::HashSet<i64>,
    namespace: Option<String>,
    task: Option<String>,
    priority: i32,
}

/// A message the list keeps, with its status.
struct Picked<R = Record> {
    created_at_us: i64,
    offset: u64,
    cand: usize,
    status: &'static str,
    consumed_by: u64,
    total_groups: u64,
    record: R,
}

/// A message's status as `list_messages_v1` (010 ≈860) derives it:
/// `dead_letter` when a DLQ row holds the offset; `completed` when every bus
/// group's cursor passed it, or — with no bus group — the queue-mode cursor
/// did; `processing` while any cursor of the partition holds a live lease;
/// otherwise `pending`. Also `(consumedBy, totalGroups)` over the bus groups.
pub(super) fn message_status(
    offset: u64,
    cursors: &[(String, crate::rsm::effect::CursorRow)],
    dlq: &std::collections::HashSet<i64>,
    now: i64,
) -> (&'static str, u64, u64) {
    let off = offset as i64;
    let (mut bus, mut passed, mut queue_mode_passed, mut live) = (0u64, 0u64, false, false);
    for (g, c) in cursors {
        if g == "__QUEUE_MODE__" {
            queue_mode_passed |= off <= c.committed;
        } else {
            bus += 1;
            if off <= c.committed {
                passed += 1;
            }
        }
        live |= c.lease_expires_at_us.is_some_and(|e| e > now);
    }
    let status = if dlq.contains(&off) {
        "dead_letter"
    } else if (bus > 0 && passed == bus) || (bus == 0 && queue_mode_passed) {
        "completed"
    } else if live {
        "processing"
    } else {
        "pending"
    };
    (status, passed, bus)
}

#[derive(Deserialize)]
struct FetchBody {
    #[serde(default)]
    entries: Vec<FetchEntry>,
    #[serde(rename = "maxWaitMs", default)]
    max_wait_ms: Option<u64>,
    #[serde(rename = "minBytes", default)]
    min_bytes: Option<i64>,
    /// The ceiling of the whole call, beside each entry's own `maxBytes`.
    #[serde(rename = "maxBytes", default)]
    max_bytes: Option<i64>,
}

/// The partition NAME a fetch entry addresses: a string, a number spelled in
/// decimal, or `Default`.
pub(super) fn fetch_partition_name(p: Option<&Value>) -> String {
    match p {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        _ => "Default".to_string(),
    }
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

/// Percent-decode one path segment (`%3A` → `:`, `%2F` → `/`) the way axum's
/// `Path` decodes the segments of the routes the router names: a `%` decodes
/// only before two hex digits and is otherwise kept as it stands, and `+`
/// stays `+` (a space is `+` only in a query string).
pub(super) fn pct_decode(segment: &str) -> String {
    if !segment.contains('%') {
        return segment.to_string();
    }
    let hex = |c: u8| (c as char).to_digit(16);
    let b = segment.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(hi), Some(lo)) = (hex(b[i + 1]), hex(b[i + 2])) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

impl RaftFacade {
    /// Every route with a name in its path: a queue, partition, consumer
    /// group, transaction id, trace name or DLQ id. The path is split at its
    /// slashes and only then is each segment percent-decoded, here and nowhere
    /// else: every SDK encodes the names it writes into a path
    /// (`encodeURIComponent`, `rawurlencode`, `url.PathEscape`), so a name is
    /// what its segment decodes to — the name a push carried in its body, the
    /// one axum's `Path` hands the pop — and an encoded `/` stays inside it.
    pub(super) async fn api_dynamic(
        &self,
        ctx: &ReqCtx,
        req: &ApiReq,
    ) -> Result<Option<ApiOut>, RsmError> {
        let raw: Vec<&str> = req.path.trim_matches('/').split('/').collect();
        let names: Vec<String> = raw.iter().map(|s| pct_decode(s)).collect();
        let p: Vec<&str> = names.iter().map(String::as_str).collect();
        let out = match p.as_slice() {
            ["api", "v1", "resources", "queues", queue] if req.method == "GET" => {
                Some(self.api_get_queue(ctx.clone(), queue).await?)
            }
            ["api", "v1", "resources", "queues", queue] if req.method == "DELETE" => {
                Some(self.api_delete_queue(ctx.clone(), queue).await?)
            }
            ["api", "v1", "resources", "queues", queue, "depth"] if req.method == "GET" => Some(
                self.api_queue_depth(ctx.clone(), queue, req.query.as_deref())
                    .await?,
            ),
            ["api", "v1", "resources", "queues", queue, "sizes"] if req.method == "GET" => {
                Some(self.api_queue_sizes(ctx.clone(), queue).await?)
            }
            ["api", "v1", "status", "queues", queue] if req.method == "GET" => Some(
                self.api_status_queue(ctx.clone(), queue, req.query.as_deref())
                    .await?,
            ),
            // A transaction id is the client's string: one like
            // `metric-sample|<uuid>` arrives as `metric-sample%7C<uuid>`.
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
            // Reached only from `handlers/ephemeral.rs`, which re-dispatches
            // its DELETE here with the name axum already decoded, written into
            // the path as it stands (`handlers::raft::apply_local_control`
            // reads it back the same way): this segment is the name itself.
            ["api", "v1", "ephemeral", "queue", _] if req.method == "DELETE" => {
                Some(self.api_ephemeral_delete(ctx.clone(), raw[4]).await?)
            }
            _ => None,
        };
        Ok(out)
    }

    pub(super) async fn api_fetch(&self, ctx: ReqCtx, body: &[u8]) -> Result<ApiOut, RsmError> {
        let request: FetchBody =
            serde_json::from_slice(body).map_err(|e| super::rejected("bad_body", e))?;
        if request.entries.len() > 1024 {
            return Ok(ApiOut::json(
                400,
                json!({"error":"too many entries"}).to_string(),
            ));
        }
        if request.entries.iter().any(|e| e.offset < 0) {
            return Ok(ApiOut::json(
                400,
                json!({"error":"offset must be non-negative"}).to_string(),
            ));
        }
        let max_wait = request.max_wait_ms.unwrap_or(0).min(30_000);
        let min_bytes = request.min_bytes.unwrap_or(1).max(0) as usize;
        // The call's own ceiling ([`Self::fetch_once`]); without one, the
        // default total (`QUEEN_FETCH_MAX_BYTES`, 50 MiB, Kafka's
        // `fetch.max.bytes` ballpark): 1,024 entries of 8 MiB each were 8 GiB
        // in one answer.
        let ctx = ReqCtx {
            fetch_max_bytes: Some(
                request
                    .max_bytes
                    .map(|b| b.max(1) as usize)
                    .unwrap_or_else(fetch_default_max_bytes),
            ),
            ..ctx
        };
        // The same (queue, partition, offset) asked twice is read once: the
        // first entry stays, with the larger of the two per-entry ceilings.
        let mut asks: Vec<crate::rsm::facade::RecordFetch> = Vec::with_capacity(request.entries.len());
        let mut seen: HashMap<(String, String, u64), usize> = HashMap::new();
        for e in request.entries {
            let ask = crate::rsm::facade::RecordFetch {
                partition: fetch_partition_name(e.partition.as_ref()),
                queue: e.queue,
                offset: e.offset as u64,
                max_bytes: e.max_bytes.unwrap_or(1 << 20).clamp(1, 8 << 20) as usize,
            };
            let key = (ask.queue.clone(), ask.partition.clone(), ask.offset);
            match seen.get(&key) {
                Some(&i) => asks[i].max_bytes = asks[i].max_bytes.max(ask.max_bytes),
                None => {
                    seen.insert(key, asks.len());
                    asks.push(ask);
                }
            }
        }
        let answers = self
            .fetch_wait(&ctx, &asks, max_wait, min_bytes, true)
            .await?;
        // Each payload written as it is stored (json_out): a tree of an
        // 8 MiB answer took about 19 times that.
        Ok(ApiOut::json(
            200,
            super::json_out::fetch_answer(&asks, &answers),
        ))
    }

    /// [`crate::rsm::facade::Rsm::fetch_records`]: the read of `POST
    /// /api/v1/fetch`, answered as records. The entries are already bounded
    /// (`partition`, a non-negative offset, a clamped `max_bytes`); the count
    /// ceiling of the route applies the same.
    ///
    /// Nothing is counted here: this is the Kafka facade's read, and the facade
    /// trims it to its client's budget and reads the rest again on the next
    /// poll. Its reads are counted where its answer is final
    /// (`queen_kafka::queen::LocalDispatch::note_fetched`, crate::kafka_inproc).
    pub(in crate::rsm::facade::real) async fn fetch_records_impl(
        &self,
        ctx: ReqCtx,
        entries: Vec<crate::rsm::facade::RecordFetch>,
        max_wait_ms: u64,
        min_bytes: usize,
    ) -> Result<Vec<crate::rsm::facade::RecordsFetched>, RsmError> {
        self.fetch_typed(ctx, entries, max_wait_ms, min_bytes, false)
            .await
    }

    /// [`crate::rsm::facade::Rsm::fetch_log`]: [`Self::fetch_records_impl`]
    /// with each record's transaction id, read as the route reads it.
    pub(in crate::rsm::facade::real) async fn fetch_log_impl(
        &self,
        ctx: ReqCtx,
        entries: Vec<crate::rsm::facade::RecordFetch>,
        max_wait_ms: u64,
        min_bytes: usize,
    ) -> Result<Vec<crate::rsm::facade::RecordsFetched>, RsmError> {
        self.fetch_typed(ctx, entries, max_wait_ms, min_bytes, true)
            .await
    }

    /// The typed fetches: the route's count ceiling and wait cap, then the
    /// route's own [`Self::fetch_wait`].
    async fn fetch_typed(
        &self,
        ctx: ReqCtx,
        entries: Vec<crate::rsm::facade::RecordFetch>,
        max_wait_ms: u64,
        min_bytes: usize,
        want_txn: bool,
    ) -> Result<Vec<crate::rsm::facade::RecordsFetched>, RsmError> {
        if entries.len() > 1024 {
            return Err(RsmError::Rejected {
                code: "bad_body".into(),
                message: "too many entries".into(),
            });
        }
        self.fetch_wait(&ctx, &entries, max_wait_ms.min(30_000), min_bytes, want_txn)
            .await
    }

    /// The long poll of a fetch: read, and while the answer carries fewer than
    /// `min_bytes` payload bytes and no entry errored, park on the partitions
    /// read until an append to one of them or `max_wait` (re-reading at least
    /// every second, the floor for a missed wake). The bytes are counted as the
    /// records are read, never by re-reading the answer.
    ///
    /// A parked fetch that has read nothing also answers, empty, once it has
    /// waited [`topic_wake_floor`] and its topic was written on this node
    /// meanwhile.
    /// A Kafka client keeps ONE fetch in flight per broker, and leaves out of
    /// it every partition it still holds records of: a partition it drains
    /// while this fetch is parked on quiet ones waits for this fetch to come
    /// back, up to the client's `fetch.max.wait.ms` (500 ms in Java). The early
    /// answer lets it ask again with every partition it can take. Measured
    /// 2026-10-17 with the OpenMessaging Benchmark's stock Java consumer (1 KB,
    /// 100 partitions, 500,000 msg/s, three GCP nodes): e2e p50/p99/p99.9
    /// 46/625/792 ms with fetches held to the client's 500 ms, 11/39/56 ms with
    /// the client's wait set to 10 ms. An idle topic still parks to
    /// `max_wait`, and the floor bounds the extra round trips of a client
    /// whose partitions are quiet on a busy topic.
    async fn fetch_wait(
        &self,
        ctx: &ReqCtx,
        asks: &[crate::rsm::facade::RecordFetch],
        max_wait: u64,
        min_bytes: usize,
        want_txn: bool,
    ) -> Result<Vec<crate::rsm::facade::RecordsFetched>, RsmError> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(max_wait);
        // A long-poll watches exactly the partitions it reads, registered
        // BEFORE the first read: apply wakes it on an append to one of them,
        // whether or not a consumer group subscribes the queue, and an append
        // landing between a read and the park is a permit rather than a lost
        // wake.
        let watch = (max_wait > 0 && min_bytes > 0).then(|| {
            let parts: Vec<(String, String)> = asks
                .iter()
                .map(|e| {
                    (
                        crate::handlers::tenant_queue_key(&ctx.tenant, &e.queue),
                        e.partition.clone(),
                    )
                })
                .collect();
            self.notifier.watch_partitions(&parts)
        });
        let floor = topic_wake_floor();
        let topic = (watch.is_some() && !floor.is_zero()).then(|| {
            let mut qkeys: Vec<String> = asks
                .iter()
                .map(|e| crate::handlers::tenant_queue_key(&ctx.tenant, &e.queue))
                .collect();
            qkeys.sort_unstable();
            qkeys.dedup();
            self.notifier.watch_topics(&qkeys)
        });
        let early = std::time::Instant::now() + floor;
        // The topic was written on this node since the fetch arrived.
        let mut moved = false;
        loop {
            let (answers, bytes) = self.fetch_once(ctx, asks, want_txn).await?;
            if max_wait == 0 || min_bytes == 0 {
                return Ok(answers);
            }
            let error = answers.iter().any(|a| a.error.is_some());
            let now = std::time::Instant::now();
            // The early answer is an EMPTY one: a fetch holding some bytes keeps
            // waiting for its `min_bytes`, which is how a client asks for batches.
            if error
                || bytes >= min_bytes
                || now >= deadline
                || (moved && bytes == 0 && now >= early)
            {
                return Ok(answers);
            }
            // Capped at a second: the wake is the fast path, and the re-read on
            // the cap is the floor if one is ever missed.
            let wait = deadline
                .saturating_duration_since(now)
                .min(std::time::Duration::from_secs(1));
            match (&watch, &topic) {
                (Some(w), Some(t)) if !moved => {
                    tokio::select! {
                        _ = w.wait(wait) => {}
                        woke = t.wait(wait) => {
                            moved = woke;
                            // Not before the floor; an append to one of this
                            // fetch's own partitions still ends the wait.
                            let left = early.saturating_duration_since(std::time::Instant::now());
                            if moved && !left.is_zero() {
                                w.wait(left.min(wait)).await;
                            }
                        }
                    }
                }
                (Some(w), _) => {
                    w.wait(wait).await;
                }
                (None, _) => {}
            }
        }
    }

    /// One read of every entry, in ONE blocking task: the partition rows from
    /// one store transaction, then each entry's records from the queue log.
    /// Answers the entries in order, and the payload bytes they carry (each
    /// record counted as at least one byte), which is what `min_bytes` is
    /// measured in.
    ///
    /// A call may carry a ceiling of its own ([`ReqCtx::fetch_max_bytes`]),
    /// counted in the same bytes. The entries are then read in order while
    /// any of it is left, each for no more than what is left, and an entry
    /// reached with none left answers its bounds and no records: the caller
    /// sees where that partition stands and reads it on its next call.
    /// Without it a call read every entry to its own budget, up to 1024 times
    /// 8 MiB, however little of that its caller could use (a Kafka consumer
    /// behind on a thousand partitions is answered 32 MiB of a gigabyte). The
    /// first record of an entry is read whatever it weighs, so a call goes
    /// over its ceiling by one record at most and never stalls on a large one.
    async fn fetch_once(
        &self,
        ctx: &ReqCtx,
        asks: &[crate::rsm::facade::RecordFetch],
        want_txn: bool,
    ) -> Result<(Vec<crate::rsm::facade::RecordsFetched>, usize), RsmError> {
        use crate::rsm::facade::{RecordRead, RecordsFetched};
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let qlog = self.qlog_reader.clone();
        let reader = self.reader.clone();
        let encryption = self.encryption.clone();
        let max_total = ctx.fetch_max_bytes.map(|total| total.max(1));
        let asks: Vec<(String, String, u64, usize)> = asks
            .iter()
            .map(|a| (a.queue.clone(), a.partition.clone(), a.offset, a.max_bytes))
            .collect();
        tokio::task::spawn_blocking(move || -> Result<_, RsmError> {
            // The sealed segment files are the segment reader's index; the
            // queue log finds a record by itself, so a qlog node lists none.
            let list_sealed = qlog.is_none();
            let parts = store
                .read(|r| {
                    let mut out = Vec::with_capacity(asks.len());
                    for (queue, part, _, _) in &asks {
                        let p = match r.pid_of(&tenant, queue, part)? {
                            Some(pid) => {
                                let row = r.partition(pid)?;
                                let mut sealed = Vec::new();
                                if list_sealed {
                                    r.scan_partition_files(pid, usize::MAX, &mut |f| {
                                        sealed.push(f);
                                        true
                                    })?;
                                }
                                row.map(|row| Part { pid, row, sealed })
                            }
                            None => None,
                        };
                        // The queue row is read only to tell an unknown queue
                        // from a partition nobody has written yet.
                        let queue_exists = p.is_some() || r.queue(&tenant, queue)?.is_some();
                        out.push((queue_exists, p));
                    }
                    Ok(out)
                })
                .map_err(read_error)?;
            let mut out = Vec::with_capacity(asks.len());
            let mut bytes = 0usize;
            for ((queue, _partition, offset, max_bytes), (queue_exists, part)) in
                asks.into_iter().zip(parts)
            {
                let Some(part) = part else {
                    out.push(RecordsFetched {
                        error: (!queue_exists).then_some("UNKNOWN_TOPIC_OR_PARTITION"),
                        ..RecordsFetched::default()
                    });
                    continue;
                };
                let high = (part.row.last_offset + 1).max(0) as u64;
                let log_start = part.row.log_start;
                if offset < log_start || offset > high {
                    out.push(RecordsFetched {
                        records: Vec::new(),
                        high_watermark: high,
                        log_start_offset: log_start,
                        error: Some("OFFSET_OUT_OF_RANGE"),
                    });
                    continue;
                }
                // What is left of the call's ceiling is this entry's, when it
                // is less than its own. The ceiling is at least a byte, so
                // none left means something was read.
                let max_bytes = match max_total {
                    Some(total) if bytes >= total => {
                        out.push(RecordsFetched {
                            records: Vec::new(),
                            high_watermark: high,
                            log_start_offset: log_start,
                            error: None,
                        });
                        continue;
                    }
                    Some(total) => max_bytes.min(total - bytes),
                    None => max_bytes,
                };
                let mut records: Vec<RecordRead> = Vec::new();
                let mut used = 0usize;
                walk_payloads(
                    &reader,
                    qlog.as_ref(),
                    &tenant,
                    &queue,
                    &part,
                    offset,
                    high,
                    FETCH_RECORDS_PER_ENTRY,
                    want_txn,
                    |offset, created_at_us, txn, payload, encrypted| {
                        let payload = if encrypted {
                            match encryption.decrypt_payload_bytes(&payload) {
                                Some(p) => bytes::Bytes::from(p),
                                None => payload,
                            }
                        } else {
                            payload
                        };
                        let weight = payload.len().max(1);
                        if !records.is_empty() && used.saturating_add(weight) > max_bytes {
                            return false;
                        }
                        used = used.saturating_add(weight);
                        records.push(RecordRead {
                            offset,
                            created_at_us,
                            payload,
                            txn,
                        });
                        true
                    },
                )?;
                bytes = bytes.saturating_add(used);
                out.push(RecordsFetched {
                    records,
                    high_watermark: high,
                    log_start_offset: log_start,
                    error: None,
                });
            }
            Ok((out, bytes))
        })
        .await
        .map_err(|e| RsmError::Internal(format!("fetch read task: {e}")))?
    }

    /// `POST /api/v1/partitions/changed`: the body read into [`ChangedAsk`]s,
    /// the read [`crate::rsm::facade::Rsm::partitions_changed`] makes
    /// ([`Self::changed_read`]), the answer rendered. The whole request is
    /// refused (400) above [`CHANGED_MAX_ENTRIES`] entries and for a `since`
    /// that is not a timestamp — the 1.5.0 handler refused a malformed one
    /// too, and reading it as "no since" would turn a caller's incremental
    /// pass into a full listing without a word. Everything else is answered
    /// per entry.
    pub(super) async fn api_partitions_changed(
        &self,
        ctx: ReqCtx,
        body: &[u8],
    ) -> Result<ApiOut, RsmError> {
        use crate::rsm::planner::timers::iso_us;
        let root: Value =
            serde_json::from_slice(body).map_err(|e| super::rejected("bad_body", e))?;
        let entries = root
            .get("entries")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if entries.len() > CHANGED_MAX_ENTRIES {
            return Ok(ApiOut::json(
                400,
                json!({"error":"too many entries"}).to_string(),
            ));
        }
        let mut asks = Vec::with_capacity(entries.len());
        for (i, e) in entries.iter().enumerate() {
            let since_us = match e.get("since") {
                None | Some(Value::Null) => None,
                Some(v) => match v
                    .as_str()
                    .and_then(crate::rsm::dashboard::model::parse_ts_us)
                {
                    Some(us) => Some(us),
                    None => {
                        return Ok(ApiOut::json(
                            400,
                            json!({
                                "error": format!(
                                    "bad body: entry {i}: since is not a timestamp: {v}"
                                )
                            })
                            .to_string(),
                        ))
                    }
                },
            };
            asks.push(ChangedAsk {
                queue: e
                    .get("queue")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                since_us,
                after: match e.get("after") {
                    None | Some(Value::Null) => None,
                    Some(Value::String(s)) => Some(s.clone()),
                    // Never a cursor this broker issued: BAD_CURSOR.
                    Some(other) => Some(other.to_string()),
                },
                limit: e
                    .get("limit")
                    .and_then(Value::as_i64)
                    .map_or(CHANGED_MAX_LIMIT, |l| {
                        l.clamp(1, CHANGED_MAX_LIMIT as i64) as usize
                    }),
            });
        }
        let queues: Vec<String> = asks.iter().map(|a| a.queue.clone()).collect();
        let answer = self.changed_read(&ctx, asks).await?;
        let entries: Vec<Value> = queues
            .into_iter()
            .zip(answer.entries)
            .map(|(queue, a)| match a.error {
                Some(error) => json!({"queue": queue, "error": error}),
                None => json!({
                    "queue": queue,
                    "partitions": a.partitions.into_iter().map(|p| json!({
                        "name": p.name,
                        "id": p.id,
                        "lastOffset": p.last_offset,
                        "logStart": p.log_start,
                        "lastWriteAt": iso_us(p.last_write_at_us),
                    })).collect::<Vec<_>>(),
                    "next": a.next,
                }),
            })
            .collect();
        Ok(ApiOut::json(
            200,
            json!({
                "safeTime": iso_us(answer.safe_time_us),
                "safeTimeDegraded": false,
                "entries": entries,
            })
            .to_string(),
        ))
    }

    /// [`crate::rsm::facade::Rsm::partitions_changed`]: the route's count
    /// ceiling, then the route's own read.
    pub(in crate::rsm::facade::real) async fn partitions_changed_impl(
        &self,
        ctx: ReqCtx,
        asks: Vec<ChangedAsk>,
    ) -> Result<PartitionsChanged, RsmError> {
        if asks.len() > CHANGED_MAX_ENTRIES {
            return Err(RsmError::Rejected {
                code: "bad_body".into(),
                message: "too many entries".into(),
            });
        }
        self.changed_read(&ctx, asks).await
    }

    /// The read of a discovery call, the route's and the typed one alike: ONE
    /// store read in one blocking task, `safeTime` first, then one page per
    /// ask ([`changed_page`]).
    async fn changed_read(
        &self,
        ctx: &ReqCtx,
        asks: Vec<ChangedAsk>,
    ) -> Result<PartitionsChanged, RsmError> {
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        tokio::task::spawn_blocking(move || {
            store.read(|r| {
                // safeTime: the greatest stamp this node has APPLIED, read
                // FIRST — before any queue or partition row. Every record
                // this node has not applied yet is stamped strictly above it,
                // so every record stamped at or below it is in the rows read
                // below and readable by a fetch here: a reader may close a
                // time window at it and lose nothing. (The wall clock at the
                // answer, which this replaced, is not that: a record is
                // stamped when it is PLANNED and visible once it is APPLIED,
                // so a planned, older-stamped record could still be on its
                // way.) Each step, and where it is enforced:
                //
                // 1. The applied clock only moves forward along the log:
                //    apply refuses an entry whose `now_us` is below the last
                //    applied one (`Applier::gates`, TimeWentBackwards, I5,
                //    rsm/apply.rs), and `meta.max_created_at_us` only rises
                //    (the Append arm of rsm/apply_shard.rs).
                // 2. A record is stamped at or above its entry's clock: a
                //    push stamps `created_at = max(now_us, the partition's
                //    last created_at + 1)` (`Planner::plan_push_known`,
                //    rsm/planner/push.rs), the one place an Append is built.
                // 3. An entry's clock is above every stamp before it in the
                //    log: a cycle is stamped with `Committed::plan_now` =
                //    max(wall, last applied now_us + 1, max_created_at + 1)
                //    (rsm/state/mod.rs), lifted above every entry still in
                //    flight — `Overlay::plan_now` (rsm/planner/mod.rs), and
                //    with lanes "L2: one clock for the cycle" over every
                //    in-flight `now_us` and `created_hi`
                //    (rsm/batcher_lanes.rs) — and a new leader plans only
                //    after it applied the first entry of its own term, hence
                //    every entry of the terms before (I13, `RunState::on_role`,
                //    rsm/batcher.rs).
                //    So by 2 and 3 every record of an entry not applied here
                //    is stamped strictly above both values read now.
                // 4. What is read below is at least as new as these two
                //    values: every keyspace is RAM, read LIVE by every handle
                //    under the tables' locks (`Keyspace::is_ram`, the "Phase
                //    C" section of rsm/store/mod.rs), and apply writes these
                //    meta rows at the end of each entry — after its effects,
                //    after its payload bytes became readable (the segment
                //    and qlog flushes), just before the applied index
                //    (`Applier::execute`, rsm/apply.rs). A partition row read
                //    after them holds every record of every entry they
                //    reflect.
                //
                // A follower is the same argument at its own applied index.
                // The value is the serving node's: pair it with reads served
                // by the same node (pages from several nodes: their minimum).
                // It moves only when an entry applies, so an idle node answers
                // the same safeTime until the next write. Never degraded.
                let safe_time_us = r.last_now_us()?.max(r.max_created_at_us()?);
                let entries = asks
                    .iter()
                    .map(|ask| changed_page(r, &tenant, ask))
                    .collect::<crate::rsm::store::Result<Vec<_>>>()?;
                Ok(PartitionsChanged {
                    safe_time_us,
                    entries,
                })
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("partitions changed: {e}")))?
        .map_err(read_error)
    }

    /// `GET /api/v1/messages`: the messages created in `[from, to)` (default
    /// the last hour; `to` rounds up
    /// to the next minute), newest first, each with its status against the
    /// partition's cursors ([`message_status`]), filtered by queue, partition,
    /// namespace, task and status, then paged by `limit`/`offset`.
    ///
    /// The queue-log path seeks the time window in its indexes, selects the
    /// page from metadata and only then reads its payloads. The legacy segment
    /// path walks backwards in chunks; it may scan a suffix newer than `to`.
    pub(super) async fn api_messages(
        &self,
        ctx: ReqCtx,
        query: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        use crate::rsm::dashboard::model::{parse_ts_us, trunc_us, US_PER_MIN};
        let q = query_map(query);
        let get = |k: &str| q.get(k).filter(|s| !s.is_empty()).cloned();
        let queue = get("queue");
        let partition = get("partition");
        let namespace = get("namespace");
        let task = get("task");
        let status = get("status");
        let limit = get("limit")
            .and_then(|s| s.parse().ok())
            .unwrap_or(200usize)
            .clamp(1, 1000);
        let offset = get("offset").and_then(|s| s.parse().ok()).unwrap_or(0usize);
        // Every page ranks `offset + limit` messages before it skips
        // `offset`: deep pages cost as much as reading all of them.
        if offset > messages_max_offset() {
            return Ok(ApiOut::json(
                400,
                json!({
                    "error": format!(
                        "offset {offset} is past the ceiling {} (QUEEN_MESSAGES_MAX_OFFSET): narrow the window with from/to",
                        messages_max_offset()
                    ),
                    "code": "bad_request",
                })
                .to_string(),
            ));
        }
        let now = super::super::wall_micros();
        let from_us = get("from")
            .and_then(|s| parse_ts_us(&s))
            .unwrap_or(now - 3_600_000_000);
        let to_us = trunc_us(
            get("to").and_then(|s| parse_ts_us(&s)).unwrap_or(now),
            US_PER_MIN,
        ) + US_PER_MIN;

        let need_sealed = self.qlog_reader.is_none();
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let (mode, mut cands) = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut queues = Vec::new();
                match &queue {
                    Some(q) => queues.push(q.clone()),
                    None => {
                        r.scan_queues(&tenant, usize::MAX, &mut |q, _| {
                            queues.push(q.to_string());
                            true
                        })?;
                    }
                }
                let mut has_queue_mode = false;
                let mut bus_groups = BTreeSet::new();
                let mut cands = Vec::new();
                for qn in queues {
                    let cfg = r.queue(&tenant, &qn)?;
                    let ns = cfg.as_ref().and_then(|c| c.namespace.clone());
                    let tk = cfg.as_ref().and_then(|c| c.task.clone());
                    // namespace/task narrow the listing only: `mode` is
                    // computed over the queue/partition filter alone.
                    let listed = namespace.as_ref().is_none_or(|n| ns.as_ref() == Some(n))
                        && task.as_ref().is_none_or(|t| tk.as_ref() == Some(t));
                    let mut pids = Vec::new();
                    r.scan_queue_partitions(&tenant, &qn, None, usize::MAX, &mut |pid| {
                        pids.push(pid);
                        true
                    })?;
                    for pid in pids {
                        let Some(row) = r.partition(pid)? else {
                            continue;
                        };
                        if partition.as_ref().is_some_and(|x| x != &row.partition) {
                            continue;
                        }
                        let mut cursors = Vec::new();
                        r.scan_cursors(pid, usize::MAX, &mut |g, c| {
                            if g == "__QUEUE_MODE__" {
                                has_queue_mode = true;
                            } else {
                                bus_groups.insert(g.to_string());
                            }
                            cursors.push((g.to_string(), c));
                            true
                        })?;
                        if !listed
                            || row.last_offset < row.log_start as i64
                            || newest_stamp(&row) < from_us
                        {
                            continue;
                        }
                        let mut dlq = std::collections::HashSet::new();
                        let pref = keys::dlq_by_pos_pid_prefix(pid);
                        r.scan_raw(Keyspace::DlqByPos, &pref, &pref, usize::MAX, &mut |k, _| {
                            if let Some(o) = keys::dlq_by_pos_offset_of(k) {
                                dlq.insert(o);
                            }
                            true
                        })?;
                        let mut sealed = Vec::new();
                        if need_sealed {
                            r.scan_partition_files(pid, usize::MAX, &mut |f| {
                                sealed.push(f);
                                true
                            })?;
                        }
                        cands.push(MsgPart {
                            part: Part { pid, row, sealed },
                            cursors,
                            dlq,
                            namespace: ns.clone(),
                            task: tk.clone(),
                            priority: cfg.as_ref().map(|c| c.priority).unwrap_or(0),
                        });
                    }
                }
                let bus = bus_groups.len();
                let ty = match (has_queue_mode, bus > 0) {
                    (false, false) => "none",
                    (true, false) => "queue",
                    (false, true) => "bus",
                    (true, true) => "hybrid",
                };
                Ok((
                    json!({"hasQueueMode":has_queue_mode,"busGroupsCount":bus,"type":ty}),
                    cands,
                ))
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("message list read: {e}")))?
        .map_err(read_error)?;

        cands.sort_by_key(|c| std::cmp::Reverse(newest_stamp(&c.part.row)));
        let need = offset.saturating_add(limit);
        let qlog = self.qlog_reader.clone();
        let reader = self.reader.clone();
        let tenant = ctx.tenant.clone();
        let status_filter = status.clone();
        let (picked, cands) = tokio::task::spawn_blocking(move || {
            if let Some(qlog) = qlog.as_ref() {
                let picked = messages::indexed_page(
                    qlog,
                    &tenant,
                    &cands,
                    &messages::Window {
                        from_us,
                        to_us,
                        now,
                        status: status_filter.as_deref(),
                        offset,
                        limit,
                    },
                )?;
                return Ok((picked, cands));
            }
            // (created_at, offset, candidate, status, consumedBy, totalGroups, record),
            // kept sorted newest first and at most `need` long.
            let mut best: Vec<Picked> = Vec::new();
            for (ci, c) in cands.iter().enumerate() {
                if best.len() >= need
                    && best
                        .last()
                        .is_some_and(|w| newest_stamp(&c.part.row) < w.created_at_us)
                {
                    break; // no message of this or any later partition is newer
                }
                let low = c.part.row.log_start;
                let mut high = (c.part.row.last_offset + 1).max(0) as u64;
                'partition: while high > low {
                    let lo = high.saturating_sub(MESSAGE_CHUNK).max(low);
                    let mut chunk = Vec::new();
                    walk_records(
                        &reader,
                        qlog.as_ref(),
                        &tenant,
                        &c.part,
                        lo,
                        high,
                        usize::MAX,
                        |rec| {
                            chunk.push(rec);
                            true
                        },
                    )?;
                    for rec in chunk.into_iter().rev() {
                        if rec.created_at_us >= to_us {
                            continue;
                        }
                        if rec.created_at_us < from_us {
                            break 'partition;
                        }
                        let (st, consumed_by, total) =
                            message_status(rec.offset, &c.cursors, &c.dlq, now);
                        if status_filter.as_deref().is_some_and(|f| f != st) {
                            continue;
                        }
                        let key = (rec.created_at_us, rec.offset);
                        if best.len() >= need
                            && best
                                .last()
                                .is_some_and(|w| key <= (w.created_at_us, w.offset))
                        {
                            // A partition's stamps only grow with the offset:
                            // nothing older here can make the page either.
                            break 'partition;
                        }
                        let at = best.partition_point(|w| (w.created_at_us, w.offset) > key);
                        best.insert(
                            at,
                            Picked {
                                created_at_us: rec.created_at_us,
                                offset: rec.offset,
                                cand: ci,
                                status: st,
                                consumed_by,
                                total_groups: total,
                                record: rec,
                            },
                        );
                        best.truncate(need);
                    }
                    high = lo;
                }
            }
            Ok::<_, RsmError>((best.into_iter().skip(offset).take(limit).collect(), cands))
        })
        .await
        .map_err(|e| RsmError::Internal(format!("message list walk: {e}")))??;

        // Each row written as the tree printed it, its payload copied in as
        // stored (twice: `data` and `payload`), and the page cut at
        // `QUEEN_READ_PAGE_MAX_BYTES`: a cut page says where the next one
        // starts (`pagination.nextOffset`, `hasMore`).
        let budget = read_page_max_bytes();
        let mut out = String::from("{\"messages\":[");
        let mut total = 0usize;
        let mut cut = false;
        for mut p in picked {
            if total > 0 && out.len() >= budget {
                cut = true;
                break;
            }
            decrypt_record(&self.encryption, &mut p.record);
            let c = &cands[p.cand];
            let lease = c
                .cursors
                .iter()
                .find(|(g, _)| g == "__QUEUE_MODE__")
                .and_then(|(_, cur)| cur.lease_expires_at_us);
            if total > 0 {
                out.push(',');
            }
            let mut fields = message_fields(&p.record);
            fields.extend([
                ("status", Field::Json(json!(p.status))),
                ("queueStatus", Field::Json(json!(p.status))),
                (
                    "busStatus",
                    Field::Json(json!({"consumedBy":p.consumed_by,"totalGroups":p.total_groups})),
                ),
                (
                    "queuePath",
                    Field::Json(json!(format!("{}/{}", c.part.row.queue, c.part.row.partition))),
                ),
                ("namespace", Field::Json(json!(c.namespace))),
                ("task", Field::Json(json!(c.task))),
                ("queuePriority", Field::Json(json!(c.priority))),
                ("payloadAvailable", Field::Json(json!(true))),
                ("createdAt", Field::Json(json!(iso_ms(p.created_at_us)))),
                ("leaseExpiresAt", Field::Json(json!(lease.map(iso_ms)))),
            ]);
            push_object(&mut out, fields);
            total += 1;
        }
        out.push_str("],\"mode\":");
        out.push_str(&mode.to_string());
        let pagination = if cut {
            json!({"limit":limit,"offset":offset,"nextOffset":offset + total,"hasMore":true})
        } else {
            json!({"limit":limit,"offset":offset})
        };
        out.push_str(",\"pagination\":");
        out.push_str(&pagination.to_string());
        out.push_str(",\"total\":");
        out.push_str(&total.to_string());
        out.push('}');
        Ok(ApiOut::json(200, out))
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
        let rec_offset = rec.offset;
        let detail=tokio::task::spawn_blocking(move || store.read(|r| {
            let cfg=r.queue(&tenant,&queue)?;
            let mut groups=Vec::new(); let mut cursors=Vec::new(); let mut dlq=None;
            r.scan_cursors(pid,usize::MAX,&mut |g,c| { let consumed=rec_offset as i64<=c.committed; groups.push(json!({"name":g,"committed":c.committed,"consumed":consumed,"leaseExpiresAt":c.lease_expires_at_us.map(crate::rsm::planner::timers::iso_us)})); cursors.push((g.to_string(),c)); true })?;
            let pref=keys::dlq_by_pos_pid_prefix(pid); r.scan_raw(Keyspace::DlqByPos,&pref,&pref,usize::MAX,&mut |_k,v| { if let Ok(ids)=rows::dlq_ids_decode(v) { for id in ids { if let Ok(Some(row))=r.dlq(&tenant,&queue,&id) { if row.txn==txn2 { dlq=Some(row); return false; } } } } true })?;
            Ok((cfg,groups,cursors,dlq))
        })).await.map_err(|e|RsmError::Internal(format!("message detail: {e}")))?.map_err(read_error)?;
        let (cfg, groups, cursors, dlq) = detail;
        let status = if dlq.is_some() {
            "dead_letter"
        } else {
            message_status(
                rec.offset,
                &cursors,
                &std::collections::HashSet::new(),
                super::super::wall_micros(),
            )
            .0
        };
        let has_queue_mode = groups
            .iter()
            .any(|g| g.get("name").and_then(Value::as_str) == Some("__QUEUE_MODE__"));
        let bus_groups = groups
            .iter()
            .filter(|g| g.get("name").and_then(Value::as_str) != Some("__QUEUE_MODE__"))
            .count();
        // The payload copied in as stored, not parsed (a 64 MiB message
        // was a 2.5 GB tree).
        let mut fields: Vec<(&str, Field<'_>)> = vec![
            ("id", Field::Json(json!(uuid_bytes_to_string(&rec.id)))),
            ("transactionId", Field::Json(json!(rec.txn))),
            ("data", Field::Payload(&rec.payload)),
            ("payload", Field::Payload(&rec.payload)),
            ("traceId", Field::Json(json!(rec.trace_id.map(|x| uuid_bytes_to_string(&x))))),
            ("producerSub", Field::Json(json!(rec.producer_sub))),
            ("createdAt", Field::Json(json!(crate::rsm::planner::timers::iso_us(rec.created_at_us)))),
            ("partitionId", Field::Json(json!(uuid_bytes_to_string(&part.row.uuid)))),
            ("partition", Field::Json(json!(part.row.partition))),
            ("queue", Field::Json(json!(part.row.queue))),
            ("queuePath", Field::Json(json!(format!("{}/{}", part.row.queue, part.row.partition)))),
            ("namespace", Field::Json(json!(cfg.as_ref().and_then(|x| x.namespace.clone())))),
            ("task", Field::Json(json!(cfg.as_ref().and_then(|x| x.task.clone())))),
            ("status", Field::Json(json!(status))),
            ("errorMessage", Field::Json(json!(dlq.as_ref().map(|x| x.error.clone())))),
            ("retryCount", Field::Json(json!(dlq.as_ref().map(|x| x.retry_count).unwrap_or(0)))),
            ("isEncrypted", Field::Json(json!(rec.encrypted))),
            ("queueConfig", Field::Json(json!(cfg.as_ref().map(super::config_options)))),
        ];
        let ty = match (has_queue_mode, bus_groups > 0) {
            (false, false) => "none",
            (true, false) => "queue",
            (false, true) => "bus",
            (true, true) => "hybrid",
        };
        fields.push((
            "mode",
            Field::Json(json!({"hasQueueMode":has_queue_mode,"busGroupsCount":bus_groups,"type":ty})),
        ));
        fields.push(("consumerGroups", Field::Json(Value::Array(groups))));
        let mut out = String::with_capacity(2 * rec.payload.len() + 1024);
        push_object(&mut out, fields);
        Ok(ApiOut::json(200, out))
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
        let budget = read_page_max_bytes();
        let encryption = self.encryption.clone();
        // Ranked without payloads (the whole DLQ with its payloads was
        // copied on every call), then only the page's rows are read whole,
        // one at a time, until the page's byte budget.
        let (total, out) = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                // The newest `offset + limit`, newest first, ties in scan
                // order (the stable sort this replaces): a bounded heap, not
                // every head of the DLQ.
                let need = offset.saturating_add(limit);
                let mut heap: std::collections::BinaryHeap<
                    std::cmp::Reverse<(i64, std::cmp::Reverse<usize>, DlqSlot)>,
                > = std::collections::BinaryHeap::new();
                let mut total = 0usize;
                each_dlq_head(
                    r,
                    &tenant,
                    queue.as_deref(),
                    group.as_deref(),
                    usize::MAX,
                    &mut |e| {
                        let key = (e.3.failed_at_us, std::cmp::Reverse(total), DlqSlot(e));
                        total += 1;
                        if heap.len() < need {
                            heap.push(std::cmp::Reverse(key));
                        } else if heap
                            .peek()
                            .is_some_and(|m| (key.0, key.1) > (m.0 .0, m.0 .1))
                        {
                            heap.pop();
                            heap.push(std::cmp::Reverse(key));
                        }
                    },
                )?;
                let mut rows: Vec<DlqEntry> = heap
                    .into_sorted_vec()
                    .into_iter()
                    .map(|std::cmp::Reverse((_, _, DlqSlot(e)))| e)
                    .collect();
                // `into_sorted_vec` of `Reverse` keys is newest first already.
                rows.truncate(need);
                let mut partitions: HashMap<Pid, (String, [u8; 16])> = HashMap::new();
                let mut out = String::from("{\"messages\":[");
                let mut n = 0usize;
                let mut cut = false;
                for (t, q, id, head) in rows.into_iter().skip(offset).take(limit) {
                    if n > 0 && out.len() >= budget {
                        cut = true;
                        break;
                    }
                    let Some(mut row) = r.dlq(&t, &q, &id)? else {
                        continue; // deleted since the scan
                    };
                    if let Some(payload) = encryption.decrypt_payload_bytes(&row.payload) {
                        row.payload = payload;
                    }
                    if !partitions.contains_key(&head.pid) {
                        if let Some(part) = r.partition(head.pid)? {
                            partitions.insert(head.pid, (part.partition, part.uuid));
                        }
                    }
                    if n > 0 {
                        out.push(',');
                    }
                    push_object(
                        &mut out,
                        dlq_fields(&q, &id, &row, partitions.get(&head.pid)),
                    );
                    n += 1;
                }
                let pagination = if cut {
                    json!({"limit":limit,"offset":offset,"nextOffset":offset + n,"hasMore":true})
                } else {
                    json!({"limit":limit,"offset":offset})
                };
                out.push_str("],\"pagination\":");
                out.push_str(&pagination.to_string());
                out.push_str(",\"total\":");
                out.push_str(&total.to_string());
                out.push('}');
                Ok((total, out))
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("dlq read: {e}")))?
        .map_err(read_error)?;
        let _ = total;
        Ok(ApiOut::json(200, out))
    }

    pub(super) async fn api_groups(
        &self,
        ctx: ReqCtx,
        only: Option<&str>,
    ) -> Result<ApiOut, RsmError> {
        let store = self.store.clone();
        let qlog = self.qlog_reader.clone();
        let reader = self.reader.clone();
        let tenant = ctx.tenant.clone();
        let only = only.map(str::to_string);
        let now = super::super::wall_micros();
        let answer = tokio::task::spawn_blocking(move || {
            store.read(|r| match only.as_deref() {
                Some(group) => group_detail(r, qlog.as_ref(), &reader, &tenant, group, now),
                None => group_view(r, qlog.as_ref(), &reader, &tenant, now).map(Value::Array),
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("groups read: {e}")))?
        .map_err(read_error)?;
        // The list is a bare array; one group is `{queue: {...}}`, 404 when
        // it has no cursor.
        if answer.as_object().is_some_and(|o| o.is_empty()) {
            return Ok(ApiOut::json(
                404,
                json!({"error":"Consumer group not found"}).to_string(),
            ));
        }
        Ok(ApiOut::json(200, answer.to_string()))
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
        let qlog = self.qlog_reader.clone();
        let reader = self.reader.clone();
        let tenant = ctx.tenant.clone();
        let rows = tokio::task::spawn_blocking(move || {
            store.read(|r| lagging_partitions(r, qlog.as_ref(), &reader, &tenant, min, now))
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
        // Typed: the small fields as values, `data` borrowed as its raw text
        // (the whole body as a tree took 20 times its size).
        let v: TraceBody<'_> =
            serde_json::from_slice(body).map_err(|e| super::rejected("bad_body", e))?;
        let s = |a: &Option<Value>, b: &Option<Value>| -> Option<String> {
            a.as_ref()
                .or(b.as_ref())
                .and_then(Value::as_str)
                .map(str::to_string)
        };
        let txn = s(&v.transaction_id, &v.transaction_id_snake).unwrap_or_default();
        if txn.is_empty() {
            return Ok(ApiOut::json(
                400,
                json!({"error":"transactionId is required"}).to_string(),
            ));
        }
        let data: &[u8] = match v.data {
            Some(raw) => raw.get().as_bytes(),
            None => b"null",
        };
        if data.len() > trace_max_bytes() {
            return Ok(ApiOut::json(
                413,
                json!({
                    "success": false,
                    "error": format!(
                        "trace data is {} bytes, the ceiling is {} (QUEEN_TRACE_MAX_BYTES)",
                        data.len(),
                        trace_max_bytes()
                    ),
                    "code": "payload_too_large",
                })
                .to_string(),
            ));
        }
        // What `Value` would not parse is refused, as the whole-body tree
        // refused it.
        let Some(data) = super::json_out::checked(data) else {
            return Ok(ApiOut::json(
                400,
                json!({"error":"data is not a JSON value","code":"bad_body"}).to_string(),
            ));
        };
        let pid_s = s(&v.partition_id, &v.partition_id_snake);
        let pid = self.resolve_pid(&ctx.tenant, pid_s.as_deref()).await?;
        if pid_s.is_some() && pid.is_none() {
            // A pid outside the tenant answers with 404.
            return Ok(ApiOut::json(
                404,
                json!({"success":false,"error":"Partition not found"}).to_string(),
            ));
        }
        let names = v
            .trace_names
            .as_ref()
            .or(v.names.as_ref())
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
                .trace_id
                .as_ref()
                .and_then(Value::as_str)
                .and_then(uuid_string_to_bytes)
                .unwrap_or_else(crate::util::uuidv7_bytes),
            tenant: ctx.tenant.clone(),
            pid,
            message_id: v
                .message_id
                .as_ref()
                .and_then(Value::as_str)
                .and_then(uuid_string_to_bytes),
            txn,
            consumer_group: v
                .consumer_group
                .as_ref()
                .and_then(Value::as_str)
                .map(str::to_string),
            event_type: s(&v.event_type, &v.type_).unwrap_or_else(|| "event".to_string()),
            data: data.as_bytes().to_vec(),
            worker: v.worker.as_ref().and_then(Value::as_str).map(str::to_string),
            names,
            created_at_us: super::super::wall_micros(),
        };
        // A key over the engine's limit would reach apply as `KeyTooLong`,
        // which stops every node on the same entry: refused here instead.
        if let Err(why) = crate::rsm::traces::check_keys(&event, self.store.max_key_len()) {
            return Ok(ApiOut::json(400, json!({"error":why}).to_string()));
        }
        let id = uuid_bytes_to_string(&event.trace_id);
        // Catalogue version 4 keeps traces on disk (`rsm/traces.rs`), once
        // every member reads it; until then the version-1 RAM row.
        let effect = if self.cluster_allows(crate::rsm::effect::VERSION_4) {
            Effect::TraceRecord { event }
        } else {
            Effect::TraceAppend { event }
        };
        self.submit_effects(&ctx, vec![effect]).await?;
        Ok(ApiOut::json(
            201,
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
        let traces = self.traces.clone();
        let tenant = ctx.tenant.clone();
        let names = tokio::task::spawn_blocking(move || {
            // name → (traces carrying it, the distinct messages it touched,
            // last at): the legacy RAM rows of this tenant, then the trace
            // environment's index (no body is read).
            let mut stats: std::collections::BTreeMap<String, crate::rsm::traces::NameStat> =
                std::collections::BTreeMap::new();
            store.read(|r| {
                legacy_traces(r, &tenant, None, None, &mut |_k, e| {
                    let msg = crate::rsm::traces::message_hash(e.pid, &e.txn);
                    for n in &e.names {
                        stats
                            .entry(n.clone())
                            .or_default()
                            .add(1, msg, e.created_at_us);
                    }
                })
            })?;
            traces.read(|t| t.name_stats(&tenant, &mut stats))?;
            let total = stats.len();
            let rows = stats
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|(name, s)| {
                    json!({
                        "trace_name":name,
                        "trace_count":s.traces,
                        "message_count":s.messages.len(),
                        "last_seen":crate::rsm::planner::timers::iso_us(s.last_seen_us)
                    })
                })
                .collect::<Vec<_>>();
            Ok::<_, crate::rsm::store::StoreError>((total, rows))
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
        if pid.is_some() && wanted_pid.is_none() {
            // Not this tenant's partition: nothing, never "no pid filter".
            return Ok(ApiOut::json(
                200,
                json!({"traces":[],"events":[],"total":0,"pagination":{"limit":limit,"offset":offset}}).to_string(),
            ));
        }
        let tenant = ctx.tenant.clone();
        let txn = txn.map(str::to_string);
        let name = name.map(str::to_string);
        let store = self.store.clone();
        let traces = self.traces.clone();
        let (total, events, cut) = tokio::task::spawn_blocking(move || {
            // Newest first; traces of one instant in the order the legacy read
            // gave them (its stable sort over the primary key order). Only the
            // page is kept: every other match is counted from the index.
            let mut legacy: Vec<(i64, Vec<u8>, TraceEvent)> = Vec::new();
            store.read(|r| {
                legacy_traces(
                    r,
                    &tenant,
                    wanted_pid.zip(txn.as_deref()),
                    name.as_deref(),
                    &mut |k, e| {
                        let tie = crate::rsm::traces::legacy_tie(&tenant, k).unwrap_or_default();
                        legacy.push((e.created_at_us, tie, e));
                    },
                )
            })?;
            legacy.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
            let mut legacy = legacy.into_iter().peekable();
            let (total, page) = traces.read(|t| {
                let mut pager: crate::rsm::traces::Pager<FoundTrace> =
                    crate::rsm::traces::Pager::new(offset, limit);
                {
                    let mut feed = |created: i64, tie: &[u8], id| {
                        while let Some((at, _, _)) = legacy.peek() {
                            if *at < created {
                                break;
                            }
                            if let Some((at, tie, e)) = legacy.next() {
                                pager.push(at, &tie, FoundTrace::Legacy(e));
                            }
                        }
                        pager.push(created, tie, FoundTrace::Disk(id));
                        true
                    };
                    match (wanted_pid, txn.as_deref(), name.as_deref()) {
                        (Some(p), Some(x), _) => t.message_rev(&tenant, Some(p), x, &mut feed)?,
                        (None, None, Some(n)) => t.name_rev(&tenant, n, &mut feed)?,
                        _ => {}
                    }
                }
                for (at, tie, e) in legacy.by_ref() {
                    pager.push(at, &tie, FoundTrace::Legacy(e));
                }
                let (total, found) = pager.finish();
                // The page's bodies, in this same read: a trim between the
                // index walk and the bodies cannot take one away.
                let mut page = Vec::with_capacity(found.len());
                for f in found {
                    match f {
                        FoundTrace::Legacy(e) => page.push(e),
                        FoundTrace::Disk(id) => page.extend(t.body(id)?),
                    }
                }
                Ok((total, page))
            })?;
            // Each event written once, its `data` as stored, and the page
            // cut at `QUEEN_READ_PAGE_MAX_BYTES` (the answer carries the list
            // twice, `traces` and `events`).
            let budget = read_page_max_bytes();
            let (events, cut) = store.read(|r| {
                let mut list = String::new();
                let mut n = 0usize;
                let mut cut = false;
                for e in page {
                    if n > 0 && 2 * list.len() >= budget {
                        cut = true;
                        break;
                    }
                    let location = e
                        .pid
                        .and_then(|pid| r.partition(pid).ok().flatten())
                        .map(|p| (p.queue, p.partition));
                    if n > 0 {
                        list.push(',');
                    }
                    push_object(&mut list, trace_fields(&e, location));
                    n += 1;
                }
                Ok((list, if cut { Some(n) } else { None }))
            })?;
            Ok::<_, crate::rsm::store::StoreError>((total, events, cut))
        })
        .await
        .map_err(|e| RsmError::Internal(format!("traces: {e}")))?
        .map_err(read_error)?;
        let pagination = match cut {
            Some(n) => json!({"limit":limit,"offset":offset,"nextOffset":offset + n,"hasMore":true}),
            None => json!({"limit":limit,"offset":offset}),
        };
        let mut out = String::with_capacity(2 * events.len() + 128);
        out.push_str("{\"events\":[");
        out.push_str(&events);
        out.push_str("],\"pagination\":");
        out.push_str(&pagination.to_string());
        out.push_str(",\"total\":");
        out.push_str(&total.to_string());
        out.push_str(",\"traces\":[");
        out.push_str(&events);
        out.push_str("]}");
        Ok(ApiOut::json(200, out))
    }

    pub(super) async fn resolve_pid(
        &self,
        tenant: &str,
        id: Option<&str>,
    ) -> Result<Option<Pid>, RsmError> {
        let Some(id) = id else { return Ok(None) };
        if let Ok(p) = id.parse::<u64>() {
            // Dense small integers: only the owner may address one.
            let store = self.store.clone();
            let tenant = tenant.to_string();
            return tokio::task::spawn_blocking(move || {
                store.read(|r| {
                    Ok(r.partition(p)?
                        .filter(|row| row.tenant == tenant)
                        .map(|_| p))
                })
            })
            .await
            .map_err(|e| RsmError::Internal(format!("partition lookup: {e}")))?
            .map_err(read_error);
        }
        let uuid = uuid_string_to_bytes(id);
        let store = self.store.clone();
        let tenant = tenant.to_string();
        // No index maps a partition's uuid to its pid: the rows are scanned,
        // read in place (no copy per row), and at most
        // `UUID_SCAN_MAX_ROWS` of them; past that the id is not found (a
        // 404), and a numeric id always works.
        let Some(uuid) = uuid else { return Ok(None) };
        tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut found = None;
                r.scan_raw(
                    Keyspace::Partitions,
                    &[],
                    &[],
                    UUID_SCAN_MAX_ROWS,
                    &mut |k, v| {
                        if let (Some(pid), Ok(p)) = (keys::pid_of(k), rows::partition_ref_decode(v)) {
                            if p.head.uuid == uuid && p.tenant == tenant {
                                found = Some(pid);
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
        // That one partition's row and files, not every partition of the
        // tenant with its file list.
        let store = self.store.clone();
        let t = tenant.to_string();
        let part = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let Some(row) = r.partition(pid)?.filter(|row| row.tenant == t) else {
                    return Ok(None);
                };
                let mut sealed = Vec::new();
                r.scan_partition_files(pid, usize::MAX, &mut |f| {
                    sealed.push(f);
                    true
                })?;
                Ok(Some(Part { pid, row, sealed }))
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("partition read: {e}")))?
        .map_err(read_error)?;
        let Some(part) = part else {
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

#[allow(clippy::too_many_arguments)]
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

/// The most records one fetch entry answers, whatever its byte budget.
const FETCH_RECORDS_PER_ENTRY: usize = 10_000;

/// [`walk_records`] for a reader that wants only the payloads: each record's
/// offset, its segment's write time, its transaction id when `want_txn`, its
/// payload as a slice of the segment it was read from (no copy) and whether that
/// payload is encrypted. Same walk, same bounds, same gaps.
///
/// From the queue log a record is read as a pop renders it: looked up under
/// its log's lock and read with the lock released, the next one looked for in
/// the file this one came from. The walk used to hold the lock across the
/// read: a fetch from an old offset reads from the disk, and the appends to
/// that log waited behind it. Once it finds its records out of the page
/// cache, the walk also has the kernel read ahead
/// ([`QLogReader::read_owned_ahead`]).
#[allow(clippy::too_many_arguments)]
pub(super) fn walk_payloads(
    reader: &crate::rsm::segments::Reader,
    qlog: Option<&QLogReader>,
    tenant: &str,
    queue: &str,
    part: &Part,
    mut off: u64,
    high: u64,
    limit: usize,
    want_txn: bool,
    mut cb: impl FnMut(u64, i64, Option<String>, bytes::Bytes, bool) -> bool,
) -> Result<(), RsmError> {
    let qid = qlog.map(|_| QLogReader::queue_id_of(tenant, queue));
    let mut ahead = crate::rsm::qlog::set::ReadAhead::default();
    let mut n = 0usize;
    while off < high && n < limit {
        let got = match (qlog, qid) {
            (Some(q), Some(id)) => q
                .read_owned_ahead(id, part.pid, off, high, &mut ahead)
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
        let blob = bytes::Bytes::from(blob);
        if let Some(frames) = unpack_frames_ref(&blob) {
            for (i, f) in frames.into_iter().enumerate() {
                let pos = base + i as u64;
                if pos < off || pos >= high {
                    continue;
                }
                n += 1;
                let txn = want_txn.then(|| f.txn.to_string());
                if !cb(pos, created, txn, blob.slice_ref(f.payload), f.encrypted) {
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

fn env_limit(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// `POST /api/v1/fetch`'s call total when the body names none
/// (`QUEEN_FETCH_MAX_BYTES`, default 50 MiB).
fn fetch_default_max_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| env_limit("QUEEN_FETCH_MAX_BYTES", 50 << 20))
}

/// The bytes one page of messages, dead letters or traces may carry
/// (`QUEEN_READ_PAGE_MAX_BYTES`, default 16 MiB). A page past it stops early
/// and says so in its paging; the first row always goes out.
pub(super) fn read_page_max_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| env_limit("QUEEN_READ_PAGE_MAX_BYTES", 16 << 20))
}

/// One trace's `data` at most (`QUEEN_TRACE_MAX_BYTES`, default 1 MiB).
fn trace_max_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| env_limit("QUEEN_TRACE_MAX_BYTES", 1 << 20))
}

pub(super) fn payload_json(b: &[u8]) -> Value {
    if b.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(b)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(b).into_owned()))
    }
}

/// `POST /api/v1/traces`, read field by field: `data` stays the body's own
/// text. A small field of the wrong type reads as absent, as it did from the
/// tree.
#[derive(Deserialize)]
struct TraceBody<'a> {
    #[serde(rename = "transactionId", default)]
    transaction_id: Option<Value>,
    #[serde(rename = "transaction_id", default)]
    transaction_id_snake: Option<Value>,
    #[serde(rename = "partitionId", default)]
    partition_id: Option<Value>,
    #[serde(rename = "partition_id", default)]
    partition_id_snake: Option<Value>,
    #[serde(rename = "traceNames", default)]
    trace_names: Option<Value>,
    #[serde(default)]
    names: Option<Value>,
    #[serde(rename = "traceId", default)]
    trace_id: Option<Value>,
    #[serde(rename = "messageId", default)]
    message_id: Option<Value>,
    #[serde(rename = "consumerGroup", default)]
    consumer_group: Option<Value>,
    #[serde(rename = "eventType", default)]
    event_type: Option<Value>,
    #[serde(rename = "type", default)]
    type_: Option<Value>,
    #[serde(borrow, default)]
    data: Option<&'a serde_json::value::RawValue>,
    #[serde(default)]
    worker: Option<Value>,
}

/// One DLQ row with the queue, group and id it is filed under.
type DlqEntry = (String, String, [u8; 16], DlqRow);

fn decrypt_record(encryption: &crate::encryption::Encryption, record: &mut Record) {
    if record.encrypted {
        if let Some(payload) = encryption.decrypt_payload_bytes(&record.payload) {
            record.payload = payload;
        }
    }
}
/// A message row's own fields (`message_json` before 2.6.1): the payload
/// borrowed, never parsed.
fn message_fields(r: &Record) -> Vec<(&'static str, Field<'_>)> {
    let txn_hash = hex::encode(crate::util::txn_hash128(&r.txn));
    vec![
        ("id", Field::Json(json!(uuid_bytes_to_string(&r.id)))),
        ("transactionId", Field::Json(json!(r.txn))),
        ("txnHash", Field::Json(json!(txn_hash))),
        ("data", Field::Payload(&r.payload)),
        ("payload", Field::Payload(&r.payload)),
        (
            "traceId",
            Field::Json(json!(r.trace_id.map(|x| uuid_bytes_to_string(&x)))),
        ),
        ("producerSub", Field::Json(json!(r.producer_sub))),
        (
            "createdAt",
            Field::Json(json!(crate::rsm::planner::timers::iso_us(r.created_at_us))),
        ),
        ("offset", Field::Json(json!(r.offset))),
        ("queue", Field::Json(json!(r.queue))),
        ("partition", Field::Json(json!(r.partition))),
        (
            "partitionId",
            Field::Json(json!(uuid_bytes_to_string(&r.partition_id))),
        ),
        ("status", Field::Json(json!("pending"))),
        (
            "segment",
            Field::Json(json!({"seq":r.segment_base,"frameIdx":r.frame_idx})),
        ),
        ("isEncrypted", Field::Json(json!(r.encrypted))),
    ]
}

/// The deepest `offset` `GET /api/v1/messages` serves
/// (`QUEEN_MESSAGES_MAX_OFFSET`, default 10,000).
fn messages_max_offset() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| env_limit("QUEEN_MESSAGES_MAX_OFFSET", 10_000))
}

pub(super) fn scan_dlq_rows<R: Reads + ?Sized>(
    r: &R,
    tenant: &str,
    queue: Option<&str>,
    group: Option<&str>,
) -> crate::rsm::store::Result<Vec<DlqEntry>> {
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
/// A dead letter without its payload: every field but the payload, which is
/// skipped in place (never copied). `None` for a row that does not decode.
fn dlq_head_decode(b: &[u8]) -> Option<DlqRow> {
    use crate::rsm::effect::Reader;
    let mut r = Reader::new(b);
    if r.u8("v").ok()? != rows::ROW_V1 {
        return None;
    }
    let pid = r.u64("pid").ok()?;
    let group = r.str("group").ok()?;
    let offset = r.i64("offset").ok()?;
    let message_id = r.opt_bytes16("message_id").ok()?;
    let txn = r.str("txn").ok()?;
    let n = r.u32("payload").ok()? as usize;
    let at = b.len() - r.remaining();
    let rest = b.get(at.checked_add(n)?..)?;
    let mut r = Reader::new(rest);
    Some(DlqRow {
        pid,
        group,
        offset,
        message_id,
        txn,
        payload: Vec::new(),
        error: r.str("error").ok()?,
        retry_count: r.u32("retry_count").ok()?,
        failed_at_us: r.i64("failed_at_us").ok()?,
    })
}

/// [`scan_dlq_rows`] without the payloads, and at most `max_rows` of them:
/// what a count, an error bucket or the ranking of a page needs. The second
/// value says the scan stopped at `max_rows`.
pub(super) fn scan_dlq_heads<R: Reads + ?Sized>(
    r: &R,
    tenant: &str,
    queue: Option<&str>,
    group: Option<&str>,
    max_rows: usize,
) -> crate::rsm::store::Result<(Vec<DlqEntry>, bool)> {
    let mut out = Vec::new();
    let capped = each_dlq_head(r, tenant, queue, group, max_rows, &mut |e| out.push(e))?;
    Ok((out, capped))
}

/// [`scan_dlq_heads`], each row handed over as it is read.
pub(super) fn each_dlq_head<R: Reads + ?Sized>(
    r: &R,
    tenant: &str,
    queue: Option<&str>,
    group: Option<&str>,
    max_rows: usize,
    each: &mut dyn FnMut(DlqEntry),
) -> crate::rsm::store::Result<bool> {
    let prefix = keys::queues_prefix(tenant);
    let mut err = None;
    let mut seen = 0usize;
    let mut capped = false;
    r.scan_raw(
        Keyspace::Dlq,
        &prefix,
        &prefix,
        usize::MAX,
        &mut |k, v| {
            if seen >= max_rows {
                capped = true;
                return false;
            }
            seen += 1;
            match (keys::dlq_parts(k), dlq_head_decode(v)) {
                (Some((t, q, id)), Some(row))
                    if queue.is_none_or(|x| x == q) && group.is_none_or(|x| x == row.group) =>
                {
                    each((t, q, id, row));
                    true
                }
                (Some(_), Some(_)) => true,
                _ => {
                    err = Some(crate::rsm::store::StoreError::corrupt(
                        Keyspace::Dlq,
                        "dlq row",
                    ));
                    false
                }
            }
        },
    )?;
    match err {
        Some(e) => Err(e),
        None => Ok(capped),
    }
}

/// A dead letter in the page heap: ordered by the heap's key alone.
struct DlqSlot(DlqEntry);

impl PartialEq for DlqSlot {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for DlqSlot {}
impl PartialOrd for DlqSlot {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for DlqSlot {
    fn cmp(&self, _: &Self) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
}

/// The most dead letters a DLQ page or a status count reads.
pub(super) const DLQ_SCAN_MAX_ROWS: usize = 10_000;

/// One dead letter's fields, its payload borrowed (`dlq_json` before 2.6.1).
fn dlq_fields<'a>(
    q: &'a str,
    id: &[u8; 16],
    r: &'a DlqRow,
    partition: Option<&(String, [u8; 16])>,
) -> Vec<(&'static str, Field<'a>)> {
    vec![
        ("id", Field::Json(json!(uuid_bytes_to_string(id)))),
        ("queue", Field::Json(json!(q))),
        ("partition", Field::Json(json!(partition.map(|p| p.0.clone())))),
        ("partitionId", Field::Json(json!(r.pid.to_string()))),
        (
            "createdAt",
            Field::Json(json!(crate::rsm::planner::timers::iso_us(r.failed_at_us))),
        ),
        ("consumerGroup", Field::Json(json!(r.group))),
        ("offset", Field::Json(json!(r.offset))),
        (
            "messageId",
            Field::Json(json!(r.message_id.map(|x| uuid_bytes_to_string(&x)))),
        ),
        ("transactionId", Field::Json(json!(r.txn))),
        ("data", Field::Payload(&r.payload)),
        ("payload", Field::Payload(&r.payload)),
        ("errorMessage", Field::Json(json!(r.error))),
        ("retryCount", Field::Json(json!(r.retry_count))),
        (
            "failedAt",
            Field::Json(json!(crate::rsm::planner::timers::iso_us(r.failed_at_us))),
        ),
    ]
}

/// One `(partition, group)` cursor with its lag inputs. `pending` is the §9
/// arithmetic `GREATEST(last_offset - GREATEST(committed, log_start - 1), 0)`.
struct CursorLag {
    queue: String,
    partition: String,
    partition_uuid: [u8; 16],
    group: String,
    worker: Option<String>,
    committed: i64,
    total_consumed: u64,
    pending: u64,
    lease_live: bool,
    /// The stamp of the oldest message this cursor has not consumed; `None`
    /// when it is caught up.
    oldest_unconsumed_us: Option<i64>,
}

impl CursorLag {
    /// `EXTRACT(EPOCH FROM (NOW() - oldest_unconsumed_at))::integer`: rounded
    /// to the nearest second, `None` for a caught-up cursor.
    fn lag_seconds(&self, now: i64) -> Option<i64> {
        self.oldest_unconsumed_us
            .map(|t| ((now - t) as f64 / 1_000_000.0).round() as i64)
    }
}

/// The stamp of the oldest message a cursor at `committed` has not consumed:
/// the record covering `committed + 1`, or the first one after it once
/// retention has deleted past the cursor. The queue log answers this from
/// its in-memory index, without reading a payload.
pub(super) fn oldest_unconsumed_us(
    qlog: Option<&QLogReader>,
    reader: &crate::rsm::segments::Reader,
    tenant: &str,
    pid: Pid,
    p: &PartitionRow,
    sealed: &dyn Fn() -> Vec<u32>,
    committed: i64,
) -> Option<i64> {
    let high = (p.last_offset + 1).max(0) as u64;
    let from = ((committed + 1).max(0) as u64).max(p.log_start);
    if from >= high {
        return None;
    }
    match qlog {
        Some(q) => {
            let mut found = None;
            let _ = q.claim_frames(
                QLogReader::queue_id_of(tenant, &p.queue),
                pid,
                from,
                high,
                false,
                &mut |_, _, created_at_us, _| {
                    found = Some(created_at_us);
                    false
                },
            );
            found
        }
        None => {
            let files = sealed();
            let mut off = from;
            // A retention gap at `from`: the next live record is the oldest.
            while off < high {
                match reader.read_at_within(
                    bucket_of(tenant, &p.queue, &p.partition),
                    pid,
                    off,
                    &files,
                    None,
                ) {
                    Ok(Some(f)) => return Some(f.created_at_us),
                    Ok(None) => off += 1,
                    Err(_) => return None,
                }
            }
            None
        }
    }
}

/// Every cursor of the tenant (of one group when `only` is set), with its lag
/// inputs. Queue-mode cursors (`__QUEUE_MODE__`) are included.
fn cursor_lags<R: Reads + ?Sized>(
    r: &R,
    qlog: Option<&QLogReader>,
    reader: &crate::rsm::segments::Reader,
    tenant: &str,
    only: Option<&str>,
    now: i64,
) -> crate::rsm::store::Result<Vec<CursorLag>> {
    let mut queues = Vec::new();
    r.scan_queues(tenant, usize::MAX, &mut |q, _| {
        queues.push(q.to_string());
        true
    })?;
    let mut out = Vec::new();
    for queue in queues {
        let mut pids = Vec::new();
        r.scan_queue_partitions(tenant, &queue, None, usize::MAX, &mut |pid| {
            pids.push(pid);
            true
        })?;
        for pid in pids {
            let Some(p) = r.partition(pid)? else { continue };
            let mut cursors = Vec::new();
            match only {
                Some(g) => {
                    if let Some(c) = r.cursor(pid, g)? {
                        cursors.push((g.to_string(), c));
                    }
                }
                None => {
                    r.scan_cursors(pid, usize::MAX, &mut |g, c| {
                        cursors.push((g.to_string(), c));
                        true
                    })?;
                }
            }
            for (group, c) in cursors {
                let pending = p.pending_from(c.committed);
                let oldest = if pending > 0 {
                    let sealed = || {
                        let mut files = Vec::new();
                        let _ = r.scan_partition_files(pid, usize::MAX, &mut |f| {
                            files.push(f);
                            true
                        });
                        files
                    };
                    oldest_unconsumed_us(qlog, reader, tenant, pid, &p, &sealed, c.committed)
                } else {
                    None
                };
                out.push(CursorLag {
                    queue: queue.clone(),
                    partition: p.partition.clone(),
                    partition_uuid: p.uuid,
                    group,
                    worker: c.worker.clone(),
                    committed: c.committed,
                    total_consumed: c.total_consumed,
                    pending,
                    lease_live: rows::lease_live(&c, now),
                    oldest_unconsumed_us: oldest,
                });
            }
        }
    }
    Ok(out)
}

/// `subscriptionTimestamp`: mode `all` registers at the epoch, which the
/// planner encodes as `i64::MIN`.
fn subscription_ts_json(us: i64) -> Value {
    Value::String(crate::rsm::planner::timers::iso_us(us.max(0)))
}

/// `GET /api/v1/consumer-groups/:group`:
/// `{ queue: { conflation, kind, liveMembers, lastSeenAt, inactiveSeconds,
/// partitions: [...] } }`, the reader fields as the listing's ([`Readers`]).
fn group_detail<R: Reads + ?Sized>(
    r: &R,
    qlog: Option<&QLogReader>,
    reader: &crate::rsm::segments::Reader,
    tenant: &str,
    group: &str,
    now: i64,
) -> crate::rsm::store::Result<Value> {
    let lags = cursor_lags(r, qlog, reader, tenant, Some(group), now)?;
    let mut by_queue: std::collections::BTreeMap<String, Vec<&CursorLag>> =
        std::collections::BTreeMap::new();
    for l in &lags {
        by_queue.entry(l.queue.clone()).or_default().push(l);
    }
    let kafka = kafka_groups::kafka_groups(r, tenant, [group], now)?;
    let mut answer = serde_json::Map::new();
    for (queue, mut rows) in by_queue {
        rows.sort_by(|a, b| a.partition.cmp(&b.partition));
        let readers = Readers::of(&kafka, group, &queue, now);
        let conflation = r
            .group(tenant, &queue, group)?
            .is_some_and(|m| m.meta.conflation);
        let partitions: Vec<Value> = rows
            .iter()
            .map(|l| {
                json!({
                    "partition":l.partition,
                    "workerId":l.worker,
                    "lastConsumedAt":Value::Null,
                    "totalConsumed":l.total_consumed,
                    "offsetLag":l.pending,
                    "timeLagSeconds":l.lag_seconds(now).unwrap_or(0),
                    "leaseActive":l.lease_live
                })
            })
            .collect();
        let mut entry = serde_json::Map::new();
        entry.insert("conflation".into(), json!(conflation));
        readers.insert_into(&mut entry, now);
        entry.insert("partitions".into(), Value::Array(partitions));
        answer.insert(queue, Value::Object(entry));
    }
    Ok(Value::Object(answer))
}

/// `GET /api/v1/consumer-groups/lagging`: every cursor, queue mode included,
/// whose oldest unconsumed message is older than `min_lag_seconds`, oldest
/// first.
fn lagging_partitions<R: Reads + ?Sized>(
    r: &R,
    qlog: Option<&QLogReader>,
    reader: &crate::rsm::segments::Reader,
    tenant: &str,
    min_lag_seconds: i64,
    now: i64,
) -> crate::rsm::store::Result<Vec<Value>> {
    let lagging: Vec<CursorLag> = cursor_lags(r, qlog, reader, tenant, None, now)?
        .into_iter()
        .filter(|l| l.lag_seconds(now).is_some_and(|lag| lag > min_lag_seconds))
        .collect();
    let kafka =
        kafka_groups::kafka_groups(r, tenant, lagging.iter().map(|l| l.group.as_str()), now)?;
    let mut rows: Vec<(i64, Value)> = lagging
        .into_iter()
        .filter_map(|l| {
            let lag = l.lag_seconds(now)?;
            let oldest = l.oldest_unconsumed_us?;
            Some((
                lag,
                json!({
                    "consumer_group":l.group,
                    "queue_name":l.queue,
                    "partition_name":l.partition,
                    "partition_id":uuid_bytes_to_string(&l.partition_uuid),
                    "worker_id":l.worker,
                    "kind":if kafka.contains_key(&l.group) { "kafka" } else { "queen" },
                    "offset_lag":l.pending,
                    "time_lag_seconds":lag,
                    "lag_hours":(lag as f64 / 36.0).round() / 100.0,
                    "oldest_unconsumed_at":iso_ms(oldest),
                    "last_consumed_at":Value::Null
                }),
            ))
        })
        .collect();
    rows.sort_by(|a, b| b.0.cmp(&a.0));
    Ok(rows.into_iter().map(|(_, v)| v).collect())
}

/// `to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`.
pub(super) fn iso_ms(us: i64) -> String {
    let s = crate::rsm::planner::timers::iso_us(us.div_euclid(1000) * 1000);
    // iso_us renders six fractional digits: keep three.
    format!("{}Z", &s[..s.len() - 4])
}

/// Who reads a queue for a group, as every group row reports it: `kind`
/// (`kafka` for a group the Kafka facade's rows name, else `queen`), and for
/// a Kafka group whose facade reports presence ([`kafka_groups`])
/// `liveMembers` (members subscribed to the queue that heartbeat),
/// `lastSeenAt` and `inactiveSeconds` (0 while the queue is read for the
/// group, else seconds since it was last seen read, on this broker's clock;
/// null when nothing dates it). All three are null on a native group, which
/// has no membership to report: a native consumer is seen only through its
/// leases.
struct Readers {
    kind: &'static str,
    live: Option<kafka_groups::Liveness>,
}

impl Readers {
    fn of(
        kafka: &HashMap<String, kafka_groups::KafkaGroup>,
        group: &str,
        queue: &str,
        now: i64,
    ) -> Readers {
        match kafka.get(group) {
            Some(k) => Readers {
                kind: "kafka",
                live: k.on(queue, now),
            },
            None => Readers {
                kind: "queen",
                live: None,
            },
        }
    }

    fn insert_into(&self, row: &mut serde_json::Map<String, Value>, now: i64) {
        let live = self.live;
        row.insert("kind".into(), json!(self.kind));
        row.insert(
            "liveMembers".into(),
            live.map_or(Value::Null, |l| json!(l.live_members)),
        );
        row.insert(
            "lastSeenAt".into(),
            live.and_then(|l| l.last_seen_us)
                .map_or(Value::Null, |us| json!(iso_ms(us))),
        );
        row.insert(
            "inactiveSeconds".into(),
            live.and_then(|l| l.inactive_seconds(now))
                .map_or(Value::Null, |s| json!(s)),
        );
    }
}

/// `GET /api/v1/consumer-groups`: one row per `(group, queue)` with at least
/// one cursor, ordered by group then queue. `members` counts partition
/// cursors. `state` is `Lagging` past 300 s of time lag, else `Stable` once
/// anything was consumed, else `Dead` — except for a Kafka group whose facade
/// reports presence ([`Readers`]): a Kafka consumer commits positions and
/// consumes nothing the cursor counts, so its state is its membership —
/// `Lagging` or `Stable` by the same 300 s while the queue is read for it,
/// `Empty` (Kafka's word for a group with no members) when it is not.
fn group_view<R: Reads + ?Sized>(
    r: &R,
    qlog: Option<&QLogReader>,
    reader: &crate::rsm::segments::Reader,
    tenant: &str,
    now: i64,
) -> crate::rsm::store::Result<Vec<Value>> {
    #[derive(Default)]
    struct Agg {
        members: u64,
        with_lag: u64,
        total_lag: u64,
        max_time_lag: i64,
        max_consumed: u64,
    }
    let mut groups: std::collections::BTreeMap<(String, String), Agg> =
        std::collections::BTreeMap::new();
    for l in cursor_lags(r, qlog, reader, tenant, None, now)? {
        let a = groups
            .entry((l.group.clone(), l.queue.clone()))
            .or_default();
        a.members += 1;
        if l.pending > 0 {
            a.with_lag += 1;
        }
        a.total_lag += l.pending;
        a.max_time_lag = a.max_time_lag.max(l.lag_seconds(now).unwrap_or(0));
        a.max_consumed = a.max_consumed.max(l.total_consumed);
    }
    let kafka = kafka_groups::kafka_groups(r, tenant, groups.keys().map(|(g, _)| g.as_str()), now)?;
    let mut out = Vec::with_capacity(groups.len());
    for ((group, queue), a) in groups {
        let meta = if group == "__QUEUE_MODE__" {
            None
        } else {
            r.group(tenant, &queue, &group)?
        };
        let readers = Readers::of(&kafka, &group, &queue, now);
        let state = match readers.live {
            Some(l) if !l.live => "Empty",
            Some(_) if a.max_time_lag > 300 => "Lagging",
            Some(_) => "Stable",
            None if a.max_time_lag > 300 => "Lagging",
            None if a.max_consumed > 0 => "Stable",
            None => "Dead",
        };
        let mut row = json!({
            "name":group,
            "topics":[queue],
            "queueName":queue,
            "members":a.members,
            "partitionCursors":a.members,
            "partitionsWithLag":a.with_lag,
            "totalLag":a.total_lag,
            "maxTimeLag":a.max_time_lag,
            "state":state,
            "storage":"segments",
            "subscriptionMode":meta.as_ref().map(|m| format!("{:?}", m.meta.mode).to_lowercase()),
            "subscriptionTimestamp":meta.as_ref().map(|m| subscription_ts_json(m.meta.subscription_timestamp_us)),
            "subscriptionCreatedAt":meta.as_ref().map(|m| crate::rsm::planner::timers::iso_us(m.meta.registered_at_us)),
            "conflation":meta.as_ref().is_some_and(|m| m.meta.conflation)
        });
        if let Some(o) = row.as_object_mut() {
            readers.insert_into(o, now);
        }
        out.push(row);
    }
    Ok(out)
}

/// A trace a listing found: a legacy RAM row (catalogue version 1), already
/// decoded, or a row of the trace environment, whose body is read only if it
/// makes the page.
enum FoundTrace {
    Legacy(TraceEvent),
    Disk(crate::rsm::traces::TraceId),
}

/// The legacy RAM traces (catalogue version 1) of `tenant` — of one message
/// when `msg` names it, else all of the tenant's — that carry `name` when one
/// is given, in primary-key order: `cb(primary key, event)`. A prefix walk of
/// the tenant's rows, where the read before traces moved to disk walked every
/// tenant's.
fn legacy_traces<R: Reads + ?Sized>(
    r: &R,
    tenant: &str,
    msg: Option<(Pid, &str)>,
    name: Option<&str>,
    cb: &mut dyn FnMut(&[u8], TraceEvent),
) -> Result<(), crate::rsm::store::StoreError> {
    let prefix = match msg {
        Some((pid, txn)) => keys::trace_prefix(tenant, Some(pid), txn),
        None => keys::queues_prefix(tenant),
    };
    let mut err = None;
    r.scan_raw(
        Keyspace::Traces,
        &prefix,
        &prefix,
        usize::MAX,
        &mut |k, v| match rows::trace_decode(v) {
            Ok(e) => {
                if e.tenant == tenant
                    && msg.is_none_or(|(p, t)| e.pid == Some(p) && e.txn == t)
                    && name.is_none_or(|n| e.names.iter().any(|x| x == n))
                {
                    cb(k, e);
                }
                true
            }
            Err(e) => {
                err = Some(crate::rsm::store::StoreError::corrupt(
                    Keyspace::Traces,
                    format!("{e}"),
                ));
                false
            }
        },
    )?;
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// One trace event's fields, its `data` copied in as stored
/// (`trace_json` before 2.6.1 parsed it into a tree).
fn trace_fields(e: &TraceEvent, location: Option<(String, String)>) -> Vec<(&'static str, Field<'_>)> {
    let trace_id = json!(uuid_bytes_to_string(&e.trace_id));
    let partition_id = json!(e.pid.map(|p| p.to_string()));
    let message_id = json!(e.message_id.map(|x| uuid_bytes_to_string(&x)));
    let created_at = json!(crate::rsm::planner::timers::iso_us(e.created_at_us));
    let (queue_name, partition_name) = location
        .map(|(q, p)| (Some(q), Some(p)))
        .unwrap_or((None, None));
    vec![
        ("traceId", Field::Json(trace_id.clone())),
        ("trace_id", Field::Json(trace_id)),
        ("partitionId", Field::Json(partition_id.clone())),
        ("partition_id", Field::Json(partition_id)),
        ("messageId", Field::Json(message_id.clone())),
        ("message_id", Field::Json(message_id)),
        ("transactionId", Field::Json(json!(e.txn))),
        ("transaction_id", Field::Json(json!(e.txn))),
        ("consumerGroup", Field::Json(json!(e.consumer_group))),
        ("consumer_group", Field::Json(json!(e.consumer_group))),
        ("eventType", Field::Json(json!(e.event_type))),
        ("event_type", Field::Json(json!(e.event_type))),
        ("data", Field::Doc(&e.data)),
        ("worker", Field::Json(json!(e.worker))),
        ("worker_id", Field::Json(json!(e.worker))),
        ("names", Field::Json(json!(e.names))),
        ("trace_names", Field::Json(json!(e.names))),
        ("createdAt", Field::Json(created_at.clone())),
        ("created_at", Field::Json(created_at)),
        ("queue_name", Field::Json(json!(queue_name))),
        ("partition_name", Field::Json(json!(partition_name))),
        ("message_payload", Field::Json(Value::Null)),
    ]
}

/// Queues per discovery call. Above it the whole call is refused rather than
/// cut short: a dropped entry would leave its caller waiting on a queue
/// nobody looked at.
const CHANGED_MAX_ENTRIES: usize = 1024;

/// Partitions per discovery page: the default, and the ceiling `limit` is
/// clamped to (never refused).
const CHANGED_MAX_LIMIT: usize = 1000;

/// One discovery page of `ask.queue`: its partitions in pid order (creation
/// order), the first `limit` after the cursor's pid that pass `since`
/// (`last_write_at_us >= since`), and the cursor of the next page.
///
/// Walks the queue's pid index ([`TypedReads::scan_queue_partitions`]) from
/// just past the cursor and stops at the page's last match, so a COMPLETE pass
/// — every page of one ask until `next` is `None` — reads each partition row
/// of the queue once, with or without `since`: one scan per pass, whatever the
/// page size, where sorting every partition on every page cost
/// O(partitions × pages). The answer is O(what matched). A pid only grows, so
/// a partition created during a pass is past the cursor and the pass still
/// meets it; none is ever behind it. Partitions being deleted (in the garbage
/// set) are skipped, as every reader skips them.
///
/// The cursor is `p|<pid of the page's last partition>`, issued exactly when
/// the page is full. It is opaque to the caller: anything else — the 1.x
/// `n|name` and `t|micros|name` ones included — is `BAD_CURSOR`, after the
/// unknown queue check.
fn changed_page<R: Reads + ?Sized>(
    r: &R,
    tenant: &str,
    ask: &ChangedAsk,
) -> crate::rsm::store::Result<ChangedAnswer> {
    if r.queue(tenant, &ask.queue)?.is_none() {
        return Ok(ChangedAnswer {
            error: Some("UNKNOWN_TOPIC_OR_PARTITION"),
            ..ChangedAnswer::default()
        });
    }
    let after = match parse_changed_cursor(ask.after.as_deref()) {
        Ok(after) => after,
        Err(error) => {
            return Ok(ChangedAnswer {
                error: Some(error),
                ..ChangedAnswer::default()
            })
        }
    };
    let from = match after {
        None => None,
        Some(pid) => match pid.checked_add(1) {
            Some(next) => Some(next),
            // Nothing can come after the last pid there is.
            None => return Ok(ChangedAnswer::default()),
        },
    };
    let limit = ask.limit.clamp(1, CHANGED_MAX_LIMIT);
    let mut partitions = Vec::new();
    let mut last: Pid = 0;
    let mut failed = None;
    r.scan_queue_partitions(tenant, &ask.queue, from, usize::MAX, &mut |pid| {
        let row = match r.is_garbage(pid) {
            Ok(true) => Ok(None),
            Ok(false) => r.partition(pid),
            Err(e) => Err(e),
        };
        match row {
            Ok(Some(p)) => {
                if ask.since_us.is_none_or(|s| p.last_write_at_us >= s) {
                    partitions.push(ChangedPartition {
                        id: uuid_bytes_to_string(&p.uuid),
                        name: p.partition,
                        last_offset: p.last_offset,
                        log_start: p.log_start,
                        last_write_at_us: p.last_write_at_us,
                    });
                    last = pid;
                }
                partitions.len() < limit
            }
            Ok(None) => true,
            Err(e) => {
                failed = Some(e);
                false
            }
        }
    })?;
    if let Some(e) = failed {
        return Err(e);
    }
    let next = (partitions.len() >= limit).then(|| format!("p|{last}"));
    Ok(ChangedAnswer {
        partitions,
        next,
        error: None,
    })
}

/// The pid a discovery page resumes after: `None` with no cursor (absent or
/// `""`), the pid of a `p|<decimal pid>` cursor, `BAD_CURSOR` for anything
/// else.
fn parse_changed_cursor(raw: Option<&str>) -> Result<Option<Pid>, &'static str> {
    const BAD: &str = "BAD_CURSOR";
    match raw {
        None | Some("") => Ok(None),
        Some(s) => {
            let digits = s.strip_prefix("p|").ok_or(BAD)?;
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(BAD);
            }
            digits.parse::<Pid>().map(Some).map_err(|_| BAD)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::pct_decode;

    #[test]
    fn a_segment_decodes_to_the_name_it_encodes() {
        assert_eq!(pct_decode("orders-consumer"), "orders-consumer");
        assert_eq!(pct_decode("laravel%3Adefault"), "laravel:default");
        assert_eq!(pct_decode("a%2Fb"), "a/b");
        assert_eq!(pct_decode("a%20b%3Ac"), "a b:c");
        assert_eq!(pct_decode("%C3%A9t%C3%A9"), "été");
        assert_eq!(pct_decode("50%2525"), "50%25", "decoded once");
        assert_eq!(pct_decode("a+b"), "a+b", "`+` is a space only in a query");
        assert_eq!(pct_decode("100%"), "100%", "a trailing % is kept");
        assert_eq!(pct_decode("%zz%41"), "%zzA", "a bad escape is kept");
        assert_eq!(pct_decode("%+5x"), "%+5x", "a sign is not a hex digit");
    }
}

/// How long a parked fetch waits at least before it answers because its topic
/// was written elsewhere on this node ([`RaftFacade::fetch_wait`]):
/// `QUEEN_FETCH_TOPIC_WAKE_MS`, default 5 ms; `0` parks a fetch on its own
/// partitions only, as before 2026-10-17.
fn topic_wake_floor() -> std::time::Duration {
    static MS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    std::time::Duration::from_millis(*MS.get_or_init(|| {
        std::env::var("QUEEN_FETCH_TOPIC_WAKE_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map_or(5, |v| v.min(30_000))
    }))
}
