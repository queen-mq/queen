//! The pop → process → commit loop, and the idle-flush sweep.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::Value;

use queen_protocol::{
    CycleAck, CycleRequest, CycleResponse, Message, RegisterRequest, RegisterResponse, SinkPushItem,
    StateGetRequest, StateGetResponse, StateOp, SubscriptionMode,
};

use super::engine::{self, Emit, Envelope};
use super::ops::{EmitCtx, GateCtx, Op, Record, Sink};
use super::{Stages, Terminal};
use crate::consumer::Cancel;
use crate::error::{Error, Result};
use crate::http::Opts;
use crate::inner::Inner;
use crate::queue::QueueBuilder;

/// How long a partition stays eligible for the idle-flush sweep after we last
/// saw it. Beyond this it is assumed to belong to another worker, and
/// speculatively flushing it would race that worker's own cycles.
const PARTITION_RECENCY: Duration = Duration::from_secs(300);

/// What a running stream has done so far.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamMetrics {
    pub cycles: u64,
    pub flush_cycles: u64,
    pub messages: u64,
    pub push_items: u64,
    pub state_ops: u64,
    pub late_events: u64,
    pub gate_allowed: u64,
    pub gate_denied: u64,
    pub errors: u64,
}

#[derive(Default)]
struct Counters {
    cycles: AtomicU64,
    flush_cycles: AtomicU64,
    messages: AtomicU64,
    push_items: AtomicU64,
    state_ops: AtomicU64,
    late_events: AtomicU64,
    gate_allowed: AtomicU64,
    gate_denied: AtomicU64,
    errors: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> StreamMetrics {
        StreamMetrics {
            cycles: self.cycles.load(Ordering::Relaxed),
            flush_cycles: self.flush_cycles.load(Ordering::Relaxed),
            messages: self.messages.load(Ordering::Relaxed),
            push_items: self.push_items.load(Ordering::Relaxed),
            state_ops: self.state_ops.load(Ordering::Relaxed),
            late_events: self.late_events.load(Ordering::Relaxed),
            gate_allowed: self.gate_allowed.load(Ordering::Relaxed),
            gate_denied: self.gate_denied.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

/// How a stream runs.
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// The durable identity of the query. Two processes sharing it share state
    /// and cursor, which is how a stream scales out.
    pub query_id: String,
    pub batch_size: i32,
    /// Partitions claimed per poll.
    pub max_partitions: i32,
    pub max_wait: Duration,
    pub subscription_mode: Option<SubscriptionMode>,
    pub subscription_from: Option<String>,
    /// Wipe existing state when the chain's shape changed. Without it a changed
    /// chain is refused, which is the point.
    pub reset: bool,
    /// Defaults to `streams.{query_id}`.
    pub consumer_group: Option<String>,
    pub cancel: Option<Cancel>,
}

impl RunOptions {
    pub fn new(query_id: impl Into<String>) -> Self {
        Self {
            query_id: query_id.into(),
            batch_size: 200,
            max_partitions: 4,
            max_wait: Duration::from_millis(1000),
            subscription_mode: None,
            subscription_from: None,
            reset: false,
            consumer_group: None,
            cancel: None,
        }
    }

    pub fn batch_size(mut self, n: i32) -> Self {
        self.batch_size = n.max(1);
        self
    }

    pub fn max_partitions(mut self, n: i32) -> Self {
        self.max_partitions = n.max(1);
        self
    }

    pub fn max_wait(mut self, d: Duration) -> Self {
        self.max_wait = d;
        self
    }

    pub fn reset(mut self, enabled: bool) -> Self {
        self.reset = enabled;
        self
    }

    pub fn consumer_group(mut self, group: impl Into<String>) -> Self {
        self.consumer_group = Some(group.into());
        self
    }

    pub fn cancel(mut self, token: Cancel) -> Self {
        self.cancel = Some(token);
        self
    }

    pub fn subscription_mode(mut self, mode: SubscriptionMode) -> Self {
        self.subscription_mode = Some(mode);
        self
    }

    pub fn subscription_from(mut self, from: impl Into<String>) -> Self {
        self.subscription_from = Some(from.into());
        self
    }
}

pub(crate) struct Runner {
    inner: Arc<Inner>,
    source: QueueBuilder,
    stages: Stages,
    opts: RunOptions,
    consumer_group: String,
    /// The id the broker assigned, distinct from the caller's `query_id`.
    server_query_id: String,
    stopped: AtomicBool,
    counters: Counters,
    /// One lock per partition. The cycle loop and the flush sweep both do
    /// read-state → compute → commit against the same rows; the broker only
    /// serializes the *commits*, so without this a flush could read a window,
    /// have the loop emit and delete it, and then emit it a second time.
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    recent: Mutex<HashMap<String, (String, Instant)>>,
    watermarks: Mutex<HashMap<String, i64>>,
}

/// A running stream.
pub struct StreamHandle {
    runner: Arc<Runner>,
    loop_task: Option<tokio::task::JoinHandle<()>>,
    flush_task: Option<tokio::task::JoinHandle<()>>,
}

impl std::fmt::Debug for StreamHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamHandle")
            .field("query_id", &self.runner.server_query_id)
            .field("metrics", &self.metrics())
            .finish()
    }
}

impl StreamHandle {
    /// Stop and wait for the in-flight cycle and flush to finish.
    pub async fn stop(mut self) -> Result<()> {
        self.runner.stopped.store(true, Ordering::SeqCst);
        if let Some(t) = self.flush_task.take() {
            t.abort();
            let _ = t.await;
        }
        if let Some(t) = self.loop_task.take() {
            let _ = t.await;
        }
        Ok(())
    }

    pub fn metrics(&self) -> StreamMetrics {
        self.runner.counters.snapshot()
    }

    /// The broker-assigned query id.
    pub fn query_id(&self) -> &str {
        &self.runner.server_query_id
    }
}

pub(crate) async fn start(
    inner: Arc<Inner>,
    source: QueueBuilder,
    stages: Stages,
    config_hash: String,
    opts: RunOptions,
) -> Result<StreamHandle> {
    let source_queue = source
        .name()
        .ok_or_else(|| Error::Invalid("a stream needs a named source queue".into()))?
        .to_string();
    let sink_queue = match &stages.terminal {
        Some(Terminal::Sink(s)) => Some(s.queue.clone()),
        _ => None,
    };

    let reg = RegisterRequest {
        name: opts.query_id.clone(),
        source_queue,
        sink_queue,
        config_hash,
        reset: opts.reset,
    };

    let registered: RegisterResponse = match inner
        .http
        .post_json("/streams/v1/queries", &reg, &Opts::default())
        .await
    {
        Ok(Some(r)) => r,
        Ok(None) => return Err(Error::Decode("register returned an empty body".into())),
        Err(e) if e.status() == Some(409) => {
            return Err(Error::Invalid(format!(
                "this query is registered with a different operator chain ({e}). Its stored state \
                 was computed by the old shape, so reusing it would silently mix the two. Pass \
                 RunOptions::reset(true) to wipe that state, or run under a new query id."
            )))
        }
        Err(e) => return Err(e),
    };
    if !registered.success {
        return Err(Error::Invalid(format!(
            "stream registration failed: {}",
            registered.error.as_deref().unwrap_or("no reason given")
        )));
    }

    let consumer_group = opts
        .consumer_group
        .clone()
        .unwrap_or_else(|| format!("streams.{}", opts.query_id));

    let idle_flush = stages
        .window
        .as_ref()
        .map(|w| w.idle_flush_ms)
        .filter(|ms| *ms > 0);

    let runner = Arc::new(Runner {
        inner,
        source,
        stages,
        opts,
        consumer_group,
        server_query_id: registered.query_id,
        stopped: AtomicBool::new(false),
        counters: Counters::default(),
        locks: Mutex::new(HashMap::new()),
        recent: Mutex::new(HashMap::new()),
        watermarks: Mutex::new(HashMap::new()),
    });

    let loop_runner = Arc::clone(&runner);
    let loop_task = tokio::spawn(async move { loop_runner.run_loop().await });

    let flush_task = idle_flush.map(|ms| {
        let r = Arc::clone(&runner);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(ms as u64));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                if r.is_stopped() {
                    break;
                }
                r.flush_tick().await;
            }
        })
    });

    Ok(StreamHandle {
        runner,
        loop_task: Some(loop_task),
        flush_task,
    })
}

struct PartitionGroup {
    partition_id: String,
    partition: String,
    lease_id: String,
    consumer_group: String,
    messages: Vec<Message>,
}

impl Runner {
    fn is_stopped(&self) -> bool {
        if self.stopped.load(Ordering::SeqCst) {
            return true;
        }
        if self.opts.cancel.as_ref().is_some_and(|c| c.is_cancelled()) {
            self.stopped.store(true, Ordering::SeqCst);
            return true;
        }
        false
    }

    async fn run_loop(self: Arc<Self>) {
        while !self.is_stopped() {
            let popped = match self.pop().await {
                Ok(m) => m,
                Err(e) => {
                    self.counters.errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(error = %e, "stream pop failed");
                    if e.is_terminal_refusal() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };
            if popped.is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            for group in group_by_partition(popped, &self.consumer_group) {
                if self.is_stopped() {
                    break;
                }
                self.touch(&group.partition_id, &group.partition);
                let n = group.messages.len() as u64;
                match self.cycle(group).await {
                    Ok(()) => {
                        self.counters.cycles.fetch_add(1, Ordering::Relaxed);
                        self.counters.messages.fetch_add(n, Ordering::Relaxed);
                    }
                    Err(e) => {
                        self.counters.errors.fetch_add(1, Ordering::Relaxed);
                        // Nothing committed, so the lease lapses and the broker
                        // redelivers. That is the whole fault model.
                        tracing::error!(error = %e, "stream cycle failed; batch will be redelivered");
                    }
                }
            }
        }
    }

    async fn pop(&self) -> Result<Vec<Message>> {
        let mut b = self
            .source
            .clone()
            .batch(self.opts.batch_size)
            .partitions(self.opts.max_partitions)
            .wait(true)
            .poll_timeout(self.opts.max_wait)
            .group(&self.consumer_group);
        if let Some(m) = self.opts.subscription_mode {
            b = b.subscription_mode(m);
        }
        if let Some(f) = &self.opts.subscription_from {
            b = b.subscription_from(f.clone());
        }
        b.pop().await
    }

    fn touch(&self, partition_id: &str, partition: &str) {
        if partition_id.is_empty() {
            return;
        }
        let mut recent = self.recent.lock().unwrap();
        recent.insert(
            partition_id.to_string(),
            (partition.to_string(), Instant::now()),
        );
        recent.retain(|_, (_, at)| at.elapsed() < PARTITION_RECENCY);
    }

    fn lock_for(&self, partition_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.locks.lock().unwrap();
        Arc::clone(
            locks
                .entry(partition_id.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    // ------------------------------------------------------------ one cycle

    async fn cycle(&self, mut group: PartitionGroup) -> Result<()> {
        let lock = self.lock_for(&group.partition_id);
        let _guard = lock.lock().await;

        // Partition FIFO order, which is (createdAt, id) — a multi-message
        // push shares a createdAt, so the id breaks the tie deterministically.
        let mut messages = std::mem::take(&mut group.messages);
        messages.sort_by(|a, b| {
            let ta = super::ops::parse_iso_ms(&a.created_at).unwrap_or(0);
            let tb = super::ops::parse_iso_ms(&b.created_at).unwrap_or(0);
            ta.cmp(&tb).then_with(|| a.id.cmp(&b.id))
        });

        // 1. Pre-reducer stateless ops.
        let mut records: Vec<(Record, String)> = messages
            .iter()
            .map(|m| {
                let key = m.partition_id.clone();
                (Record::from_message(m.clone()), key)
            })
            .collect();
        records = apply_stateless(&self.stages.pre, records)?;

        // 2. keyBy override.
        if let Some(f) = &self.stages.key_by {
            for (rec, key) in records.iter_mut() {
                *key = f(rec);
            }
        }

        if self.stages.gate.is_some() {
            return self.gate_cycle(records, &messages, &group).await;
        }

        // 3. Window + reduce, or straight through.
        let (state_ops, emits) = match (&self.stages.window, &self.stages.reducer) {
            (Some(window), Some(reducer)) => {
                let loaded = self.load_state(&group.partition_id, None, None).await?;
                self.windowed(window, reducer, records, &messages, &group.partition_id, &loaded)?
            }
            (Some(window), None) => {
                // A window with no reducer only annotates; every record passes
                // through with its window metadata.
                let mut emits = Vec::new();
                for (rec, key) in records {
                    let Some(ts) = window.timestamp_of_record(&rec) else {
                        return Err(Error::Invalid(
                            "a windowed stream could not determine a message's timestamp".into(),
                        ));
                    };
                    for env in window.annotate(rec, key, ts) {
                        emits.push(Emit {
                            key: env.key,
                            window_start: env.window_start,
                            window_end: env.window_end,
                            window_key: env.window_key,
                            value: env.record.data,
                        });
                    }
                }
                (Vec::new(), emits)
            }
            (None, _) => (
                Vec::new(),
                records
                    .into_iter()
                    .map(|(rec, key)| Emit {
                        key,
                        window_start: 0,
                        window_end: 0,
                        window_key: String::new(),
                        value: rec.data,
                    })
                    .collect(),
            ),
        };

        // 4. Post-reducer ops, then the terminal.
        let emits = self.apply_post(emits, &group.partition, &group.partition_id)?;
        let push_items = self
            .terminal(&emits, &group.partition, &group.partition_id)
            .await?;

        // 5. Ack the whole batch: the cycle is atomic across it.
        let ack = messages.last().map(|last| CycleAck {
            transaction_id: last.transaction_id.clone(),
            lease_id: if last.lease_id.is_empty() {
                group.lease_id.clone()
            } else {
                last.lease_id.clone()
            },
            status: "completed".into(),
            count: messages.len() as i64,
        });

        self.commit(
            &group.partition_id,
            &group.consumer_group,
            state_ops,
            push_items,
            ack,
            true,
        )
        .await
    }

    fn windowed(
        &self,
        window: &super::ops::Window,
        reducer: &super::ops::Reducer,
        records: Vec<(Record, String)>,
        messages: &[Message],
        partition_id: &str,
        loaded: &BTreeMap<String, Value>,
    ) -> Result<(Vec<StateOp>, Vec<Emit>)> {
        let event_time = window.event_time.is_some();

        // Restore the watermark before filtering, so a restart does not accept
        // events it had already decided were late.
        if event_time {
            if let Some(wm) = engine::watermark_of(loaded) {
                self.watermarks
                    .lock()
                    .unwrap()
                    .insert(partition_id.to_string(), wm);
            }
        }
        let watermark = self.watermarks.lock().unwrap().get(partition_id).copied();

        let mut envelopes: Vec<Envelope> = Vec::new();
        let mut max_event_time = i64::MIN;
        let mut dropped_late = 0u64;

        for (rec, key) in records {
            let Some(ts) = window.timestamp_of_record(&rec) else {
                return Err(Error::Invalid(
                    "a windowed stream could not determine a message's timestamp — an event-time \
                     extractor returned nothing, or the message carries no createdAt"
                        .into(),
                ));
            };
            if event_time {
                max_event_time = max_event_time.max(ts);
                // The stored watermark already has the lateness allowance
                // subtracted, so this compares directly.
                if watermark.is_some_and(|wm| ts < wm) {
                    if window.on_late == super::ops::LatePolicy::Drop {
                        dropped_late += 1;
                        continue;
                    }
                }
            }
            envelopes.extend(window.annotate(rec, key, ts));
        }
        if dropped_late > 0 {
            self.counters
                .late_events
                .fetch_add(dropped_late, Ordering::Relaxed);
        }

        // The close trigger.
        let clock = if event_time {
            let advanced = watermark
                .unwrap_or(i64::MIN)
                .max(if max_event_time > i64::MIN {
                    max_event_time - window.allowed_lateness_ms()
                } else {
                    i64::MIN
                });
            self.watermarks
                .lock()
                .unwrap()
                .insert(partition_id.to_string(), advanced);
            Some(advanced)
        } else {
            messages
                .iter()
                .filter_map(|m| super::ops::parse_iso_ms(&m.created_at))
                .max()
        };

        let mut outcome = if window.is_session() {
            let now = clock.unwrap_or_else(now_ms);
            engine::run_session(window, reducer, &envelopes, loaded, now)
        } else {
            engine::run_reduce(
                reducer,
                &envelopes,
                loaded,
                clock,
                &window.tag(),
                window.grace_ms(),
            )
        };

        if event_time {
            if let Some(wm) = self.watermarks.lock().unwrap().get(partition_id) {
                if *wm > i64::MIN {
                    outcome.state_ops.push(engine::watermark_op(*wm));
                }
            }
        }

        Ok((outcome.state_ops, outcome.emits))
    }

    // -------------------------------------------------------------- gate

    async fn gate_cycle(
        &self,
        records: Vec<(Record, String)>,
        messages: &[Message],
        group: &PartitionGroup,
    ) -> Result<()> {
        let Some(gate) = &self.stages.gate else {
            return Ok(());
        };
        let loaded = self.load_state(&group.partition_id, None, None).await?;

        let mut live: BTreeMap<String, Value> = BTreeMap::new();
        let mut touched: Vec<String> = Vec::new();
        let stream_time = now_ms();

        let mut allowed: Vec<(Record, String)> = Vec::new();
        let mut denied = false;

        for (rec, key) in records {
            engine::check_user_key(&key)?;
            let entry = live
                .entry(key.clone())
                .or_insert_with(|| loaded.get(&key).cloned().unwrap_or(serde_json::json!({})));

            let mut ctx = GateCtx {
                state: entry,
                stream_time_ms: stream_time,
                partition_id: &group.partition_id,
                partition: &group.partition,
                key: &key,
            };
            if gate(&rec, &mut ctx) {
                if !touched.contains(&key) {
                    touched.push(key.clone());
                }
                allowed.push((rec, key));
            } else {
                denied = true;
                break;
            }
        }

        let allowed_count = allowed.len();
        if allowed_count == 0 {
            // Commit nothing. The lease runs out on its own and the batch comes
            // back in the same order — which is the whole point of the gate:
            // back-pressure without a deferred queue and without reordering.
            self.counters
                .gate_denied
                .fetch_add(messages.len() as u64, Ordering::Relaxed);
            return Ok(());
        }

        let state_ops: Vec<StateOp> = touched
            .into_iter()
            .map(|key| StateOp::Upsert {
                value: live.get(&key).cloned().unwrap_or(serde_json::json!({})),
                key,
            })
            .collect();

        let emits: Vec<Emit> = allowed
            .iter()
            .map(|(rec, key)| Emit {
                key: key.clone(),
                window_start: 0,
                window_end: 0,
                window_key: String::new(),
                value: rec.data.clone(),
            })
            .collect();
        let emits = self.apply_post(emits, &group.partition, &group.partition_id)?;
        let push_items = self
            .terminal(&emits, &group.partition, &group.partition_id)
            .await?;

        let last_allowed = &messages[allowed_count - 1];
        let ack = CycleAck {
            transaction_id: last_allowed.transaction_id.clone(),
            lease_id: if last_allowed.lease_id.is_empty() {
                group.lease_id.clone()
            } else {
                last_allowed.lease_id.clone()
            },
            status: "completed".into(),
            count: allowed_count as i64,
        };

        self.counters
            .gate_allowed
            .fetch_add(allowed_count as u64, Ordering::Relaxed);
        if denied {
            self.counters
                .gate_denied
                .fetch_add((messages.len() - allowed_count) as u64, Ordering::Relaxed);
        }

        // Holding the lease on a partial ack is what preserves FIFO: the tail
        // is not claimable by another worker, so it cannot overtake.
        self.commit(
            &group.partition_id,
            &group.consumer_group,
            state_ops,
            push_items,
            Some(ack),
            !denied,
        )
        .await
    }

    // --------------------------------------------------------- idle flush

    async fn flush_tick(&self) {
        let Some(window) = &self.stages.window else {
            return;
        };
        let partitions: Vec<(String, String)> = {
            let recent = self.recent.lock().unwrap();
            recent
                .iter()
                .map(|(pid, (name, _))| (pid.clone(), name.clone()))
                .collect()
        };
        for (partition_id, partition) in partitions {
            if self.is_stopped() {
                break;
            }
            if let Err(e) = self.flush_partition(window, &partition_id, &partition).await {
                self.counters.errors.fetch_add(1, Ordering::Relaxed);
                tracing::error!(partition_id, error = %e, "stream idle flush failed");
            }
        }
    }

    async fn flush_partition(
        &self,
        window: &super::ops::Window,
        partition_id: &str,
        partition: &str,
    ) -> Result<()> {
        let lock = self.lock_for(partition_id);
        let _guard = lock.lock().await;

        // In event-time mode the watermark only moves when events arrive, so a
        // silent partition must not be swept with the wall clock — that would
        // close windows the data has not reached yet.
        let clock = if window.event_time.is_some() {
            match self.watermarks.lock().unwrap().get(partition_id).copied() {
                Some(wm) => wm,
                None => return Ok(()),
            }
        } else {
            now_ms()
        };

        let Some(reducer) = &self.stages.reducer else {
            return Ok(());
        };

        let outcome = if window.is_session() {
            let loaded = self.load_state(partition_id, None, None).await?;
            let out = engine::run_session(window, reducer, &[], &loaded, clock);
            if out.emits.is_empty() {
                return Ok(());
            }
            out
        } else {
            let ripe_at = clock - window.grace_ms();
            let prefix = format!("{}{}", window.tag(), queen_protocol::STATE_KEY_SEP);
            let loaded = self
                .load_state(partition_id, Some(prefix), Some(ripe_at))
                .await?;
            if loaded.is_empty() {
                return Ok(());
            }
            // Everything the broker returned is ripe by construction, so it all
            // closes.
            engine::run_reduce(reducer, &[], &loaded, Some(clock), &window.tag(), window.grace_ms())
        };

        if outcome.emits.is_empty() {
            return Ok(());
        }

        let emits = self.apply_post(outcome.emits, partition, partition_id)?;
        let push_items = self.terminal(&emits, partition, partition_id).await?;

        self.counters.flush_cycles.fetch_add(1, Ordering::Relaxed);
        // No ack: a flush consumes no source messages.
        self.commit(
            partition_id,
            &self.consumer_group,
            outcome.state_ops,
            push_items,
            None,
            true,
        )
        .await
    }

    // ------------------------------------------------------------- shared

    fn apply_post(
        &self,
        emits: Vec<Emit>,
        partition: &str,
        partition_id: &str,
    ) -> Result<Vec<Emit>> {
        if self.stages.post.is_empty() || emits.is_empty() {
            return Ok(emits);
        }
        let mut working = emits;
        for op in &self.stages.post {
            let mut next = Vec::with_capacity(working.len());
            for e in working {
                let ctx = emit_ctx(&e, partition, partition_id);
                let rec = Record {
                    data: e.value.clone(),
                    message: None,
                    ctx: Some(ctx),
                };
                match op {
                    Op::Map(f) => next.push(Emit {
                        value: f(&rec),
                        ..e
                    }),
                    Op::Filter(f) => {
                        if f(&rec) {
                            next.push(e);
                        }
                    }
                    Op::FlatMap(f) => {
                        for value in f(&rec) {
                            next.push(Emit {
                                value,
                                key: e.key.clone(),
                                window_key: e.window_key.clone(),
                                ..e
                            });
                        }
                    }
                    other => {
                        return Err(Error::Invalid(format!(
                            "operator '{}' cannot run after a reducer",
                            other.kind()
                        )))
                    }
                }
            }
            working = next;
        }
        Ok(working)
    }

    async fn terminal(
        &self,
        emits: &[Emit],
        partition: &str,
        partition_id: &str,
    ) -> Result<Vec<SinkPushItem>> {
        match &self.stages.terminal {
            Some(Terminal::Sink(sink)) => Ok(build_push_items(sink, emits, partition)),
            Some(Terminal::Foreach(f)) => {
                for e in emits {
                    let ctx = emit_ctx(e, partition, partition_id);
                    // Effects run BEFORE the ack commits, so a failure here
                    // redelivers rather than silently losing the side effect.
                    f(e.value.clone(), ctx)
                        .await
                        .map_err(|m| Error::Invalid(format!("foreach failed: {m}")))?;
                }
                Ok(Vec::new())
            }
            None => Ok(Vec::new()),
        }
    }

    async fn load_state(
        &self,
        partition_id: &str,
        key_prefix: Option<String>,
        ripe_at_or_before: Option<i64>,
    ) -> Result<BTreeMap<String, Value>> {
        let req = StateGetRequest {
            query_id: self.server_query_id.clone(),
            partition_id: partition_id.to_string(),
            keys: Vec::new(),
            key_prefix,
            ripe_at_or_before,
        };
        let resp: Option<StateGetResponse> = self
            .inner
            .http
            .post_json("/streams/v1/state/get", &req, &Opts::default())
            .await?;
        let resp = resp.ok_or_else(|| Error::Decode("state get returned an empty body".into()))?;
        if !resp.success {
            return Err(Error::Invalid(format!(
                "state get failed: {}",
                resp.error.as_deref().unwrap_or("no reason given")
            )));
        }
        Ok(resp.rows.into_iter().map(|r| (r.key, r.value)).collect())
    }

    async fn commit(
        &self,
        partition_id: &str,
        consumer_group: &str,
        state_ops: Vec<StateOp>,
        push_items: Vec<SinkPushItem>,
        ack: Option<CycleAck>,
        release_lease: bool,
    ) -> Result<()> {
        self.counters
            .state_ops
            .fetch_add(state_ops.len() as u64, Ordering::Relaxed);
        self.counters
            .push_items
            .fetch_add(push_items.len() as u64, Ordering::Relaxed);

        let req = CycleRequest {
            query_id: self.server_query_id.clone(),
            partition_id: partition_id.to_string(),
            consumer_group: consumer_group.to_string(),
            state_ops,
            push_items,
            ack,
            release_lease,
        };
        let resp: Option<CycleResponse> = self
            .inner
            .http
            .post_json("/streams/v1/cycle", &req, &Opts::default())
            .await?;
        let resp = resp.ok_or_else(|| Error::Decode("cycle returned an empty body".into()))?;
        if !resp.success {
            // A rolled-back cycle is still HTTP 200; treating it as success
            // would mean believing state advanced when it did not.
            return Err(Error::Invalid(format!(
                "stream cycle rolled back: {}",
                resp.error.as_deref().unwrap_or("no reason given")
            )));
        }
        Ok(())
    }
}

impl super::ops::Window {
    /// Timestamp for a record that may or may not still carry its message.
    pub(crate) fn timestamp_of_record(&self, rec: &Record) -> Option<i64> {
        let msg = rec.message.as_ref()?;
        self.timestamp_of(msg)
    }
}

pub(crate) fn build_push_items(
    sink: &Sink,
    emits: &[Emit],
    source_partition: &str,
) -> Vec<SinkPushItem> {
    emits
        .iter()
        .map(|e| SinkPushItem {
            queue: sink.queue.clone(),
            partition: Some(sink.resolve_partition(&e.value, source_partition)),
            // A scalar cannot be a message payload on its own, so wrap it the
            // same way the other SDKs do rather than pushing a bare number.
            payload: if e.value.is_object() {
                e.value.clone()
            } else {
                serde_json::json!({ "value": e.value })
            },
            message_id: None,
            transaction_id: None,
        })
        .collect()
}

pub(crate) fn emit_ctx(e: &Emit, partition: &str, partition_id: &str) -> EmitCtx {
    EmitCtx {
        partition: partition.to_string(),
        partition_id: partition_id.to_string(),
        key: e.key.clone(),
        window_key: (!e.window_key.is_empty()).then(|| e.window_key.clone()),
        window_start: (e.window_end != 0).then_some(e.window_start),
        window_end: (e.window_end != 0).then_some(e.window_end),
    }
}

fn apply_stateless(ops: &[Op], mut records: Vec<(Record, String)>) -> Result<Vec<(Record, String)>> {
    for op in ops {
        let mut next = Vec::with_capacity(records.len());
        for (rec, key) in records {
            match op {
                Op::Map(f) => {
                    let data = f(&rec);
                    next.push((Record { data, ..rec }, key));
                }
                Op::Filter(f) => {
                    if f(&rec) {
                        next.push((rec, key));
                    }
                }
                Op::FlatMap(f) => {
                    for data in f(&rec) {
                        next.push((
                            Record {
                                data,
                                message: rec.message.clone(),
                                ctx: rec.ctx.clone(),
                            },
                            key.clone(),
                        ));
                    }
                }
                other => {
                    return Err(Error::Invalid(format!(
                        "operator '{}' is not a stateless stage",
                        other.kind()
                    )))
                }
            }
        }
        records = next;
    }
    Ok(records)
}

fn group_by_partition(messages: Vec<Message>, fallback_group: &str) -> Vec<PartitionGroup> {
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, PartitionGroup> = HashMap::new();
    for m in messages {
        let pid = m.partition_id.clone();
        let entry = groups.entry(pid.clone()).or_insert_with(|| {
            order.push(pid.clone());
            PartitionGroup {
                partition_id: pid,
                partition: m.partition.clone(),
                lease_id: m.lease_id.clone(),
                consumer_group: if m.consumer_group.is_empty() {
                    fallback_group.to_string()
                } else {
                    m.consumer_group.clone()
                },
                messages: Vec::new(),
            }
        });
        entry.messages.push(m);
    }
    order
        .into_iter()
        .filter_map(|pid| groups.remove(&pid))
        .collect()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streams::ops::{SinkPartition, SinkPartition::Source};
    use std::sync::Arc;

    fn message(pid: &str, partition: &str, id: &str, created: &str) -> Message {
        Message {
            id: id.into(),
            transaction_id: format!("txn-{id}"),
            trace_id: None,
            data: serde_json::json!({ "n": 1 }),
            producer_sub: None,
            created_at: created.into(),
            partition_id: pid.into(),
            partition: partition.into(),
            lease_id: "L1".into(),
            consumer_group: "g".into(),
        }
    }

    #[test]
    fn grouping_keeps_partitions_apart_and_in_arrival_order() {
        let msgs = vec![
            message("p1", "a", "1", "2026-08-04T10:00:00.000Z"),
            message("p2", "b", "2", "2026-08-04T10:00:00.000Z"),
            message("p1", "a", "3", "2026-08-04T10:00:01.000Z"),
        ];
        let groups = group_by_partition(msgs, "fallback");
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].partition_id, "p1");
        assert_eq!(groups[0].messages.len(), 2);
        assert_eq!(groups[1].partition_id, "p2");
        assert_eq!(groups[1].messages.len(), 1);
    }

    #[test]
    fn grouping_falls_back_to_the_runners_group() {
        let mut m = message("p1", "a", "1", "2026-08-04T10:00:00.000Z");
        m.consumer_group = String::new();
        let groups = group_by_partition(vec![m], "streams.my-query");
        assert_eq!(groups[0].consumer_group, "streams.my-query");
    }

    #[test]
    fn stateless_ops_chain_in_order() {
        let ops = vec![
            Op::Map(Arc::new(|r| serde_json::json!({ "n": r.number("n").unwrap_or(0.0) * 2.0 }))),
            Op::Filter(Arc::new(|r| r.number("n").unwrap_or(0.0) > 2.0)),
        ];
        let records = vec![
            (
                Record {
                    data: serde_json::json!({ "n": 1.0 }),
                    message: None,
                    ctx: None,
                },
                "k".to_string(),
            ),
            (
                Record {
                    data: serde_json::json!({ "n": 5.0 }),
                    message: None,
                    ctx: None,
                },
                "k".to_string(),
            ),
        ];
        let out = apply_stateless(&ops, records).unwrap();
        // 1*2 = 2 is filtered out; 5*2 = 10 survives.
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0.data["n"], 10.0);
    }

    #[test]
    fn flat_map_fans_out_keeping_the_key() {
        let ops = vec![Op::FlatMap(Arc::new(|_| {
            vec![serde_json::json!(1), serde_json::json!(2)]
        }))];
        let records = vec![(
            Record {
                data: serde_json::json!({}),
                message: None,
                ctx: None,
            },
            "key-1".to_string(),
        )];
        let out = apply_stateless(&ops, records).unwrap();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|(_, k)| k == "key-1"));
    }

    #[test]
    fn scalar_emits_are_wrapped_for_the_sink() {
        let sink = Sink {
            queue: "out".into(),
            partition: Source,
        };
        let emits = vec![
            Emit {
                key: "k".into(),
                window_start: 0,
                window_end: 10,
                window_key: "w".into(),
                value: serde_json::json!(42),
            },
            Emit {
                key: "k".into(),
                window_start: 0,
                window_end: 10,
                window_key: "w".into(),
                value: serde_json::json!({ "count": 3 }),
            },
        ];
        let items = build_push_items(&sink, &emits, "eu");
        // A bare number is not a valid message payload; objects pass through.
        assert_eq!(items[0].payload, serde_json::json!({ "value": 42 }));
        assert_eq!(items[1].payload, serde_json::json!({ "count": 3 }));
        assert_eq!(items[0].partition.as_deref(), Some("eu"));
    }

    #[test]
    fn a_fixed_sink_partition_overrides_the_source_lane() {
        let sink = Sink {
            queue: "out".into(),
            partition: SinkPartition::Fixed("all".into()),
        };
        let emits = vec![Emit {
            key: "k".into(),
            window_start: 0,
            window_end: 10,
            window_key: "w".into(),
            value: serde_json::json!({}),
        }];
        assert_eq!(
            build_push_items(&sink, &emits, "eu")[0].partition.as_deref(),
            Some("all")
        );
    }

    #[test]
    fn emit_ctx_omits_window_bounds_for_unwindowed_records() {
        let e = Emit {
            key: "k".into(),
            window_start: 0,
            window_end: 0,
            window_key: String::new(),
            value: serde_json::json!({}),
        };
        let ctx = emit_ctx(&e, "eu", "p1");
        assert_eq!(ctx.key, "k");
        assert_eq!(ctx.partition, "eu");
        assert!(ctx.window_key.is_none());
        assert!(ctx.window_start.is_none());

        let e = Emit {
            window_end: 60_000,
            window_key: "2026-08-04T10:00:00.000Z".into(),
            ..e
        };
        let ctx = emit_ctx(&e, "eu", "p1");
        assert_eq!(ctx.window_end, Some(60_000));
        assert!(ctx.window_key.is_some());
    }

    #[test]
    fn run_options_have_the_same_defaults_as_the_other_sdks() {
        let o = RunOptions::new("q");
        assert_eq!(o.batch_size, 200);
        assert_eq!(o.max_partitions, 4);
        assert_eq!(o.max_wait, Duration::from_millis(1000));
        assert!(!o.reset);
        assert!(o.consumer_group.is_none());
    }
}
