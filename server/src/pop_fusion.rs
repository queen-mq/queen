// POP FUSION — server-side pop-claim coalescing.
//
// MOTIVATION (2026-08-02, sparse-partition diagnosis). A hot-list pop's claim
// leg is ONE PG transaction: lease UPDATEs + a synchronous commit. On the
// sparse CM shape (1000 partitions × ~1 msg/s each, 8-core cell) the wait
// sampler showed LWLock/WALWrite as the dominant wait (16.4k samples vs 3.5k
// CPU with the admission window at 64): every pop pays its own seat on the WAL
// flush train, and wall-per-pop (~88 ms measured) is ~90% commit-queue time.
// The C++ 0.16 broker amortized this through its shared engine loops (~235k
// ops/s at ~1,058 commits/s in the 2026-06-07 soak); the seg-engine port
// rebuilt fusion for push (fusion.rs) and ack (ack_fusion.rs) but the pop claim
// stayed 1 pop = 1 transaction. This module restores the missing third leg:
// N pop claim legs share ONE transaction, hence ONE commit.
//
// PREREQUISITE. The batched queen.log_pop_list_v1 (2026-08-02 rewrite): ~6
// statements per claim regardless of candidate count, every row lock taken
// FOR UPDATE SKIP LOCKED. Jobs inside a fused transaction therefore never wait
// on each other (candidates are disjoint by hot-list checkout; SKIP LOCKED
// never blocks), so tx duration ≈ Σ exec — with the OLD per-partition FOREACH
// body the same fusion would have multiplied a long serial leg and starved the
// ring, which is exactly what the k=512 checkout experiment measured.
//
// FIRE-ON-IDLE (the ack_fusion pattern, verbatim). At most ONE flush per shard
// is in flight. On arrival with an idle shard the job fires IMMEDIATELY — at
// low load a pop claims in ~one DB RTT, latency identical to the direct path,
// so on dense shapes (where push commits dominate anyway) fusion degenerates to
// today's behavior. Under load, arrivals accumulate for the in-flight flush's
// RTT and the next flush carries all of them: batch size is EMERGENT
// (pop_rate × RTT / shards), not a tuned threshold. QUEEN_POP_FUSION_HOLD_MS is
// a liveness backstop tick, not a latency floor.
//
// SEMANTICS — unchanged, and this is the whole point:
//   * The commit is still SYNCHRONOUS; a served pop's lease is durable exactly
//     as before. Fusion shares the fsync, it does not skip it.
//   * A flush error (pool/SQL/timeout/commit) resolves EVERY participant to
//     FlushErr; the caller re-appends its candidates (Requeue) and returns
//     empty — byte-identical to the direct path's DB-error handling. A rolled
//     back transaction wrote nothing, so nothing leaks.
//   * A caller whose HTTP future dies mid-await leaves the flush to commit its
//     (now unobserved) leases; the InflightGuard requeues the ring entries, the
//     next serve sees the live lease ('leased' verdict), and the lease expires
//     into redelivery — the same at-least-once window the direct path already
//     has between SQL completion and render, widened by ≤ one flush RTT.
//
// ADMISSION. The slot is taken once per FLUSH, not once per pop: the
// limiter's RTT signal becomes the fused transaction — the thing that actually
// queues on PG — instead of N copies of the same WAL wait.
//
// KILL SWITCH. QUEEN_POP_FUSION unset/=0 ⇒ enabled() is false, the serve path
// never enqueues, and the pop path is byte-identical to today's.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;
use tokio::sync::{mpsc, oneshot};

use crate::db;
use crate::admission::{Admission, Lane};

// Coalescing observability: COMMITS counts fused transactions, JOBS the pop
// claims inside them. jobs/commit is the fusion gain, cross-checkable against
// pg_stat_statements (log_pop_list_v1.calls ≈ JOBS; commits ≈ COMMITS).
static POP_COMMITS: AtomicU64 = AtomicU64::new(0);
static POP_JOBS: AtomicU64 = AtomicU64::new(0);

// Rolling commit-wait ring for the fused claim transactions: the exec/commit
// split the admission law cannot see from slot age alone. Read in the
// periodic coalesce summary.
static COMMIT_WAIT: std::sync::Mutex<Vec<f64>> = std::sync::Mutex::new(Vec::new());

fn record_commit_wait(d: std::time::Duration) {
    if let Ok(mut v) = COMMIT_WAIT.lock() {
        if v.len() >= 4096 {
            let overflow = v.len() - 4095;
            v.drain(0..overflow);
        }
        v.push(d.as_secs_f64() * 1e3);
    }
}

fn commit_wait_p(v: &mut Vec<f64>, p: usize) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[(v.len() * p / 100).min(v.len() - 1)]
}

fn record_flush(jobs: usize) {
    let commits = POP_COMMITS.fetch_add(1, Ordering::Relaxed) + 1;
    let total = POP_JOBS.fetch_add(jobs as u64, Ordering::Relaxed) + jobs as u64;
    static SUMMARY: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
    if SUMMARY.tick_now().is_some() {
        tracing::info!(
            target: "pop-fusion",
            commits,
            jobs = total,
            jobs_per_commit = format!("{:.2}", total as f64 / commits as f64),
            commit_p50_ms = format!("{:.2}", {
                let mut v = COMMIT_WAIT.lock().map(|g| g.clone()).unwrap_or_default();
                commit_wait_p(&mut v, 50)
            }),
            commit_p95_ms = format!("{:.2}", {
                let mut v = COMMIT_WAIT.lock().map(|g| g.clone()).unwrap_or_default();
                commit_wait_p(&mut v, 95)
            }),
            "coalesce summary"
        );
    }
}

/// One fused claim's outcome, handed back to the waiting serve path.
pub enum PopVerdict {
    /// The fused transaction committed; this job's rows are durable.
    Served {
        meta: String,
        blobs: Vec<Vec<u8>>,
        states: String,
        /// The fused transaction's SQL wall (shared by every fused job),
        /// clocked AFTER admission + pool checkout — the pop metrics'
        /// per-request rtt, same meaning as the direct path's.
        rtt: Duration,
    },
    /// The whole flush failed (pool/SQL/timeout/commit). The caller requeues
    /// its candidates and returns empty — the direct path's DB-error behavior.
    FlushErr,
}

/// One enqueued pop claim: the exact argument list of db::pop_list.
pub struct PopJob {
    pub queue: String,
    pub group: String,
    pub partitions: Vec<String>,
    pub budget: i32,
    pub lease_seconds: i32,
    pub worker: String,
    pub auto_ack: bool,
    pub max_partitions: i32,
    pub sub_mode: String,
    pub sub_from: String,
    pub skip_window: bool,
    pub tenant: String,
    done: oneshot::Sender<PopVerdict>,
}

struct FlushCtx {
    pool: Pool,
    stmt_timeout: Duration,
    admission: Arc<Admission>,
    metrics: Arc<crate::metrics::Metrics>,
    max_jobs: usize,
    max_inflight: u32,
}

/// Process-local pop-fusion pipeline. Shards are round-robin: unlike acks,
/// jobs have no fold key — same-(queue,group) jobs carry DISJOINT candidate
/// sets by hot-list checkout, so any placement is correct.
pub struct PopFusion {
    senders: Vec<mpsc::UnboundedSender<PopJob>>,
    rr: AtomicUsize,
    enabled: bool,
}

impl PopFusion {
    pub fn new(
        shards: usize,
        pool: Pool,
        stmt_timeout: Duration,
        admission: Arc<Admission>,
        metrics: Arc<crate::metrics::Metrics>,
        hold_ms: u64,
        max_jobs: usize,
        max_inflight: u32,
        enabled: bool,
    ) -> Arc<PopFusion> {
        let shards = shards.max(1);
        let hold_ms = hold_ms.max(1);
        let max_jobs = max_jobs.max(1);
        let mut senders = Vec::with_capacity(shards);
        if enabled {
            for _ in 0..shards {
                let (tx, rx) = mpsc::unbounded_channel::<PopJob>();
                senders.push(tx);
                let ctx = Arc::new(FlushCtx {
                    pool: pool.clone(),
                    stmt_timeout,
                    admission: admission.clone(),
                    metrics: metrics.clone(),
                    max_jobs,
                    max_inflight: max_inflight.max(1),
                });
                tokio::spawn(shard_loop(rx, ctx, hold_ms));
            }
        }
        Arc::new(PopFusion {
            senders,
            rr: AtomicUsize::new(0),
            enabled,
        })
    }

    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Enqueue one claim and PARK until its flush resolves. A closed shard
    /// (shutdown) or a dropped flush sender resolve to FlushErr — the caller
    /// requeues, nothing strands.
    #[allow(clippy::too_many_arguments)]
    pub async fn claim(
        &self,
        queue: String,
        group: String,
        partitions: Vec<String>,
        budget: i32,
        lease_seconds: i32,
        worker: String,
        auto_ack: bool,
        max_partitions: i32,
        sub_mode: String,
        sub_from: String,
        skip_window: bool,
        tenant: String,
    ) -> PopVerdict {
        if !self.enabled {
            return PopVerdict::FlushErr; // defensive; the caller gates on enabled()
        }
        let (tx, rx) = oneshot::channel();
        let idx = self.rr.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        let job = PopJob {
            queue,
            group,
            partitions,
            budget,
            lease_seconds,
            worker,
            auto_ack,
            max_partitions,
            sub_mode,
            sub_from,
            skip_window,
            tenant,
            done: tx,
        };
        if self.senders[idx].send(job).is_err() {
            return PopVerdict::FlushErr;
        }
        match rx.await {
            Ok(v) => v,
            Err(_) => PopVerdict::FlushErr,
        }
    }
}

// One shard: FIFO buffer + fire-on-idle, mirroring ack_fusion::shard_loop with
// ONE deliberate divergence: MULTI-FLIGHT. Ack fusion allows a single in-flight
// flush per shard because acks need per-partition commit ORDER; pop claims have
// no cross-flush ordering at all (candidate sets are disjoint by hot-list
// checkout), so up to max_inflight flushes may overlap. Measured 2026-08-02:
// single-flight × 4 shards capped the whole broker at ~100 fires/s ≈ ~800
// pops/s — the serve pipeline width, not the WAL, became the sparse-shape
// ceiling; and raising SHARDS instead dilutes the emergent batch (jobs/commit
// × fires/s = pops/s is an identity — width must come from overlap, not from
// more buffers). The buffer still drains at most max_jobs per fire.
async fn shard_loop(mut rx: mpsc::UnboundedReceiver<PopJob>, ctx: Arc<FlushCtx>, hold_ms: u64) {
    let mut buf: VecDeque<PopJob> = VecDeque::new();
    let mut inflight: u32 = 0;
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<()>();
    let mut tick = tokio::time::interval(Duration::from_millis(hold_ms));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            maybe = rx.recv() => {
                let Some(job) = maybe else { break; };
                buf.push_back(job);
                dispatch(&mut buf, &mut inflight, &ctx, &done_tx);
            }
            Some(()) = done_rx.recv() => {
                inflight = inflight.saturating_sub(1);
                dispatch(&mut buf, &mut inflight, &ctx, &done_tx);
            }
            _ = tick.tick() => {
                dispatch(&mut buf, &mut inflight, &ctx, &done_tx);
            }
        }
    }
}

fn dispatch(
    buf: &mut VecDeque<PopJob>,
    inflight: &mut u32,
    ctx: &Arc<FlushCtx>,
    done: &mpsc::UnboundedSender<()>,
) {
    while *inflight < ctx.max_inflight && !buf.is_empty() {
        let take = buf.len().min(ctx.max_jobs);
        let jobs: Vec<PopJob> = buf.drain(..take).collect();
        *inflight += 1;
        spawn_flush(ctx.clone(), jobs, done.clone());
    }
}

// Execute N claims in ONE transaction / ONE commit and demux each job's rows
// back to its waiter. Completion is signalled AFTER the DB work resolves, so
// the shard's next flush strictly follows this one.
fn spawn_flush(ctx: Arc<FlushCtx>, jobs: Vec<PopJob>, done: mpsc::UnboundedSender<()>) {
    tokio::spawn(async move {
        let n = jobs.len();

        // Admission: one slot per fused transaction (see module header).
        let mut slot = ctx.admission.acquire(Lane::Pop).await;

        // The rtt clock starts AFTER admission + pool checkout — parity with
        // the direct path, whose recorded rtt is the SQL wall only. Clocking
        // from before acquire() would feed the admission queue back into the
        // measured wait as if it were commit cost. (The Vegas-era limiter had
        // exactly that loop: measured 2026-08-03 at 2k/1000 sparse lanes,
        // vegas_pop sat pinned at 6 and ~70 of the 80 ms fused wait was the
        // feedback. The train sensor only ever sees post-admission SQL wall.)
        let mut sql_rtt = Duration::ZERO;
        let outcome: Result<Vec<(String, Vec<Vec<u8>>, String)>, ()> = async {
            let mut client = ctx.pool.get().await.map_err(|_| ())?;
            let cancel = client.cancel_token();
            let t0 = Instant::now();

            let work = async {
                let tx = client.transaction().await?;
                let mut results = Vec::with_capacity(n);
                for j in jobs.iter() {
                    let r = db::pop_list_tx(
                        &tx,
                        &j.queue,
                        &j.group,
                        &j.partitions,
                        j.budget,
                        j.lease_seconds,
                        &j.worker,
                        j.auto_ack,
                        j.max_partitions,
                        &j.sub_mode,
                        &j.sub_from,
                        j.skip_window,
                        &j.tenant,
                    )
                    .await?;
                    results.push(r);
                }
                let t_commit = Instant::now();
                tx.commit().await?;
                Ok::<_, tokio_postgres::Error>((results, t_commit.elapsed()))
            };

            let res = tokio::time::timeout(ctx.stmt_timeout, work).await;
            sql_rtt = t0.elapsed();
            match res {
                Ok(Ok((results, commit_wait))) => {
                    record_commit_wait(commit_wait);
                    Ok(results)
                }
                Ok(Err(_)) => {
                    // Statement/commit error: the tx is aborted (or the commit
                    // failed), nothing durable happened. Connection is usable.
                    ctx.metrics.record_db_error();
                    Err(())
                }
                Err(_elapsed) => {
                    // Broker-side timeout: cancel server-side and QUARANTINE the
                    // connection (mirror of db::resolve_query_timeout's Elapsed
                    // arm — never recycle a connection whose in-flight statement
                    // was abandoned).
                    db::spawn_cancel(cancel, "pop_fusion");
                    ctx.metrics.record_db_error();
                    let _ = deadpool_postgres::Object::take(client);
                    Err(())
                }
            }
        }
        .await;

        // Feed the train sensor only on a real commit: a pool failure ran no
        // SQL, an error/timeout aborted the transaction — neither produced a
        // flush ack worth clustering. The slot release itself is RAII: no arm
        // can leak in-flight accounting.
        let rtt = sql_rtt;
        if outcome.is_ok() && !rtt.is_zero() {
            slot.commit_done(rtt);
        }
        drop(slot);

        match outcome {
            Ok(results) => {
                record_flush(n);
                for (j, (meta, blobs, states)) in jobs.into_iter().zip(results) {
                    let _ = j.done.send(PopVerdict::Served {
                        meta,
                        blobs,
                        states,
                        rtt,
                    });
                }
            }
            Err(()) => {
                for j in jobs {
                    let _ = j.done.send(PopVerdict::FlushErr);
                }
            }
        }
        let _ = done.send(());
    });
}
