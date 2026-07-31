// ACK FUSION — server-side ack coalescing ("ack fusion").
//
// MOTIVATION. With explicit acks, every full-batch completed ack that HITs the
// ack registry (server/src/ack_registry.rs) is one positional cursor advance —
// but STILL one PG transaction, hence one commit / one fsync. Measured on the VM:
// ~8k ack/s + ~3.1k lease/s + the push bundle ≈ 13k commit/s against a disk that
// sustains ~10k fsync/s, so the push bundle pays +18ms queued behind the ack
// commits. The cursor advance is MONOTONE and IDEMPOTENT — acking (partition,
// group) to 200 implies everything through 200 (incl. an earlier ack to 100) is
// committed — so many independent cursor advances FUSE into ONE transaction with
// NO loss of semantics. This module buffers full-batch acks per shard and commits
// them via queen.log_ack_multi_v1 (005_log_ack): one commit for N cursors.
//
// FIRE-ON-IDLE (mirrors the push fusion, server/src/fusion.rs). At most ONE flush
// per shard is ever in flight. On arrival, if the shard has no flush in flight, it
// flushes IMMEDIATELY — so at low load an ack commits in ~one DB RTT, latency
// identical to today (a flush of exactly one row). Under load, arrivals accumulate
// in the shard buffer for the duration of the in-flight flush's RTT and the next
// flush commits all of them together — batch size is EMERGENT (offered_rate × RTT
// / shards), not a fixed threshold. QUEEN_ACK_FUSION_HOLD_MS is a coarse liveness
// backstop tick, not a latency floor.
//
// FOLDING. Buffer entries with the same (partition, group, worker) FUSE to
// max(batch_end): the folded row commits the furthest cursor, and EVERY fused HTTP
// request receives that folded row's verdict (monotonicity — the higher advance
// implies the lower). Two DIFFERENT workers on the same (partition, group) do NOT
// fold: they stay separate rows in the same flush, each independently validated by
// the SP's per-row worker/lease guard (a stale worker is rejected — one holder per
// consumer row). Because the buffer is sharded by hash(partition), all acks for a
// given (partition, *) land in one shard, so same-key folding and per-partition
// commit order are both intra-shard and race-free.
//
// SYNCHRONOUS RESPONSE. Like the push PushState pattern, the HTTP ack response is
// resolved only when the flush COMMITS — a 200 still means the cursor is durable.
// Per-row verdicts are demuxed back to each row's waiters:
//   * Committed — cursor durably advanced → the caller renders success.
//   * Rejected  — the SP validated the row ok:false (stale/expired lease, batch
//                 clamp) but the flush itself committed; the caller falls back to
//                 the unchanged log_ack_by_hash_v1 path for THAT client only.
//   * FlushErr  — the WHOLE flush failed (pool/infra/timeout/parse); every
//                 participant errors, the lease will expire and the batch
//                 redelivers (at-least-once contract unchanged).
//
// KILL SWITCH. Disabled (QUEEN_ACK_FUSION unset / =0) ⇒ `enabled()` is false and
// the ack handler never calls this module; the ack path is byte-identical to
// today's synchronous log_ack_at_v1 fast path.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;
use tokio::sync::{mpsc, oneshot};

use crate::db;
use crate::util::FnvHashMap;

// ---------------------------------------------------------------------------
// Coalescing observability: COMMITS counts log_ack_multi_v1 transactions, ROWS
// counts the cursor advances inside them. rows/commit is the fusion gain and the
// direct broker-side evidence to cross-check against pg_stat_statements
// (log_ack_multi_v1.calls ≈ COMMITS ≪ total acks). Periodic summary only.
static ACK_COMMITS: AtomicU64 = AtomicU64::new(0);
static ACK_ROWS: AtomicU64 = AtomicU64::new(0);

fn record_flush(rows: usize) {
    let commits = ACK_COMMITS.fetch_add(1, Ordering::Relaxed) + 1;
    let total_rows = ACK_ROWS.fetch_add(rows as u64, Ordering::Relaxed) + rows as u64;
    // LOGGING_PLAN.md Phase 2: was `commits % 500 == 0`, which gets NOISIER as
    // throughput rises (>500 commits/s ⇒ multiple lines/s on the hot path). A
    // time-window sampler caps it at ~1 line / 10s regardless of load.
    static SUMMARY: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
    if SUMMARY.tick_now().is_some() {
        tracing::info!(
            target: "ack-fusion",
            commits,
            rows = total_rows,
            rows_per_commit = format!("{:.2}", total_rows as f64 / commits as f64),
            "coalesce summary"
        );
    }
}

/// Per-row commit outcome handed back to each waiting HTTP ack request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckVerdict {
    /// The cursor was durably advanced — render success + lease released.
    Committed,
    /// The SP rejected this row (stale/expired lease, batch clamp) but the flush
    /// committed. The caller falls back to the unchanged SQL ack path for this
    /// client only (exactly today's ok:false fall-through).
    Rejected,
    /// The whole flush failed (pool/infra/timeout/parse). Error the client; the
    /// lease expires and the batch redelivers (at-least-once, contract unchanged).
    FlushErr,
}

// One enqueued full-batch ack. `count` feeds total_consumed (GREATEST(count,0)).
struct AckReq {
    pid: String,
    group: String,
    worker: String,
    batch_end: i64,
    count: i32,
    done: oneshot::Sender<AckVerdict>,
}

// A folded buffer entry, keyed (pid, group, worker). `batch_end` is the max over
// all folded requests; `count` is the count of the request that set that max
// (total_consumed is a pure statistic — see fold_in). All fused requests' waiters
// share the folded row's single verdict.
struct Folded {
    pid: String,
    group: String,
    worker: String,
    batch_end: i64,
    count: i32,
    waiters: Vec<oneshot::Sender<AckVerdict>>,
}

struct FlushCtx {
    pool: Pool,
    stmt_timeout: Duration,
}

/// Process-local ack-fusion pipeline (`Arc<AckFusion>`). Sharded by partition.
pub struct AckFusion {
    senders: Vec<mpsc::UnboundedSender<AckReq>>,
    enabled: bool,
}

#[inline]
fn compose_key(pid: &str, group: &str, worker: &str) -> String {
    let mut s = String::with_capacity(pid.len() + group.len() + worker.len() + 2);
    s.push_str(pid);
    s.push('\u{1f}');
    s.push_str(group);
    s.push('\u{1f}');
    s.push_str(worker);
    s
}

// Same FNV-1a-over-partition hash the push fusion uses, so a partition's acks are
// shard-stable (fold correctly + preserve per-partition commit order).
#[inline]
fn shard_of(pid: &str, shards: usize) -> usize {
    let mut h: u64 = 1469598103934665603;
    for b in pid.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    (h as usize) % shards
}

impl AckFusion {
    /// `shards` buffers, each an independent fire-on-idle flush loop. `hold_ms` is
    /// the liveness backstop tick (QUEEN_ACK_FUSION_HOLD_MS). `enabled=false` makes
    /// the whole module inert — the handler checks `enabled()` and never enqueues.
    pub fn new(
        shards: usize,
        pool: Pool,
        stmt_timeout: Duration,
        hold_ms: u64,
        enabled: bool,
    ) -> Arc<AckFusion> {
        let shards = shards.max(1);
        let hold_ms = hold_ms.max(1);
        let mut senders = Vec::with_capacity(shards);
        if enabled {
            for _ in 0..shards {
                let (tx, rx) = mpsc::unbounded_channel::<AckReq>();
                senders.push(tx);
                let ctx = Arc::new(FlushCtx {
                    pool: pool.clone(),
                    stmt_timeout,
                });
                tokio::spawn(shard_loop(rx, ctx, hold_ms));
            }
        }
        Arc::new(AckFusion { senders, enabled })
    }

    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Enqueue a full-batch completed ack and PARK until its flush commits. The
    /// caller must have already proved the fast-path preconditions (registry HIT:
    /// worker matches, whole batch completed, upto == batch_end). Returns the
    /// per-row verdict once the flush resolves. A closed shard (shutdown) or a
    /// dropped flush sender both resolve to FlushErr.
    pub async fn ack(
        &self,
        pid: String,
        group: String,
        worker: String,
        batch_end: i64,
        count: i32,
    ) -> AckVerdict {
        if !self.enabled {
            // Defensive: the handler gates on enabled(); never reached in practice.
            return AckVerdict::FlushErr;
        }
        let (tx, rx) = oneshot::channel();
        let idx = shard_of(&pid, self.senders.len());
        let req = AckReq {
            pid,
            group,
            worker,
            batch_end,
            count,
            done: tx,
        };
        if self.senders[idx].send(req).is_err() {
            return AckVerdict::FlushErr;
        }
        match rx.await {
            Ok(v) => v,
            Err(_) => AckVerdict::FlushErr,
        }
    }
}

// Fold one request into the shard buffer. Same-key entries fuse to max(batch_end);
// the winning request's `count` rides along (total_consumed is a pure statistic,
// so the astronomically-rare same-key fold — the registry consumes each entry on
// HIT, so a second HIT for the same key can't happen — never needs an exact sum).
fn fold_in(buf: &mut FnvHashMap<String, Folded>, req: AckReq) {
    let key = compose_key(&req.pid, &req.group, &req.worker);
    match buf.get_mut(&key) {
        Some(f) => {
            if req.batch_end > f.batch_end {
                f.batch_end = req.batch_end;
                f.count = req.count;
            }
            f.waiters.push(req.done);
        }
        None => {
            buf.insert(
                key,
                Folded {
                    pid: req.pid,
                    group: req.group,
                    worker: req.worker,
                    batch_end: req.batch_end,
                    count: req.count,
                    waiters: vec![req.done],
                },
            );
        }
    }
}

// One shard owns a disjoint set of partitions (Fusion-style hash), so the buffer
// and the in-flight flag are single-task-local and lock-free.
async fn shard_loop(mut rx: mpsc::UnboundedReceiver<AckReq>, ctx: Arc<FlushCtx>, hold_ms: u64) {
    let mut buf: FnvHashMap<String, Folded> = FnvHashMap::default();
    let mut inflight = false;
    // Flush tasks signal completion here so the shard releases the in-flight slot
    // and flushes whatever accumulated during the RTT. Held for the whole loop.
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<()>();
    let mut tick = tokio::time::interval(Duration::from_millis(hold_ms));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            maybe = rx.recv() => {
                let Some(req) = maybe else { break; };
                fold_in(&mut buf, req);
                // Fire-on-idle: no flush in flight ⇒ dispatch now (a flush of
                // exactly the rows accumulated so far — one row at low load).
                if !inflight {
                    dispatch(&mut buf, &mut inflight, &ctx, &done_tx);
                }
            }
            Some(()) = done_rx.recv() => {
                inflight = false;
                // The RTT that just ended may have accumulated new rows — commit
                // them now (they necessarily commit AFTER the flush that just
                // committed, so per-partition cursor order == commit order).
                dispatch(&mut buf, &mut inflight, &ctx, &done_tx);
            }
            _ = tick.tick() => {
                // Liveness backstop: if a completion signal were ever lost, this
                // still drains the buffer. In the common path arrival/completion
                // already dispatch, so this rarely does work.
                if !inflight {
                    dispatch(&mut buf, &mut inflight, &ctx, &done_tx);
                }
            }
        }
    }
}

// Drain the buffer into one flush if there is anything to commit and no flush is
// in flight. Sets `inflight` and spawns the flush.
fn dispatch(
    buf: &mut FnvHashMap<String, Folded>,
    inflight: &mut bool,
    ctx: &Arc<FlushCtx>,
    done: &mpsc::UnboundedSender<()>,
) {
    if *inflight || buf.is_empty() {
        return;
    }
    let rows: Vec<Folded> = std::mem::take(buf).into_values().collect();
    *inflight = true;
    spawn_flush(ctx.clone(), rows, done.clone());
}

// Commit N folded cursor advances in ONE queen.log_ack_multi_v1 transaction and
// demux the per-row verdicts back to each row's waiters. Completion is signalled
// AFTER the DB op returns, so the shard's next flush strictly follows this commit.
fn spawn_flush(ctx: Arc<FlushCtx>, rows: Vec<Folded>, done: mpsc::UnboundedSender<()>) {
    tokio::spawn(async move {
        let n = rows.len();
        let t0 = Instant::now();

        // Index-aligned typed arrays (the SP realigns its result to input order).
        let mut pids: Vec<String> = Vec::with_capacity(n);
        let mut groups: Vec<String> = Vec::with_capacity(n);
        let mut ends: Vec<i64> = Vec::with_capacity(n);
        let mut workers: Vec<String> = Vec::with_capacity(n);
        let mut counts: Vec<i32> = Vec::with_capacity(n);
        for r in &rows {
            pids.push(r.pid.clone());
            groups.push(r.group.clone());
            ends.push(r.batch_end);
            workers.push(r.worker.clone());
            counts.push(r.count);
        }

        // Resolve every row's verdict, then demux. A whole-flush failure (no
        // connection / SQL error / timeout / malformed or misaligned result)
        // resolves EVERY participant to FlushErr — the safe direction (lease
        // expiry → redelivery), never a silent skipped ack.
        let verdicts: Option<Vec<AckVerdict>> = match ctx.pool.get().await {
            Ok(client) => {
                match tokio::time::timeout(
                    ctx.stmt_timeout,
                    db::ack_multi(&client, &pids, &groups, &ends, &workers, &counts),
                )
                .await
                {
                    Ok(Ok(txt)) => parse_multi(&txt, n),
                    Ok(Err(e)) => {
                        static ACK_MULTI_ERR: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
                        if let Some(suppressed) = ACK_MULTI_ERR.tick_now() {
                            tracing::error!(target: "ack-fusion", rows = n, error = %e, suppressed, "log_ack_multi error");
                        }
                        None
                    }
                    Err(_) => {
                        static ACK_MULTI_TIMEOUT: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
                        if let Some(suppressed) = ACK_MULTI_TIMEOUT.tick_now() {
                            tracing::error!(target: "ack-fusion", rows = n, suppressed, "log_ack_multi timeout");
                        }
                        None
                    }
                }
            }
            Err(e) => {
                static POOL_GET_ERR: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
                if let Some(suppressed) = POOL_GET_ERR.tick_now() {
                    tracing::error!(target: "ack-fusion", rows = n, error = %e, suppressed, "pool.get error");
                }
                None
            }
        };

        match verdicts {
            Some(vs) => {
                for (r, v) in rows.into_iter().zip(vs.into_iter()) {
                    for w in r.waiters {
                        let _ = w.send(v);
                    }
                }
            }
            None => {
                for r in rows {
                    for w in r.waiters {
                        let _ = w.send(AckVerdict::FlushErr);
                    }
                }
            }
        }

        record_flush(n);
        let _ = t0; // (rtt available if a latency metric is wired later)
        // Release the in-flight slot LAST so the shard re-dispatches only after
        // this commit is visible. Best-effort: a closed receiver means shutdown.
        let _ = done.send(());
    });
}

// Parse queen.log_ack_multi_v1's {"ok":true,"results":[{ok,acked,error},...]}.
// Returns None (⇒ whole-flush FlushErr) on any structural surprise: not ok, wrong
// length, or a non-object element — a result the broker can't trust must never
// silently pass a subset of acks. `results` is in INPUT order (the SP writes by
// ordinal), so element i is row i's verdict.
fn parse_multi(txt: &str, expected: usize) -> Option<Vec<AckVerdict>> {
    let v: serde_json::Value = serde_json::from_str(txt).ok()?;
    if !v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false) {
        return None;
    }
    let arr = v.get("results").and_then(|r| r.as_array())?;
    if arr.len() != expected {
        return None;
    }
    let mut out = Vec::with_capacity(arr.len());
    for e in arr {
        // A row-level object is required; ok:true ⇒ Committed, ok:false ⇒ Rejected.
        let ok = e.get("ok").and_then(|x| x.as_bool())?;
        out.push(if ok {
            AckVerdict::Committed
        } else {
            AckVerdict::Rejected
        });
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oneshot_pair() -> (oneshot::Sender<AckVerdict>, oneshot::Receiver<AckVerdict>) {
        oneshot::channel()
    }

    fn req(
        pid: &str,
        group: &str,
        worker: &str,
        end: i64,
        count: i32,
    ) -> (AckReq, oneshot::Receiver<AckVerdict>) {
        let (tx, rx) = oneshot_pair();
        (
            AckReq {
                pid: pid.to_string(),
                group: group.to_string(),
                worker: worker.to_string(),
                batch_end: end,
                count,
                done: tx,
            },
            rx,
        )
    }

    #[test]
    fn fold_same_key_to_max_and_keeps_all_waiters() {
        let mut buf: FnvHashMap<String, Folded> = FnvHashMap::default();
        let (r1, _rx1) = req("p1", "g", "w1", 100, 5);
        let (r2, _rx2) = req("p1", "g", "w1", 200, 7);
        fold_in(&mut buf, r1);
        fold_in(&mut buf, r2);
        // One folded row: max batch_end (200) with the winner's count (7), and
        // BOTH requests waiting on it (monotone: advancing to 200 implies 100).
        assert_eq!(buf.len(), 1);
        let f = buf.values().next().unwrap();
        assert_eq!(f.batch_end, 200);
        assert_eq!(f.count, 7);
        assert_eq!(f.waiters.len(), 2);
    }

    #[test]
    fn fold_out_of_order_keeps_max() {
        // A later, LOWER advance must not regress the folded cursor.
        let mut buf: FnvHashMap<String, Folded> = FnvHashMap::default();
        let (r1, _a) = req("p1", "g", "w1", 200, 7);
        let (r2, _b) = req("p1", "g", "w1", 100, 5);
        fold_in(&mut buf, r1);
        fold_in(&mut buf, r2);
        let f = buf.values().next().unwrap();
        assert_eq!(f.batch_end, 200);
        assert_eq!(f.count, 7);
        assert_eq!(f.waiters.len(), 2);
    }

    #[test]
    fn different_workers_do_not_fold() {
        // Same (partition, group), DIFFERENT workers ⇒ two separate rows in the
        // flush (the SP's per-row worker guard rejects the stale one).
        let mut buf: FnvHashMap<String, Folded> = FnvHashMap::default();
        let (r1, _a) = req("p1", "g", "w1", 100, 5);
        let (r2, _b) = req("p1", "g", "w2", 100, 5);
        fold_in(&mut buf, r1);
        fold_in(&mut buf, r2);
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn different_partitions_do_not_fold() {
        let mut buf: FnvHashMap<String, Folded> = FnvHashMap::default();
        let (r1, _a) = req("p1", "g", "w1", 100, 5);
        let (r2, _b) = req("p2", "g", "w1", 100, 5);
        fold_in(&mut buf, r1);
        fold_in(&mut buf, r2);
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn shard_of_is_partition_stable() {
        // The same partition always hashes to the same shard (fold + order).
        for shards in [1usize, 4, 8, 16] {
            let a = shard_of("0198f0a4-7b1e-7c3d-9a2b-1c4d5e6f7a8b", shards);
            let b = shard_of("0198f0a4-7b1e-7c3d-9a2b-1c4d5e6f7a8b", shards);
            assert_eq!(a, b);
            assert!(a < shards);
        }
    }

    #[test]
    fn parse_multi_scatters_verdicts_by_ordinal() {
        // results is INPUT order: row0 ok, row1 rejected, row2 ok.
        let txt = r#"{"ok":true,"results":[
            {"ok":true,"acked":5,"error":null},
            {"ok":false,"acked":0,"error":"invalid or expired lease"},
            {"ok":true,"acked":3,"error":null}
        ]}"#;
        let vs = parse_multi(txt, 3).expect("parses");
        assert_eq!(
            vs,
            vec![
                AckVerdict::Committed,
                AckVerdict::Rejected,
                AckVerdict::Committed
            ]
        );
    }

    #[test]
    fn parse_multi_rejects_malformed_or_misaligned() {
        // Not ok ⇒ None (whole-flush FlushErr).
        assert!(parse_multi(r#"{"ok":false}"#, 1).is_none());
        // Length mismatch ⇒ None.
        assert!(parse_multi(r#"{"ok":true,"results":[{"ok":true,"acked":1,"error":null}]}"#, 2).is_none());
        // Missing per-row ok ⇒ None (can't trust a partial ack).
        assert!(parse_multi(r#"{"ok":true,"results":[{"acked":1}]}"#, 1).is_none());
        // Missing results ⇒ None.
        assert!(parse_multi(r#"{"ok":true}"#, 0).is_none());
        // Empty flush (n=0) with an empty results array is well-formed.
        assert_eq!(parse_multi(r#"{"ok":true,"results":[]}"#, 0), Some(vec![]));
    }

    #[tokio::test]
    async fn disabled_ack_returns_flush_err_without_shards() {
        // Disabled ⇒ no shard tasks; ack() short-circuits to FlushErr (the handler
        // never calls it — it gates on enabled()).
        let f = AckFusion {
            senders: Vec::new(),
            enabled: false,
        };
        assert!(!f.enabled());
        let v = f
            .ack("p1".into(), "g".into(), "w1".into(), 10, 1)
            .await;
        assert_eq!(v, AckVerdict::FlushErr);
    }
}
