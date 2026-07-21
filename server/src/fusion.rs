use crate::util::{FnvHashMap, FnvHashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::OwnedSemaphorePermit;

use crate::frames::{pack_frames, uuid_bytes_to_string, zstd_compress, FrameIn};
use crate::metrics::Metrics;
use crate::vegas::Vegas;

// ---------------------------------------------------------------------------
// Cross-partition BUNDLING observability (Trap 3b). Every dispatch commits ONE
// multi-segment transaction (one commit / one fsync) covering N disjoint
// partitions, so BUNDLES_FIRED counts commits and SEGMENTS_FIRED counts the
// segments (partitions) inside them. commits << segments is the direct evidence
// that fusion coalesces ACROSS partitions. Set QUEEN_V2_BUNDLE_LOG=1 to also log
// each multi-partition bundle as it commits.
pub static BUNDLES_FIRED: AtomicU64 = AtomicU64::new(0);
pub static SEGMENTS_FIRED: AtomicU64 = AtomicU64::new(0);

fn record_bundle(parts: usize) {
    let commits = BUNDLES_FIRED.fetch_add(1, Ordering::Relaxed) + 1;
    let segments = SEGMENTS_FIRED.fetch_add(parts as u64, Ordering::Relaxed) + parts as u64;
    // Read the env flag once — env::var takes a process-wide lock and allocates,
    // and this runs on every bundle commit (thousands/s under load).
    static LOG_EACH: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let log_each =
        *LOG_EACH.get_or_init(|| std::env::var("QUEEN_V2_BUNDLE_LOG").ok().as_deref() == Some("1"));
    if log_each && parts >= 2 {
        eprintln!("[bundle] committed {parts} partitions in 1 txn");
    }
    // Periodic running summary so the coalescing ratio is visible without
    // per-bundle spam (commits vs segments — segments/commits is the fusion gain).
    if commits % 500 == 0 {
        eprintln!(
            "[bundle] commits={commits} segments={segments} segments_per_commit={:.2}",
            segments as f64 / commits as f64
        );
    }
}

// Per push-request state. The handler parks on `notify` until every segment its
// frames landed in has committed (pending -> 0).
pub struct ItemResult {
    pub message_id: String,
    pub txn: String,
    pub queue: String,
    pub status: &'static str, // "queued" | "duplicate" | "error"
    // Intra-request first-wins dedup (layer 1): a later item that repeats an
    // earlier item's (queue, partition, transactionId) is a follower of that
    // earlier "leader" item. Followers never produce a fusion frame; at render
    // time they copy the leader's FINAL message_id (which may itself have been
    // rewritten to a pre-existing id by the cross-flush dup path).
    pub dup_of: Option<usize>,
}

pub struct PushState {
    pub results: Mutex<Vec<ItemResult>>,
    pub pending: AtomicUsize,
    pub done: Mutex<Option<oneshot::Sender<()>>>,
}

// A single fusion frame. It carries a back-reference to its owning push request
// (`state`) and the item index within that request, so the flush can write each
// frame's verdict (queued/duplicate/error) straight into the right result slot —
// even after frames from many requests fuse into one segment.
pub struct OwnedFrame {
    pub message_id: [u8; 16],
    pub txn: String,
    pub payload: Vec<u8>,
    // Authenticated producer `sub` (from the validated JWT), carried THROUGH
    // fusion so auth-enabled pushes can still coalesce across requests. When set,
    // pack_frames stamps it into the frame (FLAG_PSUB) and pop echoes it back as
    // `producerSub`. None when auth is disabled or the token carried no sub.
    pub producer_sub: Option<String>,
    // RUSTFIX item 8: this frame's payload is already an {encrypted,iv,authTag}
    // envelope (the push handler encrypted it). Carried through so pack_frames sets
    // FLAG_ENCRYPTED on the stored frame.
    pub encrypted: bool,
    pub state: Arc<PushState>,
    pub item: usize,
}

// A partition's accumulating (not-yet-dispatched) segment. Every frame is
// self-describing (it knows its owning request + item), so the flush notifies
// each contributing request exactly once by walking the frames it carried.
struct FusionGroup {
    queue: String,
    partition: String,
    frames: Vec<OwnedFrame>,
}

pub struct AddMsg {
    pub queue: String,
    pub partition: String,
    pub frames: Vec<OwnedFrame>,
}

pub struct Fusion {
    senders: Vec<mpsc::UnboundedSender<AddMsg>>,
}

struct FlushCtx {
    pool: Pool,
    vegas: Arc<Vegas>,
    metrics: Arc<Metrics>,
    zstd_level: i32,
    stmt_timeout: Duration,
}

impl Fusion {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shards: usize,
        pool: Pool,
        vegas: Arc<Vegas>,
        metrics: Arc<Metrics>,
        zstd_level: i32,
        fusion_frames: usize,
        hold_ms: u64,
        stmt_timeout: Duration,
    ) -> Arc<Fusion> {
        // Distinct partitions a single shard will keep in-flight at once (the
        // C++ `QUEEN_PUSH_MAX_PARTITIONS_PER_BATCH` analogue). A fan-out fairness
        // guard so one shard can't monopolise the shared push lane; the real
        // per-partition serialisation is the ≤1-in-flight gate below. Self-tuned
        // default; env override only, not part of the product contract.
        let max_inflight = std::env::var("QUEEN_V2_FUSION_MAX_INFLIGHT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v >= 1)
            .unwrap_or(8);
        // Cross-partition bundle cap K (Trap 3b): the max number of distinct,
        // not-in-flight partitions packed into ONE multi-segment transaction.
        // K auto-tunes to load — the dispatcher bundles as many ready partitions
        // as are queued right now, up to this cap; idle ⇒ a single ready
        // partition fires immediately as a bundle of 1 (fire-on-idle preserved).
        // Under high partition-count load, bundling coalesces many partitions'
        // commits into one, decoupling commit rate from partition count. Internal
        // env override only — NOT part of the product contract.
        let bundle_max = std::env::var("QUEEN_V2_BUNDLE_MAX")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v >= 1)
            .unwrap_or(16);
        // The backstop sweep interval: how often a held (permit-starved) group is
        // retried when neither an arrival nor an in-flight completion woke the
        // shard. Not a latency floor for the common path — fire-on-idle dispatches
        // on arrival and completions re-dispatch immediately.
        let hold_ms = hold_ms.max(1);
        // fusion_frames (QUEEN_V2_FUSION_FRAMES) is retained for API/env compat but
        // is no longer a flush trigger: segment size is now emergent. At low load
        // fire-on-idle flushes one push at a time (segment≈1); under load the next
        // segment accumulates for exactly one in-flight flush RTT, so segments grow
        // with offered load with no fixed frame threshold. See shard_loop.
        let _ = fusion_frames;
        let mut senders = Vec::with_capacity(shards);
        for _ in 0..shards {
            let (tx, rx) = mpsc::unbounded_channel::<AddMsg>();
            senders.push(tx);
            let ctx = Arc::new(FlushCtx {
                pool: pool.clone(),
                vegas: vegas.clone(),
                metrics: metrics.clone(),
                zstd_level,
                stmt_timeout,
            });
            tokio::spawn(shard_loop(rx, ctx, hold_ms, max_inflight, bundle_max));
        }
        Arc::new(Fusion { senders })
    }

    pub fn submit(&self, msg: AddMsg) {
        let mut h: u64 = 1469598103934665603;
        for b in msg.queue.as_bytes().iter().chain(msg.partition.as_bytes()) {
            h ^= *b as u64;
            h = h.wrapping_mul(1099511628211);
        }
        let idx = (h as usize) % self.senders.len();
        let _ = self.senders[idx].send(msg);
    }
}

// One shard owns a disjoint set of (queue,partition) keys — Fusion::submit hashes
// each key to a fixed shard — so all state below (accumulating groups + the
// in-flight set) is single-task-local and needs no locking.
//
// Fire-on-idle (Trap 1): on arrival, if the push lane is uncontended (under the
// fan-out cap and a push Vegas permit is immediately free) the ready partition
// flushes IMMEDIATELY — no hold timer — as a bundle of 1, so a single/low-rate
// push commits in ~one DB RTT, not ~hold ms.
//
// Per-partition in-flight gate (Trap 3): at most ONE flush per (queue,partition)
// is ever in flight. While a partition's flush is in flight, its next segment
// keeps accumulating and is dispatched only after the previous flush COMMITS
// (the completion signal). Two flushes therefore never race on
// `UPDATE seg_partitions SET last_seq=last_seq+1`, so `Lock:transactionid`
// stalls disappear and per-partition seq order == commit order.
//
// Cross-partition BUNDLING (Trap 3b): each dispatch collects up to K =
// `bundle_max` ready, NOT-in-flight (hence disjoint) partitions and commits all
// of their segments in ONE `seg_push_segments_multi_v1` transaction (one commit /
// one fsync for N partitions). The in-flight gate IS the bundle selector — it
// already guarantees the chosen partitions are disjoint. K auto-tunes to load:
// idle ⇒ one ready partition ⇒ a bundle of 1 (fire-on-idle); busy/high partition
// count ⇒ many ready partitions accumulate while permits are scarce, then a
// freed permit fires a large bundle — decoupling commit rate from partition
// count. The per-bundle result array is demuxed back to each partition's
// contributing requests.
async fn shard_loop(
    mut rx: mpsc::UnboundedReceiver<AddMsg>,
    ctx: Arc<FlushCtx>,
    hold_ms: u64,
    max_inflight: usize,
    bundle_max: usize,
) {
    let mut groups: FnvHashMap<String, FusionGroup> = FnvHashMap::default();
    let mut inflight: FnvHashSet<String> = FnvHashSet::default();
    // Concurrently in-flight bundles for THIS shard (each holds one push Vegas
    // permit and commits one multi-segment transaction). The fan-out fairness cap
    // (`max_inflight`) now bounds concurrent bundles, not partitions — a single
    // bundle may already touch up to `bundle_max` partitions.
    let mut inflight_bundles: usize = 0;
    // Completed bundles report their partition keys back here so the shard can
    // release each partition's in-flight slot and dispatch its accumulated next
    // segment. Held for the whole loop, so the receiver never closes while we run.
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<Vec<String>>();
    let mut tick = tokio::time::interval(Duration::from_millis(hold_ms));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            maybe = rx.recv() => {
                let Some(msg) = maybe else { break; };
                let key = format!("{}\x1f{}", msg.queue, msg.partition);
                let g = groups.entry(key.clone()).or_insert_with(|| FusionGroup {
                    queue: msg.queue.clone(),
                    partition: msg.partition.clone(),
                    frames: Vec::new(),
                });
                g.frames.extend(msg.frames);
                // Fire-on-idle: if a permit is free, bundle every ready partition
                // (usually just this one when idle) and dispatch now; otherwise the
                // groups stay and accumulate (dispatched on the next completion/sweep).
                try_dispatch_bundle(&mut groups, &mut inflight, &mut inflight_bundles,
                    max_inflight, bundle_max, &ctx, &done_tx);
            }
            Some(keys) = done_rx.recv() => {
                for k in &keys {
                    inflight.remove(k);
                }
                inflight_bundles = inflight_bundles.saturating_sub(1);
                // The freed permit + released partitions can now form new bundles
                // over everything currently accumulated (including these partitions'
                // next segments — which necessarily commit AFTER the bundle that
                // just committed, so per-partition seq order == commit order).
                sweep_bundles(&mut groups, &mut inflight, &mut inflight_bundles,
                    max_inflight, bundle_max, &ctx, &done_tx);
            }
            _ = tick.tick() => {
                // Liveness backstop: retry held groups whose dispatch depends on a
                // permit freed on another shard (the push Vegas lane is shared),
                // which no local arrival/completion would have woken us for.
                sweep_bundles(&mut groups, &mut inflight, &mut inflight_bundles,
                    max_inflight, bundle_max, &ctx, &done_tx);
            }
        }
    }
}

// Form and dispatch ONE bundle now. Returns true if a bundle was fired. Gates, in
// order: (1) the per-shard concurrent-bundle fan-out cap; (2) at least one ready,
// not-in-flight partition exists; (3) a push Vegas permit must be immediately
// available — its absence means the lane is contended, so we hold and accumulate
// (Trap 1 fallback). The chosen partitions are exactly `take_front_disjoint`: up
// to `bundle_max` ready groups whose partitions are not already in flight (hence
// mutually disjoint), each marked in-flight for the bundle's duration.
#[allow(clippy::too_many_arguments)]
fn try_dispatch_bundle(
    groups: &mut FnvHashMap<String, FusionGroup>,
    inflight: &mut FnvHashSet<String>,
    inflight_bundles: &mut usize,
    max_inflight: usize,
    bundle_max: usize,
    ctx: &Arc<FlushCtx>,
    done: &mpsc::UnboundedSender<Vec<String>>,
) -> bool {
    if *inflight_bundles >= max_inflight {
        return false;
    }
    // Collect up to K ready, not-in-flight partition keys (disjoint by the gate).
    let mut chosen: Vec<String> = Vec::with_capacity(bundle_max);
    for (k, g) in groups.iter() {
        if chosen.len() >= bundle_max {
            break;
        }
        if !g.frames.is_empty() && !inflight.contains(k) {
            chosen.push(k.clone());
        }
    }
    if chosen.is_empty() {
        return false;
    }
    // One permit for the whole bundle: one DB op / one commit for all N segments.
    let permit = match ctx.vegas.try_acquire() {
        Some(p) => p,
        None => return false,
    };
    let mut bundle: Vec<FusionGroup> = Vec::with_capacity(chosen.len());
    for k in &chosen {
        let g = groups.remove(k).expect("group present (checked)");
        inflight.insert(k.clone());
        bundle.push(g);
    }
    *inflight_bundles += 1;
    spawn_bundle_flush(ctx.clone(), chosen, bundle, permit, done.clone());
    true
}

// Keep forming bundles until no permit/slot is free or nothing is ready. Used on
// a completion and on the backstop tick. Each iteration fires one bundle of up to
// `bundle_max` distinct partitions.
#[allow(clippy::too_many_arguments)]
fn sweep_bundles(
    groups: &mut FnvHashMap<String, FusionGroup>,
    inflight: &mut FnvHashSet<String>,
    inflight_bundles: &mut usize,
    max_inflight: usize,
    bundle_max: usize,
    ctx: &Arc<FlushCtx>,
    done: &mpsc::UnboundedSender<Vec<String>>,
) {
    while *inflight_bundles < max_inflight {
        if !try_dispatch_bundle(groups, inflight, inflight_bundles, max_inflight, bundle_max, ctx, done) {
            break;
        }
    }
}

// A per-group working state inside a bundle flush: the layer-2 (intra-flush)
// dedup result plus the still-to-push frame set and per-frame verdicts. Each
// group is a DISTINCT partition; its results/pending accounting is done
// independently, and its owning requests are decremented exactly ONCE for this
// group (the handler set PushState.pending = number of (queue,partition) groups).
struct BundleGroup {
    group: FusionGroup,
    leader_idx: Vec<usize>,                     // per-frame txn-leader index
    pending: Vec<usize>,                        // kept frame indices still to push
    verdict: FnvHashMap<usize, (&'static str, String)>,
    total: usize,
    db_ok: bool,
}

// Layer 2: intra-flush dedup by transactionId (first-wins). The first occurrence
// of each txn is the "leader" (the only frame that reaches the segment); every
// later same-txn frame is a follower that inherits the leader's final verdict.
fn layer2_dedup(frames: &[OwnedFrame]) -> (Vec<usize>, Vec<usize>) {
    let mut leader_of_txn: FnvHashMap<&str, usize> = FnvHashMap::default();
    let mut leader_idx: Vec<usize> = Vec::with_capacity(frames.len());
    let mut kept: Vec<usize> = Vec::new();
    for (i, f) in frames.iter().enumerate() {
        if let Some(&l) = leader_of_txn.get(f.txn.as_str()) {
            leader_idx.push(l);
        } else {
            leader_of_txn.insert(f.txn.as_str(), i);
            leader_idx.push(i);
            kept.push(i);
        }
    }
    (leader_idx, kept)
}

// Build the metas JSON + zstd blob for a group's current `pending` subset. meta
// "i" is the position WITHIN this subset — the same index the SP echoes in dups
// and records as frame_idx, matching the packed blob layout.
fn build_metas_and_blob(group: &FusionGroup, pending: &[usize], zstd_level: i32) -> (String, Vec<u8>) {
    let mut metas = String::with_capacity(pending.len() * 80 + 2);
    metas.push('[');
    for (pos, &fi) in pending.iter().enumerate() {
        if pos > 0 {
            metas.push(',');
        }
        let f = &group.frames[fi];
        metas.push_str("{\"i\":");
        metas.push_str(&pos.to_string());
        metas.push_str(",\"mid\":\"");
        metas.push_str(&uuid_bytes_to_string(&f.message_id));
        metas.push_str("\",\"txn\":\"");
        json_escape_into(&mut metas, &f.txn);
        metas.push_str("\"}");
    }
    metas.push(']');

    let fins: Vec<FrameIn> = pending
        .iter()
        .map(|&fi| {
            let f = &group.frames[fi];
            FrameIn {
                message_id: f.message_id,
                txn: &f.txn,
                trace_id: None,
                // Stamp the authenticated producer sub into the packed frame
                // (FLAG_PSUB) so it survives fusion and is readable at pop.
                producer_sub: f.producer_sub.as_deref(),
                payload: &f.payload,
                // RUSTFIX item 8: propagate the frame's encrypted flag (crypto was
                // done at the push handler; fusion only carries the bit).
                encrypted: f.encrypted,
            }
        })
        .collect();
    let blob = zstd_compress(&pack_frames(&fins), zstd_level);
    (metas, blob)
}

// Push a bundle of N segments (distinct partitions) in ONE multi-segment
// transaction via queen.seg_push_segments_multi_v1. `segments_json` is the input
// JSON array; `blobs` are the aligned zstd blobs bound as NATIVE binary bytea[]
// (Vec<Vec<u8>> -> bytea[]), dropping the base64 text round-trip the single-
// segment wire wrapper uses. Returns the per-segment result array JSON as text,
// index-aligned with the input.
async fn push_segments_multi(
    client: &deadpool_postgres::Client,
    segments_json: &str,
    blobs: Vec<Vec<u8>>,
) -> Result<String, tokio_postgres::Error> {
    // $1::text::jsonb pins the segments param to TEXT so a &str binds; $2 is bound
    // as the native bytea[] the function expects (no cast, no base64).
    let stmt = "SELECT (queen.seg_push_segments_multi_v1($1::text::jsonb, $2))::text";
    let row = client.query_one(stmt, &[&segments_json, &blobs]).await?;
    Ok(row.get(0))
}

// Flush ONE bundle of N disjoint partitions as a single committed transaction.
// The push Vegas permit (acquired by the shard loop so availability can gate
// fire-on-idle) is held until the DB op returns. On completion the bundle's
// partition keys are sent back so the shard releases each in-flight slot and
// dispatches each partition's accumulated next segment — the signal is sent AFTER
// commit, so a partition's next flush (hence its `last_seq` allocation) strictly
// follows this one's commit (per-partition seq order == commit order).
fn spawn_bundle_flush(
    ctx: Arc<FlushCtx>,
    keys: Vec<String>,
    bundle: Vec<FusionGroup>,
    permit: OwnedSemaphorePermit,
    done: mpsc::UnboundedSender<Vec<String>>,
) {
    tokio::spawn(async move {
        // Per-group layer-2 dedup up front; pending starts as the kept leaders.
        let mut gs: Vec<BundleGroup> = bundle
            .into_iter()
            .map(|group| {
                let total = group.frames.len();
                let (leader_idx, kept) = layer2_dedup(&group.frames);
                BundleGroup {
                    group,
                    leader_idx,
                    pending: kept,
                    verdict: FnvHashMap::default(),
                    total,
                    db_ok: true,
                }
            })
            .collect();

        let n_parts = gs.len();
        let sum_total: usize = gs.iter().map(|g| g.total).sum();
        let t0 = Instant::now();

        // Retry loop: every iteration commits ONE multi-segment transaction over
        // the groups that still have pending frames. A group returns `queued`
        // (all its pending frames committed) or `duplicate` (its whole segment
        // rolled back on its own savepoint — the surviving non-dup frames are
        // repacked and retried in a fresh transaction). Queued groups drop out; a
        // no-dup bundle finishes in exactly one iteration (one commit). Bounded
        // defensively by total frames + 2.
        for _attempt in 0..(sum_total + 2) {
            let participants: Vec<usize> = gs
                .iter()
                .enumerate()
                .filter(|(_, g)| !g.pending.is_empty())
                .map(|(i, _)| i)
                .collect();
            if participants.is_empty() {
                break;
            }

            // Build the index-aligned {segments JSON, blobs} for the participants.
            let mut segs = String::with_capacity(participants.len() * 160);
            segs.push('[');
            let mut blobs: Vec<Vec<u8>> = Vec::with_capacity(participants.len());
            for (pos, &gi) in participants.iter().enumerate() {
                if pos > 0 {
                    segs.push(',');
                }
                let g = &gs[gi];
                let (metas, blob) = build_metas_and_blob(&g.group, &g.pending, ctx.zstd_level);
                segs.push_str("{\"queue\":\"");
                json_escape_into(&mut segs, &g.group.queue);
                segs.push_str("\",\"partition\":\"");
                json_escape_into(&mut segs, &g.group.partition);
                segs.push_str("\",\"metas\":");
                segs.push_str(&metas);
                segs.push_str(",\"msg_count\":");
                segs.push_str(&g.pending.len().to_string());
                segs.push('}');
                blobs.push(blob);
            }
            segs.push(']');

            let outcome = match ctx.pool.get().await {
                Ok(client) => match tokio::time::timeout(
                    ctx.stmt_timeout,
                    push_segments_multi(&client, &segs, blobs),
                )
                .await
                {
                    Ok(Ok(txt)) => parse_multi_outcome(&txt, participants.len()),
                    Ok(Err(e)) => {
                        eprintln!("[bundle] push_segments_multi error parts={}: {}", participants.len(), e);
                        None
                    }
                    Err(_) => {
                        eprintln!("[bundle] push_segments_multi timeout parts={}", participants.len());
                        None
                    }
                },
                Err(e) => {
                    eprintln!("[bundle] pool.get error: {}", e);
                    None
                }
            };

            let Some(results) = outcome else {
                // The WHOLE transaction failed (infra error / timeout / malformed
                // response) — nothing committed. Every participant's remaining
                // frames resolve to "error"; stop retrying.
                for &gi in &participants {
                    gs[gi].db_ok = false;
                    gs[gi].pending.clear();
                }
                break;
            };

            for (pos, &gi) in participants.iter().enumerate() {
                match &results[pos] {
                    PushOutcome::Queued => {
                        let g = &mut gs[gi];
                        for &fi in &g.pending {
                            let mid = uuid_bytes_to_string(&g.group.frames[fi].message_id);
                            g.verdict.insert(fi, ("queued", mid));
                        }
                        g.pending.clear();
                    }
                    PushOutcome::Duplicate(dups) => {
                        let g = &mut gs[gi];
                        // dups: (pos-in-pending, original message_id). Mark those
                        // duplicate, then repack+retry the survivors.
                        let mut is_dup = vec![false; g.pending.len()];
                        for (i, mid) in dups {
                            if *i < g.pending.len() {
                                is_dup[*i] = true;
                                g.verdict.insert(g.pending[*i], ("duplicate", mid.clone()));
                            }
                        }
                        let survivors: Vec<usize> = g
                            .pending
                            .iter()
                            .enumerate()
                            .filter(|(p, _)| !is_dup[*p])
                            .map(|(_, &fi)| fi)
                            .collect();
                        // A duplicate response must resolve at least one frame; if
                        // it resolved none, stop rather than spin (survivors -> error).
                        if survivors.len() == g.pending.len() {
                            g.db_ok = false;
                            g.pending.clear();
                        } else {
                            g.pending = survivors;
                        }
                    }
                }
            }
        }
        let rtt = t0.elapsed();
        // Release the push permit BEFORE signalling completion so the shard sees
        // the permit free when it re-dispatches these partitions' next segments.
        drop(permit);
        ctx.vegas.record(rtt);
        // Per-segment metrics (one record per partition preserves the existing
        // "avg frames per segment" prometheus semantics); the bundle counter below
        // captures the commits-vs-segments coalescing gain.
        for g in &gs {
            ctx.metrics.push.record_batch(g.total, g.db_ok, rtt);
        }
        record_bundle(n_parts);

        // ---- Assign every frame's result, PER GROUP, grouped by owning request.
        // Critical: decrement each owning request's pending counter exactly ONCE
        // PER GROUP (never once per bundle) — a request that pushed to several of
        // this bundle's partitions set PushState.pending = its group count, so it
        // must be signalled once for each of those groups here.
        for g in gs {
            let mut per_state: FnvHashMap<usize, (Arc<PushState>, Vec<(usize, &'static str, String)>)> =
                FnvHashMap::default();
            for (i, f) in g.group.frames.iter().enumerate() {
                let l = g.leader_idx[i];
                let (st, mid): (&'static str, String) = match g.verdict.get(&l) {
                    Some(v) => {
                        let (s, m) = (v.0, &v.1);
                        if i == l {
                            (s, m.clone())
                        } else if s == "error" {
                            ("error", m.clone())
                        } else {
                            ("duplicate", m.clone())
                        }
                    }
                    None => ("error", uuid_bytes_to_string(&f.message_id)),
                };
                let sk = Arc::as_ptr(&f.state) as usize;
                per_state
                    .entry(sk)
                    .or_insert_with(|| (f.state.clone(), Vec::new()))
                    .1
                    .push((f.item, st, mid));
            }

            for (_, (state, writes)) in per_state {
                {
                    let mut r = state.results.lock().unwrap();
                    for (item, st, mid) in writes {
                        if item < r.len() {
                            r[item].status = st;
                            r[item].message_id = mid;
                        }
                    }
                }
                if state.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
                    if let Some(tx) = state.done.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                }
            }
        }

        // Release every bundled partition's in-flight slot (fires even on
        // error/timeout, so a failed bundle can't wedge a partition — the DB op is
        // bounded by stmt_timeout). Best-effort: if the shard exited, drop it.
        let _ = done.send(keys);
    });
}

// Parsed outcome of one segment within a seg_push_segments_multi_v1 result array.
enum PushOutcome {
    Queued,
    // (frame index within the sent metas, original message_id) for each dup.
    Duplicate(Vec<(usize, String)>),
}

// Parse ONE segment result object. {"status":"queued",...} -> Queued;
// {"status":"duplicate","dups":[{"i":..,"mid":".."}]} -> Duplicate. None on an
// unexpected status (treated as a whole-bundle error by the caller).
fn parse_one_outcome(v: &serde_json::Value) -> Option<PushOutcome> {
    match v.get("status").and_then(|s| s.as_str()) {
        Some("queued") => Some(PushOutcome::Queued),
        Some("duplicate") => {
            let mut dups = Vec::new();
            if let Some(arr) = v.get("dups").and_then(|d| d.as_array()) {
                for d in arr {
                    let i = d.get("i").and_then(|x| x.as_i64());
                    let mid = d.get("mid").and_then(|x| x.as_str());
                    if let (Some(i), Some(mid)) = (i, mid) {
                        if i >= 0 {
                            dups.push((i as usize, mid.to_string()));
                        }
                    }
                }
            }
            Some(PushOutcome::Duplicate(dups))
        }
        _ => None,
    }
}

// Parse the multi-segment SP result: a JSON array of per-segment outcomes, in
// the same order as the input segments. Returns None (whole-bundle failure) on a
// parse error, a length mismatch, or any unexpected element status, so the caller
// treats the transaction as failed and resolves every participant to "error".
fn parse_multi_outcome(txt: &str, expected: usize) -> Option<Vec<PushOutcome>> {
    let v: serde_json::Value = serde_json::from_str(txt).ok()?;
    let arr = v.as_array()?;
    if arr.len() != expected {
        return None;
    }
    let mut out = Vec::with_capacity(arr.len());
    for e in arr {
        out.push(parse_one_outcome(e)?);
    }
    Some(out)
}

pub fn json_escape_into(out: &mut String, s: &str) {
    // Byte-scan fast path: every byte needing an escape ('"', '\\', <0x20) is
    // ASCII, and multi-byte UTF-8 units are all >= 0x80, so splitting the string
    // only at escape bytes always lands on char boundaries. Clean runs (the
    // overwhelmingly common case — UUIDs, txn ids, ISO timestamps) are appended
    // wholesale instead of char-by-char.
    let b = s.as_bytes();
    let mut start = 0;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'"' || c == b'\\' || c < 0x20 {
            if start < i {
                out.push_str(&s[start..i]);
            }
            match c {
                b'"' => out.push_str("\\\""),
                b'\\' => out.push_str("\\\\"),
                b'\n' => out.push_str("\\n"),
                b'\r' => out.push_str("\\r"),
                b'\t' => out.push_str("\\t"),
                _ => {
                    out.push_str("\\u00");
                    out.push(char::from(b"0123456789abcdef"[(c >> 4) as usize]));
                    out.push(char::from(b"0123456789abcdef"[(c & 0x0f) as usize]));
                }
            }
            start = i + 1;
        }
        i += 1;
    }
    if start < b.len() {
        out.push_str(&s[start..]);
    }
}

#[cfg(test)]
mod tests {
    use super::json_escape_into;

    // Reference implementation (the original char-by-char escape) — the
    // byte-scan fast path must match it byte-for-byte.
    fn escape_ref(s: &str) -> String {
        let mut out = String::new();
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
                c => out.push(c),
            }
        }
        out
    }

    #[test]
    fn escape_matches_reference() {
        let cases = [
            "",
            "plain",
            "0198f0a4-7b1e-7c3d-9a2b-1c4d5e6f7a8b",
            "with \"quotes\" and \\backslash\\",
            "line\nbreak\r\ttab",
            "\u{1}\u{2}\u{1f}",
            "unicode: èàò 日本語 🚀 mixed \" with 中文\\",
            "trailing escape\\",
            "\"leading",
        ];
        for c in cases {
            let mut got = String::new();
            json_escape_into(&mut got, c);
            assert_eq!(got, escape_ref(c), "mismatch for {c:?}");
        }
    }
}
