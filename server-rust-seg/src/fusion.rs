use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::OwnedSemaphorePermit;

use crate::db;
use crate::frames::{pack_frames, uuid_bytes_to_string, zstd_compress, FrameIn};
use crate::metrics::Metrics;
use crate::vegas::Vegas;

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
            tokio::spawn(shard_loop(rx, ctx, hold_ms, max_inflight));
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
// Fire-on-idle (Trap 1): on arrival, if the push lane is uncontended for this
// partition (no in-flight flush for it, under the fan-out cap, and a push Vegas
// permit is immediately free) the group flushes IMMEDIATELY — no hold timer — so
// a single/low-rate push commits in ~one DB RTT, not ~hold ms.
//
// Per-partition in-flight gate (Trap 3): at most ONE flush per (queue,partition)
// is ever in flight. While a partition's flush is in flight, its next segment
// keeps accumulating and is dispatched only after the previous flush COMMITS
// (the completion signal). Two flushes therefore never race on
// `UPDATE seg_partitions SET last_seq=last_seq+1`, so `Lock:transactionid`
// stalls disappear and per-partition seq order == commit order.
async fn shard_loop(
    mut rx: mpsc::UnboundedReceiver<AddMsg>,
    ctx: Arc<FlushCtx>,
    hold_ms: u64,
    max_inflight: usize,
) {
    let mut groups: HashMap<String, FusionGroup> = HashMap::new();
    let mut inflight: HashSet<String> = HashSet::new();
    // Completed flushes report their key back here so the shard can release the
    // in-flight slot and dispatch the accumulated next segment. Held for the whole
    // loop, so the receiver never closes while we run.
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<String>();
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
                // Fire-on-idle when the lane is uncontended; otherwise the group
                // stays and accumulates (dispatched on the next completion/sweep).
                try_dispatch(&key, &mut groups, &mut inflight, max_inflight, &ctx, &done_tx);
            }
            Some(key) = done_rx.recv() => {
                inflight.remove(&key);
                // Dispatch this partition's accumulated next segment FIRST — it must
                // commit strictly after the one that just committed (seq order) —
                // then let the freed permit/slot serve other held partitions.
                try_dispatch(&key, &mut groups, &mut inflight, max_inflight, &ctx, &done_tx);
                sweep(&mut groups, &mut inflight, max_inflight, &ctx, &done_tx);
            }
            _ = tick.tick() => {
                // Liveness backstop: retry held groups whose dispatch depends on a
                // permit freed on another shard (the push Vegas lane is shared),
                // which no local arrival/completion would have woken us for.
                sweep(&mut groups, &mut inflight, max_inflight, &ctx, &done_tx);
            }
        }
    }
}

// Attempt to flush `key`'s accumulated segment now. Returns true if dispatched.
// Gates, in order: (1) never a second in-flight flush for the same partition
// (Trap 3); (2) the per-shard distinct-partition fan-out cap; (3) a push Vegas
// permit must be immediately available — its absence means the lane is contended,
// so we hold and accumulate (Trap 1 fallback).
fn try_dispatch(
    key: &str,
    groups: &mut HashMap<String, FusionGroup>,
    inflight: &mut HashSet<String>,
    max_inflight: usize,
    ctx: &Arc<FlushCtx>,
    done: &mpsc::UnboundedSender<String>,
) -> bool {
    if inflight.contains(key) {
        return false;
    }
    if inflight.len() >= max_inflight {
        return false;
    }
    match groups.get(key) {
        Some(g) if !g.frames.is_empty() => {}
        _ => return false,
    }
    let permit = match ctx.vegas.try_acquire() {
        Some(p) => p,
        None => return false,
    };
    let grp = groups.remove(key).expect("group present (checked)");
    inflight.insert(key.to_string());
    spawn_flush(ctx.clone(), key.to_string(), grp, permit, done.clone());
    true
}

// Try to dispatch every held, non-in-flight group (greedy, permit-gated). Used on
// a completion and on the backstop tick.
fn sweep(
    groups: &mut HashMap<String, FusionGroup>,
    inflight: &mut HashSet<String>,
    max_inflight: usize,
    ctx: &Arc<FlushCtx>,
    done: &mpsc::UnboundedSender<String>,
) {
    if inflight.len() >= max_inflight {
        return;
    }
    let keys: Vec<String> = groups
        .iter()
        .filter(|(k, g)| !g.frames.is_empty() && !inflight.contains(*k))
        .map(|(k, _)| k.clone())
        .collect();
    for k in keys {
        if inflight.len() >= max_inflight {
            break;
        }
        try_dispatch(&k, groups, inflight, max_inflight, ctx, done);
    }
}

// Flush one segment. The push Vegas permit is acquired by the shard loop (so
// permit availability can gate fire-on-idle) and handed in here; it is released
// only after the DB op returns. On completion the key is sent back so the shard
// releases the partition's in-flight slot and dispatches its accumulated next
// segment — the signal is sent AFTER commit, so the next segment's flush (hence
// its `last_seq` allocation) strictly follows this one's commit.
fn spawn_flush(
    ctx: Arc<FlushCtx>,
    key: String,
    group: FusionGroup,
    permit: OwnedSemaphorePermit,
    done: mpsc::UnboundedSender<String>,
) {
    tokio::spawn(async move {
        let total = group.frames.len();

        // ---- Layer 2: intra-flush dedup by transactionId (first-wins) ---------
        // Frames from concurrent same-(queue,partition,txn) requests fuse into one
        // group. Collapse them: the first occurrence of each txn is the "leader"
        // (the only frame that reaches the segment); every later same-txn frame is
        // a follower that inherits the leader's final verdict/message_id.
        let mut leader_of_txn: HashMap<&str, usize> = HashMap::new();
        let mut leader_idx: Vec<usize> = Vec::with_capacity(total);
        let mut kept: Vec<usize> = Vec::new();
        for (i, f) in group.frames.iter().enumerate() {
            if let Some(&l) = leader_of_txn.get(f.txn.as_str()) {
                leader_idx.push(l);
            } else {
                leader_of_txn.insert(f.txn.as_str(), i);
                leader_idx.push(i);
                kept.push(i);
            }
        }

        // ---- Layer 3: push kept frames; on cross-flush dup, mark + repack ------
        // The wire SP wrote NOTHING on a unique_violation and returned the dup
        // frame indices + the ORIGINAL message ids. Mark those duplicate, drop
        // them, and repack+retry the survivors so genuinely-new frames still
        // commit as queued. The per-partition <=1-in-flight gate means no other
        // flush for this partition races us, so one retry normally suffices; the
        // loop is bounded defensively.
        //
        // verdict[frame_idx] = (status, final message_id).
        let mut verdict: HashMap<usize, (&'static str, String)> = HashMap::new();
        let mut pending: Vec<usize> = kept;
        let mut db_ok = true;
        let t0 = Instant::now();

        for _attempt in 0..(total + 2) {
            if pending.is_empty() {
                break;
            }
            // Build metas + blob for the current `pending` subset. meta "i" is the
            // position WITHIN this subset — the same index the SP echoes in dups
            // and the frame_idx it records, matching the repacked blob layout.
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
                        producer_sub: None,
                        payload: &f.payload,
                        encrypted: false,
                    }
                })
                .collect();
            let blob = zstd_compress(&pack_frames(&fins), ctx.zstd_level);
            let cnt = pending.len() as i32;

            let outcome = match ctx.pool.get().await {
                Ok(client) => match tokio::time::timeout(
                    ctx.stmt_timeout,
                    db::push_segment(&client, &group.queue, &group.partition, &metas, &blob, cnt),
                )
                .await
                {
                    Ok(Ok(txt)) => parse_push_outcome(&txt),
                    Ok(Err(e)) => {
                        eprintln!("[flush] push_segment error q={} p={}: {}", group.queue, group.partition, e);
                        None
                    }
                    Err(_) => {
                        eprintln!("[flush] push_segment timeout q={} p={}", group.queue, group.partition);
                        None
                    }
                },
                Err(e) => {
                    eprintln!("[flush] pool.get error: {}", e);
                    None
                }
            };

            match outcome {
                Some(PushOutcome::Queued) => {
                    for &fi in &pending {
                        verdict.insert(fi, ("queued", uuid_bytes_to_string(&group.frames[fi].message_id)));
                    }
                    pending.clear();
                    break;
                }
                Some(PushOutcome::Duplicate(dups)) => {
                    // dups: (pos-in-pending, original message_id). Mark those
                    // duplicate, then repack+retry the survivors.
                    let mut is_dup = vec![false; pending.len()];
                    for (pos, mid) in dups {
                        if pos < pending.len() {
                            is_dup[pos] = true;
                            verdict.insert(pending[pos], ("duplicate", mid));
                        }
                    }
                    let survivors: Vec<usize> = pending
                        .iter()
                        .enumerate()
                        .filter(|(pos, _)| !is_dup[*pos])
                        .map(|(_, &fi)| fi)
                        .collect();
                    // A duplicate response must resolve at least one frame; if it
                    // resolved none, stop rather than spin (survivors -> error).
                    if survivors.len() == pending.len() {
                        db_ok = false;
                        break;
                    }
                    pending = survivors;
                }
                None => {
                    db_ok = false;
                    break;
                }
            }
        }
        let rtt = t0.elapsed();
        // Release the push permit BEFORE signalling completion so the shard sees
        // the permit free when it re-dispatches this partition's next segment.
        drop(permit);
        ctx.vegas.record(rtt);
        ctx.metrics.push.record_batch(total, db_ok, rtt);
        // Frames still in `pending` (DB failure / retry cap) have no verdict and
        // resolve to "error" below.

        // ---- Assign every frame's result, grouped by owning request ----------
        // A follower copies its leader's FINAL verdict: "duplicate" (or "error")
        // onto the leader's final message_id. Leaders take their own verdict.
        // Group writes by request so each request's results mutex is locked once
        // and its pending counter is decremented exactly once per flush.
        let mut per_state: HashMap<usize, (Arc<PushState>, Vec<(usize, &'static str, String)>)> =
            HashMap::new();
        for (i, f) in group.frames.iter().enumerate() {
            let l = leader_idx[i];
            let (st, mid): (&'static str, String) = match verdict.get(&l) {
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

        // Release the partition's in-flight slot (fires even on error/timeout, so a
        // failed flush can't wedge the partition — the DB op is bounded by
        // stmt_timeout). Best-effort: if the shard already exited, drop it.
        let _ = done.send(key);
    });
}

// Parsed outcome of one seg_push_segment_wire_v1 call.
enum PushOutcome {
    Queued,
    // (frame index within the sent metas, original message_id) for each dup.
    Duplicate(Vec<(usize, String)>),
}

// Parse the wire SP result JSON. None on a parse failure / unexpected status
// (treated as a flush error by the caller). {"status":"queued",...} -> Queued;
// {"status":"duplicate","dups":[{"i":..,"mid":".."}]} -> Duplicate.
fn parse_push_outcome(txt: &str) -> Option<PushOutcome> {
    let v: serde_json::Value = serde_json::from_str(txt).ok()?;
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

pub fn json_escape_into(out: &mut String, s: &str) {
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
}
