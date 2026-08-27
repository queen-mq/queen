use crate::util::{now_epoch_ms, parse_iso_ms, txn_hash128, FnvHashMap, FnvHashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_postgres::NoTls;

use crate::dedup::{DedupCache, PushCheck};
// `pack_frames` and `zstd_compress` are no longer named here: the segment recipe
// moved to `frames::pack_segment_with_hashes` (see build_hashes_and_blob), which
// this module now calls by full path.
use crate::frames::{unpack_frames_ref, uuid_bytes_to_string, zstd_decompress, FrameIn};
use crate::metrics::Metrics;
use crate::admission::{Admission, Lane};

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
    let log_each = *LOG_EACH.get_or_init(|| crate::config::env_bool("QUEEN_V2_BUNDLE_LOG", false));
    if log_each && parts >= 2 {
        tracing::debug!(target: "fusion", parts, "bundle committed partitions in 1 txn");
    }
    // Periodic running summary so the coalescing ratio is visible without
    // per-bundle spam (commits vs segments — segments/commits is the fusion gain).
    static BUNDLE_SUMMARY: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
    if BUNDLE_SUMMARY.tick_now().is_some() {
        tracing::info!(
            target: "fusion",
            commits,
            segments,
            segments_per_commit = segments as f64 / commits as f64,
            "bundle summary"
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
    // Zero-copy: for a plaintext push this is a refcounted slice of the HTTP
    // request body (Bytes::slice_ref) — no per-message payload copy on ingest,
    // and dropping a flushed bundle decrements refcounts instead of freeing
    // one allocation per message. Encrypted payloads own their envelope.
    pub payload: bytes::Bytes,
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
    // Track B (§5): the tenant this group's segment belongs to (all frames in a
    // group share it — the group key includes it).
    tenant: String,
    frames: Vec<OwnedFrame>,
    // Monotonic arrival stamp of this group's FIRST frame (set when the group is
    // created and reused for its whole accumulation). Only consulted by the
    // fat-batch FLOOR (below) to age a below-threshold segment against
    // MIN_WAIT_MS; when the floor is OFF it is written but never read.
    first_frame: Instant,
}

// ---------------------------------------------------------------------------
// FAT-BATCH FLOOR (experiment flag; default OFF — see Fusion::new for the env
// wiring and the measurement that motivates it).
//
// Fire-on-idle makes a segment's size EMERGENT: a partition's accumulating
// segment is dispatched the instant the partition has no flush in flight, so the
// segment is exactly whatever landed during the previous flush's RTT. In the
// thin equilibrium a short RTT ⇒ small segments ⇒ more commits ⇒ higher per-msg
// cost, because throughput = WAL_flush_rate × msgs/flush and each fdatasync is
// ~fixed (~128µs on the VM). Measured 2026-07-22: across identical runs
// msgs/commit swung 39 ↔ 99 and throughput tracked it 1:1 (663k vs 845k msg/s
// per side). The FLOOR lets an operator DELAY dispatch of a still-small segment
// by a bounded time so segments fatten deterministically toward a target
// msgs/commit, trading a little worst-case latency (capped at MIN_WAIT_MS) for a
// higher, steadier msgs/commit. This is a knob for experiments, not a product
// contract — with MIN_FRAMES=0 (the default) every predicate here short-circuits
// to the pre-floor behavior and the binary is byte-identical.
#[derive(Clone, Copy)]
struct Floor {
    // Frame count a group must reach to be dispatch-eligible early. 0 = OFF.
    min_frames: usize,
    // Max time a below-threshold group may be held from its first frame's arrival
    // before it dispatches anyway (the bounded-latency escape). Only consulted
    // when min_frames > 0.
    min_wait: Duration,
}

impl Floor {
    #[inline]
    fn enabled(&self) -> bool {
        self.min_frames > 0
    }

    // Is a group dispatch-ELIGIBLE right now? OFF ⇒ always true, so the bundle
    // selector picks exactly the groups it did before the floor existed. ON ⇒
    // eligible only once the group has reached MIN_FRAMES *or* its oldest frame
    // has waited MIN_WAIT (so a held group is never stranded past the bound).
    #[inline]
    fn eligible(&self, frames_len: usize, first_frame: Instant, now: Instant) -> bool {
        if self.min_frames == 0 {
            return true;
        }
        frames_len >= self.min_frames || now.duration_since(first_frame) >= self.min_wait
    }

    // The wake deadline for one group the floor is actively HOLDING below the
    // frame threshold: Some(first_frame + MIN_WAIT) iff ON, still below
    // MIN_FRAMES, and not yet aged out. None when OFF, when the group already
    // meets the frame floor (it dispatches on size, no timer needed), or when it
    // has already aged past MIN_WAIT (then it is eligible and the shared-permit
    // backstop, not this timer, governs it). The returned instant is ALWAYS
    // strictly in the future (age < MIN_WAIT ⇒ first_frame + MIN_WAIT > now) — the
    // invariant that keeps the shard loop's re-arm from busy-spinning on an
    // already-elapsed deadline.
    #[inline]
    fn deadline(&self, frames_len: usize, first_frame: Instant, now: Instant) -> Option<Instant> {
        if self.min_frames == 0 || frames_len >= self.min_frames {
            return None;
        }
        if now.duration_since(first_frame) >= self.min_wait {
            return None;
        }
        Some(first_frame + self.min_wait)
    }
}

pub struct AddMsg {
    pub queue: String,
    pub partition: String,
    // Track B (§5): the request's resolved tenant. Part of the fusion group key so
    // two tenants' same-named (queue, partition) never fuse into one segment; also
    // travels in the log_push_multi_v1 tenants array so provisioning/resolution is
    // scoped. Default tenant when the feature is off ⇒ grouping is byte-identical.
    pub tenant: String,
    pub frames: Vec<OwnedFrame>,
}

pub struct Fusion {
    senders: Vec<mpsc::UnboundedSender<AddMsg>>,
    // A handle to the broker-global dedup cache (the authoritative copy lives in
    // each shard's FlushCtx). Kept here so the `sizes` reporter (obs.rs) can read
    // its resident/suppressed footprint without reaching into a shard.
    dedup: Arc<DedupCache>,
}

// Per-partition metadata for the dedup path, TTL-cached per shard (partitions
// are shard-affine, so each key lives in exactly one shard's map). `pid` is the
// log_partitions uuid the hydration and duplicate-resolution SQL needs;
// `window_secs` drives cache expiry and the window<=0 fast skip; `last_offset`
// is the hydration fallback watermark (only ever <= the true value, which
// under-vouches — always sound). The TTL bounds how long a /configure window
// change can go unnoticed; a detected change forces a full rehydration
// (`force_rehydrate`, sticky until a hydration succeeds) because the cache
// entry's expiry bookkeeping was computed under the old window.
#[derive(Clone)]
struct PartMeta {
    pid: String,
    window_secs: i32,
    last_offset: i64,
    fetched_ms: i64,
    force_rehydrate: bool,
}

const PART_META_TTL_MS: i64 = 30_000;

// 5s clock-skew slack (doc 18 §5's margin, applied on the SAFE side): hydration
// fetches and keeps slightly MORE than the window. Extra old rows only widen
// the vouched coverage (sound — they are dropped by the next expire()); the
// unsound direction (treating a near-expiry cache hit as a local duplicate
// without SQL) is avoided entirely — see segment_verified.
const DEDUP_SKEW_SLACK_MS: i64 = 5_000;

// Hydration statement (a): resolve pid + dedup window + allocator watermark.
// Doubles as the TTL meta refresh. prepare_cached: runs every ~30s/partition.
// Track B (§5): scoped by tenant ($3) so two tenants' same-named (queue, partition)
// resolve to their OWN pid + dedup window (else the dedup cache would hydrate/vouch
// across tenants). Queue identity is now the queen.queues id (log_partitions
// references it directly; dedup_window_seconds lives on queen.queues).
const PART_META_SQL: &str = "SELECT q.dedup_window_seconds, p.id::text, p.last_offset \
     FROM queen.queues q JOIN queen.log_partitions p ON p.queue_id = q.id \
     WHERE q.name = $1 AND p.name = $2 AND q.tenant_id = $3::text::uuid";

// Hydration statement (b): the partition's in-window log_txns rows (epoch ms so
// dedup.rs stays clock-format-free). $2 = window + skew slack, seconds.
const HYDRATE_SQL: &str = "SELECT t.base_offset, t.end_offset, \
     (extract(epoch from t.created_at)*1000)::int8, t.hashes \
     FROM queen.log_txns t \
     WHERE t.partition_id = $1::text::uuid \
       AND t.created_at >= now() - make_interval(secs => $2) \
     ORDER BY t.base_offset";

// Rare-path pid lookup for duplicate-mid resolution when the dedup meta cache
// has no entry (cache disabled, or the meta fetch degraded this flush).
const PART_PID_SQL: &str = "SELECT p.id::text \
     FROM queen.queues q JOIN queen.log_partitions p ON p.queue_id = q.id \
     WHERE q.name = $1 AND p.name = $2 AND q.tenant_id = $3::text::uuid";

// Rare-path covering-segment fetch for duplicate-mid resolution (§4).
const SEGMENT_AT_SQL: &str =
    "SELECT r_base, r_blob FROM queen.log_segment_at_v1($1::text::uuid, $2)";

struct FlushCtx {
    pool: Pool,
    admission: Arc<Admission>,
    metrics: Arc<Metrics>,
    zstd_level: i32,
    stmt_timeout: Duration,
    // Broker-side dedup cache (doc 18 §5, src/dedup.rs) — ONE instance shared
    // by every shard (the byte budget is global); `dedup_enabled` mirrors the
    // kill switch so the flush can skip the meta/hydration SQL entirely when
    // the cache would decline anyway (a disabled cache == always p_verified=-1).
    dedup: Arc<DedupCache>,
    dedup_enabled: bool,
    // Postgres TLS for an OUT-OF-BAND connection, built once by the caller from
    // the same Config the pool was built from (db::cancel_connector). Needed to
    // best-effort cancel a wedged server-side query on a flush timeout, because
    // CancelToken::cancel_query dials a fresh connection and wants the same TLS
    // — including the same `PG_SSL_ROOT_CERT` trust anchors — as the pool.
    // `None` = the pool is plaintext. Carried rather than rebuilt per cancel:
    // building a ClientConfig parses the whole root set, and a cancellation
    // storm is precisely when that cost would land.
    pg_cancel_tls: Option<tokio_postgres_rustls::MakeRustlsConnect>,
    // Per-shard PartMeta cache, keyed by the same composed queue+partition
    // string the shard map uses (group_key).
    part_meta: Mutex<FnvHashMap<String, PartMeta>>,
}

impl Fusion {
    /// Shared broker-global dedup cache, for the `sizes` aggregate reporter (obs.rs).
    pub fn dedup_cache(&self) -> Arc<DedupCache> {
        self.dedup.clone()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shards: usize,
        pool: Pool,
        admission: Arc<Admission>,
        metrics: Arc<Metrics>,
        zstd_level: i32,
        fusion_frames: usize,
        hold_ms: u64,
        stmt_timeout: Duration,
        dedup_cache_mb: usize,
        dedup_cache_enabled: bool,
        pg_cancel_tls: Option<tokio_postgres_rustls::MakeRustlsConnect>,
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
        // FAT-BATCH FLOOR (experiment flag; see the Floor doc comment above for
        // the 39↔99 msgs/commit measurement that motivates it). Internal env
        // override only, NOT part of the product contract — exactly like
        // QUEEN_V2_BUNDLE_MAX / QUEEN_V2_FUSION_MAX_INFLIGHT above.
        //   QUEEN_V2_FUSION_MIN_FRAMES — default 0 = feature OFF (dispatch stays
        //     byte-for-byte the pre-floor fire-on-idle behavior).
        //   QUEEN_V2_FUSION_MIN_WAIT_MS — default 2; the bounded max hold from a
        //     group's first frame. Only meaningful when MIN_FRAMES > 0.
        let floor = Floor {
            min_frames: std::env::var("QUEEN_V2_FUSION_MIN_FRAMES")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(0),
            min_wait: Duration::from_millis(
                std::env::var("QUEEN_V2_FUSION_MIN_WAIT_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(2),
            ),
        };
        if floor.enabled() {
            tracing::info!(
                target: "fusion",
                min_frames = floor.min_frames,
                min_wait_ms = floor.min_wait.as_millis() as u64,
                "fat-batch floor ON (experiment flag)"
            );
        }
        // fusion_frames (QUEEN_V2_FUSION_FRAMES) is retained for API/env compat but
        // is no longer a flush trigger: segment size is now emergent. At low load
        // fire-on-idle flushes one push at a time (segment≈1); under load the next
        // segment accumulates for exactly one in-flight flush RTT, so segments grow
        // with offered load with no fixed frame threshold. See shard_loop.
        let _ = fusion_frames;
        // ONE broker-global dedup cache shared across shards: partitions are
        // shard-affine (so no cross-shard races on one partition's entry), but
        // the QUEEN_DEDUP_CACHE_MB budget and its LRU are global.
        let dedup = Arc::new(DedupCache::new(dedup_cache_mb, dedup_cache_enabled));
        let mut senders = Vec::with_capacity(shards);
        for _ in 0..shards {
            let (tx, rx) = mpsc::unbounded_channel::<AddMsg>();
            senders.push(tx);
            let ctx = Arc::new(FlushCtx {
                pool: pool.clone(),
                admission: admission.clone(),
                metrics: metrics.clone(),
                zstd_level,
                stmt_timeout,
                dedup: dedup.clone(),
                dedup_enabled: dedup_cache_enabled,
                pg_cancel_tls: pg_cancel_tls.clone(),
                part_meta: Mutex::new(FnvHashMap::default()),
            });
            tokio::spawn(shard_loop(rx, ctx, hold_ms, max_inflight, bundle_max, floor));
        }
        Arc::new(Fusion { senders, dedup })
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
// fan-out cap and a push admission slot is immediately free) the ready partition
// flushes IMMEDIATELY — no hold timer — as a bundle of 1, so a single/low-rate
// push commits in ~one DB RTT, not ~hold ms.
//
// Per-partition in-flight gate (Trap 3): at most ONE flush per (queue,partition)
// is ever in flight. While a partition's flush is in flight, its next segment
// keeps accumulating and is dispatched only after the previous flush COMMITS
// (the completion signal). Two flushes therefore never race on
// `UPDATE log_partitions SET last_offset=last_offset+cnt`, so `Lock:transactionid`
// stalls disappear and per-partition offset order == commit order.
//
// Cross-partition BUNDLING (Trap 3b): each dispatch collects up to K =
// `bundle_max` ready, NOT-in-flight (hence disjoint) partitions and commits all
// of their segments in ONE `log_push_multi_v1` transaction (one commit /
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
    floor: Floor,
) {
    let mut groups: FnvHashMap<String, FusionGroup> = FnvHashMap::default();
    let mut inflight: FnvHashSet<String> = FnvHashSet::default();
    // Concurrently in-flight bundles for THIS shard (each holds one push admission slot
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

    // FAT-BATCH FLOOR wakeup (experiment flag; default OFF). When the floor holds
    // a still-small segment below MIN_FRAMES, the `hold_ms` backstop tick (~15ms
    // by default) is far too coarse to honor the MIN_WAIT_MS bound (default 2ms),
    // so a DEDICATED deadline timer wakes the loop at the earliest held group's
    // (first_frame + MIN_WAIT_MS). `floor_armed` records the instant the timer is
    // armed for; its `is_some()` GATES the select arm, so with MIN_FRAMES=0 the
    // arm is never polled, the timer is never reset off its dummy far-future
    // deadline, and this entire mechanism is inert — the loop is byte-identical to
    // the pre-floor select.
    let floor_timer = tokio::time::sleep(Duration::from_secs(3600));
    tokio::pin!(floor_timer);
    let mut floor_armed: Option<Instant> = None;

    loop {
        tokio::select! {
            maybe = rx.recv() => {
                let Some(msg) = maybe else { break; };
                // Track B (§5): tenant is part of the group key, so tenant A's and
                // tenant B's "orders"/"Default" accumulate as DISTINCT segments.
                let key = format!("{}\x1f{}\x1f{}", msg.tenant, msg.queue, msg.partition);
                // Stamp the group's first-frame arrival at (re)creation — the clock
                // the floor ages a held segment against. Cheap monotonic read;
                // reused for the group's whole accumulation, ignored when OFF.
                let arrival = Instant::now();
                let g = groups.entry(key.clone()).or_insert_with(|| FusionGroup {
                    queue: msg.queue.clone(),
                    partition: msg.partition.clone(),
                    tenant: msg.tenant.clone(),
                    frames: Vec::new(),
                    first_frame: arrival,
                });
                g.frames.extend(msg.frames);
                // Fire-on-idle: if a permit is free AND the group is dispatch-
                // eligible (floor OFF ⇒ always; floor ON ⇒ reached MIN_FRAMES or
                // aged past MIN_WAIT), bundle every ready partition and dispatch
                // now; otherwise the groups stay and accumulate (dispatched on the
                // next completion, backstop tick, or floor deadline). ON ⇒
                // fire-on-idle respects the floor, so idle lanes can't defeat it.
                try_dispatch_bundle(&mut groups, &mut inflight, &mut inflight_bundles,
                    max_inflight, bundle_max, floor, &ctx, &done_tx);
            }
            Some(keys) = done_rx.recv() => {
                for k in &keys {
                    inflight.remove(k);
                }
                inflight_bundles = inflight_bundles.saturating_sub(1);
                // The freed permit + released partitions can now form new bundles
                // over everything currently accumulated (including these partitions'
                // next segments — which necessarily commit AFTER the bundle that
                // just committed, so per-partition offset order == commit order).
                sweep_bundles(&mut groups, &mut inflight, &mut inflight_bundles,
                    max_inflight, bundle_max, floor, &ctx, &done_tx);
            }
            _ = tick.tick() => {
                // Liveness backstop: retry held groups whose dispatch depends on a
                // slot freed on another shard (the push admission budget is shared),
                // which no local arrival/completion would have woken us for.
                sweep_bundles(&mut groups, &mut inflight, &mut inflight_bundles,
                    max_inflight, bundle_max, floor, &ctx, &done_tx);
            }
            _ = &mut floor_timer, if floor_armed.is_some() => {
                // A floor-held group has reached MIN_WAIT_MS since its first frame
                // and is now eligible by age. Sweep dispatches it (and any peer
                // that became eligible meanwhile). The bottom-of-loop recompute
                // then re-arms for the next-earliest held group or disarms.
                sweep_bundles(&mut groups, &mut inflight, &mut inflight_bundles,
                    max_inflight, bundle_max, floor, &ctx, &done_tx);
            }
        }

        // Re-arm the floor deadline over whatever remains held below the frame
        // floor. `earliest_floor_deadline` only returns strictly-future instants,
        // so once the timer fires the freshly computed `next` can never equal the
        // now-past `floor_armed`: the `!=` guard therefore ALWAYS re-arms or
        // disarms and can never leave an elapsed timer armed (⇒ no busy-spin).
        // When OFF this is a single `enabled()` false test ⇒ skipped entirely, so
        // `floor_armed` stays None, the timer is never touched, and the loop is
        // identical to the pre-floor version.
        if floor.enabled() {
            let next = earliest_floor_deadline(&groups, &inflight, floor, Instant::now());
            if next != floor_armed {
                if let Some(dl) = next {
                    floor_timer.as_mut().reset(tokio::time::Instant::from_std(dl));
                }
                floor_armed = next;
            }
        }
    }
}

// The earliest floor wake deadline over every group this shard is holding below
// the frame threshold (skipping empty and in-flight groups — an in-flight
// partition can't dispatch until its flush completes, at which point the
// completion arm re-computes this). None ⇒ nothing is floor-held ⇒ the timer
// stays disarmed. O(live groups); only walked when the feature is on.
fn earliest_floor_deadline(
    groups: &FnvHashMap<String, FusionGroup>,
    inflight: &FnvHashSet<String>,
    floor: Floor,
    now: Instant,
) -> Option<Instant> {
    earliest_deadline(
        groups
            .iter()
            .map(|(k, g)| (g.frames.len(), g.first_frame, inflight.contains(k))),
        floor,
        now,
    )
}

// Pure core of `earliest_floor_deadline`: fold `Floor::deadline` over the held,
// not-in-flight, non-empty groups and keep the soonest. Separated from the map
// walk so it is unit-testable without constructing OwnedFrame/PushState.
fn earliest_deadline<I>(items: I, floor: Floor, now: Instant) -> Option<Instant>
where
    I: IntoIterator<Item = (usize, Instant, bool)>, // (frames_len, first_frame, inflight)
{
    if !floor.enabled() {
        return None;
    }
    let mut best: Option<Instant> = None;
    for (frames_len, first_frame, inflight) in items {
        if inflight || frames_len == 0 {
            continue;
        }
        if let Some(dl) = floor.deadline(frames_len, first_frame, now) {
            best = Some(match best {
                Some(b) if b <= dl => b,
                _ => dl,
            });
        }
    }
    best
}

// Form and dispatch ONE bundle now. Returns true if a bundle was fired. Gates, in
// order: (1) the per-shard concurrent-bundle fan-out cap; (2) at least one ready,
// not-in-flight partition exists; (3) a push admission slot must be immediately
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
    floor: Floor,
    ctx: &Arc<FlushCtx>,
    done: &mpsc::UnboundedSender<Vec<String>>,
) -> bool {
    if *inflight_bundles >= max_inflight {
        return false;
    }
    // Collect up to K ready, not-in-flight, dispatch-ELIGIBLE partition keys
    // (disjoint by the in-flight gate). `floor.eligible` is `true` for every
    // non-empty group when the feature is OFF, so the chosen set is byte-identical
    // to the pre-floor selection; when ON, a group below MIN_FRAMES whose oldest
    // frame is younger than MIN_WAIT is held back to fatten (the floor deadline
    // timer or a later arrival dispatches it within the bound).
    let now = Instant::now();
    let mut chosen: Vec<String> = Vec::with_capacity(bundle_max);
    for (k, g) in groups.iter() {
        if chosen.len() >= bundle_max {
            break;
        }
        if !g.frames.is_empty()
            && !inflight.contains(k)
            && floor.eligible(g.frames.len(), g.first_frame, now)
        {
            chosen.push(k.clone());
        }
    }
    if chosen.is_empty() {
        return false;
    }
    // One admission slot for the whole bundle: one DB op / one commit for all
    // N segments. Non-blocking — fire-on-idle needs a synchronous probe; when
    // the budget is full the shard holds and the groups keep fattening.
    let slot = match ctx.admission.try_acquire(Lane::Push) {
        Some(s) => s,
        None => return false,
    };
    let mut bundle: Vec<FusionGroup> = Vec::with_capacity(chosen.len());
    for k in &chosen {
        let g = groups.remove(k).expect("group present (checked)");
        inflight.insert(k.clone());
        bundle.push(g);
    }
    *inflight_bundles += 1;
    spawn_bundle_flush(ctx.clone(), chosen, bundle, slot, done.clone());
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
    floor: Floor,
    ctx: &Arc<FlushCtx>,
    done: &mpsc::UnboundedSender<Vec<String>>,
) {
    while *inflight_bundles < max_inflight {
        if !try_dispatch_bundle(
            groups, inflight, inflight_bundles, max_inflight, bundle_max, floor, ctx, done,
        ) {
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
    // Per-frame txn fingerprint (doc 18 §3: xxh3_128 big-endian), aligned with
    // group.frames. Drives layer-2 dedup, the cache watermarks, and the 16*K
    // hash blobs the push SQL stores in log_txns.
    hashes: Vec<[u8; 16]>,
    leader_idx: Vec<usize>, // per-frame txn-leader index
    pending: Vec<usize>,    // kept frame indices still to push
    verdict: FnvHashMap<usize, (&'static str, String)>,
    total: usize,
    db_ok: bool,
}

// Layer 2: intra-flush dedup by txn fingerprint (first-wins), comparing the 16B
// xxh3_128 hashes instead of composed strings — the exact identity SQL compares
// (§3), so a broker-local hash collision behaves identically to a SQL one. The
// first occurrence of each hash is the "leader" (the only frame that reaches
// the segment); every later same-hash frame is a follower that inherits the
// leader's final verdict.
fn layer2_dedup(hashes: &[[u8; 16]]) -> (Vec<usize>, Vec<usize>) {
    let mut leader_of: FnvHashMap<u128, usize> = FnvHashMap::default();
    let mut leader_idx: Vec<usize> = Vec::with_capacity(hashes.len());
    let mut kept: Vec<usize> = Vec::new();
    for (i, h) in hashes.iter().enumerate() {
        let k = u128::from_be_bytes(*h);
        if let Some(&l) = leader_of.get(&k) {
            leader_idx.push(l);
        } else {
            leader_of.insert(k, i);
            leader_idx.push(i);
            kept.push(i);
        }
    }
    (leader_idx, kept)
}

// Build one segment's SQL inputs for a group's current `pending` subset: the
// 16*K hash blob (frame order — position k in the blob is packed frame k, the
// same "i" the SP echoes in dups) and the packed zstd payload blob (frame
// packing UNCHANGED from the seg engine).
//
// PLAN_KV_TIMERS §15: the packing itself now lives in
// `frames::pack_segment_with_hashes`, so the timer fire (which cannot go through
// fusion — §1.8) uses THE SAME recipe instead of a second copy of it. This
// function keeps its signature and its behaviour byte for byte: it still selects
// the `pending` subset, still reuses the fingerprints the flush already computed
// (re-hashing here would add xxh3 work per frame per repack to the push hot
// path), and still emits them in frame order.
fn build_hashes_and_blob(
    group: &FusionGroup,
    hashes: &[[u8; 16]],
    pending: &[usize],
    zstd_level: i32,
) -> (Vec<u8>, Vec<u8>) {
    let mut hblob: Vec<u8> = Vec::with_capacity(pending.len() * 16);
    for &fi in pending {
        hblob.extend_from_slice(&hashes[fi]);
    }
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
                payload: f.payload.as_ref(),
                // RUSTFIX item 8: propagate the frame's encrypted flag (crypto was
                // done at the push handler; fusion only carries the bit).
                encrypted: f.encrypted,
            }
        })
        .collect();
    let seg = crate::frames::pack_segment_with_hashes(&fins, hblob, zstd_level);
    (seg.hashes, seg.blob)
}

// Push a bundle of N segments (distinct partitions) in ONE multi-segment
// transaction via queen.log_push_multi_v1 — typed parallel arrays (text[],
// text[], int4[], bytea[], int8[], bytea[]) with zero jsonb parsed on the PG
// hot path, prepared once per connection (prepare_cached). Returns the
// per-segment result array JSON as text, index-aligned with the input.
#[allow(clippy::too_many_arguments)]
async fn push_log_multi(
    client: &deadpool_postgres::Client,
    queues: &[String],
    partitions: &[String],
    msg_counts: &[i32],
    hashes: &[Vec<u8>],
    verified: &[i64],
    blobs: Vec<Vec<u8>>,
    // Track B (§5): per-segment tenant, index-aligned with the other arrays. Bound
    // as text[] and cast to uuid[] (no uuid crate). All the default tenant when the
    // feature is off ⇒ provisioning/resolution land byte-identically.
    tenants: &[String],
) -> Result<String, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT (queen.log_push_multi_v1($1, $2, $3, $4, $5, $6, $7::text[]::uuid[]))::text",
        )
        .await?;
    let row = client
        .query_one(
            &stmt,
            &[&queues, &partitions, &msg_counts, &hashes, &verified, &blobs, &tenants],
        )
        .await?;
    Ok(row.get(0))
}

// Best-effort cancellation of a wedged server-side query after a flush timeout.
// tokio::time::timeout only drops the client future — the PG backend keeps
// executing the statement (this amplified a separate wedge), so we send an
// out-of-band CancelRequest on a fresh connection via the connection's
// CancelToken. Fire-and-forget on its own task (cancel_query itself dials PG and
// must not block the flush); the log line is rate-limited process-wide. Picks
// the same TLS mode the pool uses, so the cancel connection negotiates
// identically. A failed cancel is harmless — the query just finishes on its own.
fn spawn_query_cancel(ctx: &Arc<FlushCtx>, token: tokio_postgres::CancelToken, what: &'static str) {
    static LAST_CANCEL_LOG_MS: AtomicU64 = AtomicU64::new(0);
    let now = now_epoch_ms() as u64;
    let prev = LAST_CANCEL_LOG_MS.load(Ordering::Relaxed);
    if now.saturating_sub(prev) >= 5_000
        && LAST_CANCEL_LOG_MS
            .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        tracing::warn!(target: "fusion", what, "bundle timed out; cancelling server-side query (best-effort)");
    }
    let tls = ctx.pg_cancel_tls.clone();
    tokio::spawn(async move {
        let res = match tls {
            Some(connector) => token.cancel_query(connector).await,
            None => token.cancel_query(NoTls).await,
        };
        // Best-effort: a failed cancel just means the query completes on its own.
        let _ = res;
    });
}

// ---------------------------------------------------------------------------
// Dedup-cache plumbing (doc 18 §5). All of it is best-effort: every SQL or
// consistency failure degrades to p_verified = -1 (SQL probes the full window —
// always sound) and never fails the bundle.
// ---------------------------------------------------------------------------

// Fetch (TTL-cached) the partition's dedup meta. Returns None when the
// partition is not provisioned yet (first push ever — SQL will provision it)
// or the lookup failed; both mean "can't vouch this flush".
async fn get_meta(
    ctx: &FlushCtx,
    client: &deadpool_postgres::Client,
    key: &str,
    queue: &str,
    partition: &str,
    tenant: &str,
) -> Option<PartMeta> {
    let now = now_epoch_ms();
    {
        let map = ctx.part_meta.lock().unwrap();
        if let Some(e) = map.get(key) {
            if now - e.fetched_ms < PART_META_TTL_MS {
                return Some(e.clone());
            }
        }
    }
    let stmt = client.prepare_cached(PART_META_SQL).await.ok()?;
    let rows = client.query(&stmt, &[&queue, &partition, &tenant]).await.ok()?;
    let mut map = ctx.part_meta.lock().unwrap();
    let Some(row) = rows.first() else {
        // Not provisioned (or deleted): drop any stale meta so we never hand
        // out a dead pid for duplicate resolution.
        map.remove(key);
        return None;
    };
    let window: i32 = row.get(0);
    let meta = PartMeta {
        pid: row.get(1),
        window_secs: window,
        last_offset: row.get(2),
        fetched_ms: now,
        // A window change invalidates the cache entry's expiry bookkeeping
        // (a WIDENED window means expired-away hashes may be back in scope, so
        // vouching would be unsound) — force a full rebuild, sticky until one
        // succeeds.
        force_rehydrate: map
            .get(key)
            .map(|old| old.force_rehydrate || old.window_secs != window)
            .unwrap_or(false),
    };
    map.insert(key.to_string(), meta.clone());
    Some(meta)
}

// (Re)hydrate one partition's cache entry from log_txns. ALWAYS a full-window
// fetch, even when the cache only asked for an interleave Span top-up: between
// needs_hydration() and hydrate() another partition's flush can LRU-evict this
// entry, and hydrate() would then rebuild it from the span rows alone — a
// partial entry that vouches unsoundly. Full-window rows are correct for BOTH
// hydrate() branches (the span merge skips bases already held), and interleave
// gaps are rare enough that the extra rows don't matter.
async fn hydrate_partition(
    ctx: &FlushCtx,
    client: &deadpool_postgres::Client,
    key: &str,
    meta: &PartMeta,
) -> Result<(), tokio_postgres::Error> {
    let secs: f64 = meta.window_secs as f64 + (DEDUP_SKEW_SLACK_MS as f64 / 1000.0);
    let stmt = client.prepare_cached(HYDRATE_SQL).await?;
    let rows = client.query(&stmt, &[&meta.pid, &secs]).await?;
    let mut v: Vec<(i64, i64, i64, Vec<u8>)> = Vec::with_capacity(rows.len());
    for r in rows {
        v.push((r.get(0), r.get(1), r.get(2), r.get(3)));
    }
    let now = now_epoch_ms();
    let window_start_ms = now - meta.window_secs as i64 * 1000 - DEDUP_SKEW_SLACK_MS;
    ctx.dedup.hydrate(key, v, window_start_ms, meta.last_offset, now);
    if let Some(e) = ctx.part_meta.lock().unwrap().get_mut(key) {
        e.force_rehydrate = false;
    }
    Ok(())
}

// Compute one segment's p_verified watermark for this flush attempt:
// expire → (re)hydrate if needed → verified_for_push. -1 on any degradation.
//
// LocalDuplicate deliberately does NOT short-circuit: the §5 margin rule wants
// a local reject only when the matched entry sits >5s inside the window
// (clock-skew safety), but DedupCache's public API exposes no per-entry age —
// so known-duplicate segments are routed to SQL with the full-window probe
// (p_verified = -1), which returns the authoritative duplicate verdict WITH the
// original offsets the wire response needs anyway. The cache still pays off on
// the hot path: clean segments push with a vouched watermark that skips the
// probe entirely.
#[allow(clippy::too_many_arguments)]
async fn segment_verified(
    ctx: &FlushCtx,
    client: &deadpool_postgres::Client,
    key: &str,
    queue: &str,
    partition: &str,
    tenant: &str,
    pending_hashes: &[[u8; 16]],
) -> i64 {
    let meta = match get_meta(ctx, client, key, queue, partition, tenant).await {
        Some(m) => m,
        None => return -1,
    };
    if meta.window_secs <= 0 {
        // Window 0 = dedup off for this queue: skip the cache entirely (SQL
        // skips the probe too — the seg engine's window-0 landmine is dead).
        return -1;
    }
    // One `now` for the whole decision so expire/suppression-cooldown/eviction
    // all reason on the same instant.
    let now = now_epoch_ms();
    // Window-expire BEFORE consulting the cache (the dedup.rs contract): keeps
    // the entry's coverage bookkeeping accurate under the current window.
    ctx.dedup.expire(key, meta.window_secs as i64 * 1000, now);
    // needs_hydration() may answer None because the partition is SUPPRESSED under
    // cap pressure (Fix 1) — in that case we deliberately DO NOT run the
    // multi-MB hydration query and fall through to verified_for_push, which
    // returns -1 (full-window SQL probe — always sound).
    if meta.force_rehydrate || ctx.dedup.needs_hydration(key, now).is_some() {
        if hydrate_partition(ctx, client, key, &meta).await.is_err() {
            return -1;
        }
    }
    match ctx.dedup.verified_for_push(key, pending_hashes, now) {
        PushCheck::Verified(w) => w,
        PushCheck::LocalDuplicate(_) => -1,
    }
}

fn zero_uuid() -> String {
    uuid_bytes_to_string(&[0u8; 16])
}

// Resolve each duplicate's ORIGINAL message id (wire contract §4/§11: dup
// responses return the canonical first-occurrence mid) by fetching the covering
// segment's blob and unpacking frame (off - base). Rare path — plain
// (non-cached) queries. Every failure mode (segment already retained away,
// retention gap, malformed blob, SQL error) resolves to the zero uuid, the
// spec's "original unknown" sentinel. Returns mids aligned with `dups`.
#[allow(clippy::too_many_arguments)]
async fn resolve_dup_mids(
    ctx: &FlushCtx,
    client: &deadpool_postgres::Client,
    key: &str,
    queue: &str,
    partition: &str,
    tenant: &str,
    dups: &[(usize, i64)],
) -> Vec<String> {
    let mut out = vec![zero_uuid(); dups.len()];
    // Partition uuid: from the meta cache when the dedup path populated it,
    // else one inline lookup (cache disabled, or meta degraded this flush).
    let cached: Option<String> = ctx.part_meta.lock().unwrap().get(key).map(|m| m.pid.clone());
    let pid: Option<String> = match cached {
        Some(p) => Some(p),
        None => match client.query(PART_PID_SQL, &[&queue, &partition, &tenant]).await {
            Ok(rows) => rows.first().map(|r| r.get(0)),
            Err(e) => {
                static DUP_PID_ERR: crate::obs::Sampler = crate::obs::Sampler::new(30_000);
                if let Some(suppressed) = DUP_PID_ERR.tick_now() {
                    tracing::warn!(
                        target: "fusion",
                        queue = %queue,
                        partition = %partition,
                        error = %e,
                        suppressed,
                        "bundle dup pid lookup error"
                    );
                }
                None
            }
        },
    };
    let Some(pid) = pid else { return out };
    // Memoize the last covering segment — a batch's dups usually share one.
    let mut seg: Option<(i64, i64, Vec<[u8; 16]>)> = None;
    for (pos, (_i, off)) in dups.iter().enumerate() {
        let covered = seg
            .as_ref()
            .map(|(b, e, _)| *b <= *off && *off <= *e)
            .unwrap_or(false);
        if !covered {
            seg = None;
            match client.query(SEGMENT_AT_SQL, &[&pid, off]).await {
                Ok(rows) => {
                    if let Some(row) = rows.first() {
                        let base: i64 = row.get(0);
                        let blob: Vec<u8> = row.get(1);
                        let raw = zstd_decompress(&blob);
                        if let Some(frames) = unpack_frames_ref(&raw) {
                            let mids: Vec<[u8; 16]> =
                                frames.iter().map(|f| f.message_id).collect();
                            let end = base + mids.len() as i64 - 1;
                            seg = Some((base, end, mids));
                        }
                    }
                }
                Err(e) => {
                    static SEG_AT_ERR: crate::obs::Sampler = crate::obs::Sampler::new(30_000);
                    if let Some(suppressed) = SEG_AT_ERR.tick_now() {
                        tracing::warn!(
                            target: "fusion",
                            off = *off,
                            error = %e,
                            suppressed,
                            "bundle log_segment_at error"
                        );
                    }
                }
            }
        }
        if let Some((base, end, mids)) = seg.as_ref() {
            if *base <= *off && *off <= *end {
                out[pos] = uuid_bytes_to_string(&mids[(*off - *base) as usize]);
            }
        }
    }
    out
}

// Flush ONE bundle of N disjoint partitions as a single committed transaction.
// The push admission slot (acquired by the shard loop so availability can gate
// fire-on-idle) is held until the DB op returns. On completion the bundle's
// partition keys are sent back so the shard releases each in-flight slot and
// dispatches each partition's accumulated next segment — the signal is sent AFTER
// commit, so a partition's next flush (hence its `last_offset` allocation) strictly
// follows this one's commit (per-partition offset order == commit order).
fn spawn_bundle_flush(
    ctx: Arc<FlushCtx>,
    keys: Vec<String>,
    bundle: Vec<FusionGroup>,
    mut slot: crate::admission::Slot,
    done: mpsc::UnboundedSender<Vec<String>>,
) {
    tokio::spawn(async move {
        // Per-group hashing + layer-2 dedup up front; pending starts as the
        // kept leaders. keys[i] is group i's composed cache key (same order as
        // the bundle — try_dispatch_bundle built both from `chosen`).
        let mut gs: Vec<BundleGroup> = bundle
            .into_iter()
            .map(|group| {
                let total = group.frames.len();
                let hashes: Vec<[u8; 16]> =
                    group.frames.iter().map(|f| txn_hash128(&f.txn)).collect();
                let (leader_idx, kept) = layer2_dedup(&hashes);
                BundleGroup {
                    group,
                    hashes,
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
        // (all its pending frames committed) or `duplicate` (probe-before-
        // allocate wrote NOTHING for that segment — the surviving non-dup frames
        // are repacked and retried in a fresh transaction, with a FRESH verified
        // watermark: the cache learned the segments that just committed). Queued
        // groups drop out; a no-dup bundle finishes in exactly one iteration
        // (one commit). Bounded defensively by total frames + 2.
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

            // One pooled client serves the whole attempt: dedup meta/hydration,
            // the bundle push, and (rarely) duplicate-mid resolution.
            let client = match ctx.pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    static POOL_GET_ERR: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
                    if let Some(suppressed) = POOL_GET_ERR.tick_now() {
                        tracing::error!(target: "fusion", error = %e, suppressed, "bundle pool.get error");
                    }
                    for &gi in &participants {
                        gs[gi].db_ok = false;
                        gs[gi].pending.clear();
                    }
                    break;
                }
            };

            // Per-segment dedup watermarks (doc 18 §5), index-aligned with
            // participants. Failures degrade to -1 and never fail the bundle;
            // the timeout is only a hang backstop.
            let mut verified: Vec<i64> = vec![-1i64; participants.len()];
            if ctx.dedup_enabled {
                let vres = tokio::time::timeout(ctx.stmt_timeout, async {
                    let mut v: Vec<i64> = Vec::with_capacity(participants.len());
                    for &gi in &participants {
                        let g = &gs[gi];
                        let pending_hashes: Vec<[u8; 16]> =
                            g.pending.iter().map(|&fi| g.hashes[fi]).collect();
                        v.push(
                            segment_verified(
                                &ctx,
                                &client,
                                &keys[gi],
                                &g.group.queue,
                                &g.group.partition,
                                &g.group.tenant,
                                &pending_hashes,
                            )
                            .await,
                        );
                    }
                    v
                })
                .await;
                match vres {
                    Ok(v) => verified = v,
                    Err(_) => {
                        static DEDUP_HYDR_TIMEOUT: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
                        if let Some(suppressed) = DEDUP_HYDR_TIMEOUT.tick_now() {
                            tracing::warn!(
                                target: "fusion",
                                parts = participants.len(),
                                suppressed,
                                "bundle dedup hydration timeout"
                            );
                        }
                        // The vres future (and its borrow of `client`) is dropped
                        // by the `let vres = ...;` above, so cancelling the wedged
                        // meta/hydration query on this connection is safe here.
                        spawn_query_cancel(&ctx, client.cancel_token(), "dedup hydration");
                    }
                }
            }

            // Build the index-aligned typed arrays: queue/partition names travel
            // RAW as text[] (no JSON escaping), hashes as 16*count bytea each,
            // blobs as the packed zstd bytea.
            let mut queues: Vec<String> = Vec::with_capacity(participants.len());
            let mut partitions: Vec<String> = Vec::with_capacity(participants.len());
            let mut tenants: Vec<String> = Vec::with_capacity(participants.len());
            let mut counts: Vec<i32> = Vec::with_capacity(participants.len());
            let mut hash_blobs: Vec<Vec<u8>> = Vec::with_capacity(participants.len());
            let mut blobs: Vec<Vec<u8>> = Vec::with_capacity(participants.len());
            for &gi in participants.iter() {
                let g = &gs[gi];
                let (hblob, blob) =
                    build_hashes_and_blob(&g.group, &g.hashes, &g.pending, ctx.zstd_level);
                queues.push(g.group.queue.clone());
                partitions.push(g.group.partition.clone());
                tenants.push(g.group.tenant.clone());
                counts.push(g.pending.len() as i32);
                hash_blobs.push(hblob);
                blobs.push(blob);
            }

            // Bind the timeout result to a `let` so the push future (and its
            // borrow of `client`) is dropped before the match arms — the timeout
            // branch then re-borrows `client` to cancel the wedged query.
            let push_res = tokio::time::timeout(
                ctx.stmt_timeout,
                push_log_multi(
                    &client, &queues, &partitions, &counts, &hash_blobs, &verified, blobs, &tenants,
                ),
            )
            .await;
            let outcome = match push_res {
                Ok(Ok(txt)) => parse_multi_outcome(&txt, participants.len()),
                Ok(Err(e)) => {
                    static PUSH_MULTI_ERR: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
                    if let Some(suppressed) = PUSH_MULTI_ERR.tick_now() {
                        tracing::error!(
                            target: "fusion",
                            parts = participants.len(),
                            error = %e,
                            suppressed,
                            "bundle log_push_multi error"
                        );
                    }
                    None
                }
                Err(_) => {
                    static PUSH_MULTI_TIMEOUT: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
                    if let Some(suppressed) = PUSH_MULTI_TIMEOUT.tick_now() {
                        tracing::error!(
                            target: "fusion",
                            parts = participants.len(),
                            suppressed,
                            "bundle log_push_multi timeout"
                        );
                    }
                    // Cancel the still-running INSERT/allocator query so it stops
                    // holding the log_partitions row lock (which would otherwise
                    // wedge this partition's next flush until stmt_timeout on PG).
                    spawn_query_cancel(&ctx, client.cancel_token(), "log_push_multi");
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
                    PushOutcome::Queued { base, created_ms } => {
                        // Teach the cache the committed segment BEFORE touching
                        // verdicts (contiguity math needs the pending hashes).
                        if ctx.dedup_enabled {
                            let g = &gs[gi];
                            let hs: Vec<[u8; 16]> =
                                g.pending.iter().map(|&fi| g.hashes[fi]).collect();
                            ctx.dedup.on_push_committed(&keys[gi], *base, &hs, *created_ms);
                        }
                        let g = &mut gs[gi];
                        for &fi in &g.pending {
                            let mid = uuid_bytes_to_string(&g.group.frames[fi].message_id);
                            g.verdict.insert(fi, ("queued", mid));
                        }
                        g.pending.clear();
                    }
                    PushOutcome::Duplicate(dups) => {
                        // dups: (pos-in-pending, original absolute offset). The
                        // wire reports the ORIGINAL mid — resolve it from the
                        // covering segment (rare path, own timeout; failures
                        // fall back to the zero uuid, never to a bundle error:
                        // the push side effects are already decided).
                        let mids = match tokio::time::timeout(
                            ctx.stmt_timeout,
                            resolve_dup_mids(
                                &ctx,
                                &client,
                                &keys[gi],
                                &gs[gi].group.queue,
                                &gs[gi].group.partition,
                                &gs[gi].group.tenant,
                                dups,
                            ),
                        )
                        .await
                        {
                            Ok(v) => v,
                            Err(_) => vec![zero_uuid(); dups.len()],
                        };
                        let g = &mut gs[gi];
                        // Mark the dups, then repack+retry the survivors.
                        let mut is_dup = vec![false; g.pending.len()];
                        for ((i, _off), mid) in dups.iter().zip(mids) {
                            if *i < g.pending.len() {
                                is_dup[*i] = true;
                                g.verdict.insert(g.pending[*i], ("duplicate", mid));
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
        // Feed the train sensor only when something actually committed (a pool
        // failure or an all-error flush produced no flush ack to cluster), then
        // release the slot BEFORE signalling completion so the shard sees it
        // free when it re-dispatches these partitions' next segments.
        if gs.iter().any(|g| g.db_ok) {
            slot.commit_done(rtt);
        }
        drop(slot);
        // Per-segment metrics (one record per partition preserves the existing
        // "avg frames per segment" prometheus semantics); the bundle counter below
        // captures the commits-vs-segments coalescing gain.
        for g in &gs {
            ctx.metrics.push.record_batch(g.total, g.db_ok, rtt);
        }
        record_bundle(n_parts);
        // Minimal dedup observability hookup (Fix 3): surface the cache counters
        // alongside the periodic bundle summary. hydrations_total exploding while
        // suppressed_partitions stays 0 = healthy; a rising suppressed gauge =
        // cap pressure, size QUEEN_DEDUP_CACHE_MB per the warning line's formula.
        static DEDUP_SUMMARY: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
        if ctx.dedup_enabled && DEDUP_SUMMARY.tick_now().is_some() {
            // resident bytes/hashes let an operator (and the storage-redesign
            // validation) read the achieved footprint per hash directly.
            let rbytes = ctx.dedup.resident_bytes();
            let rhashes = ctx.dedup.resident_hashes();
            let per_hash = if rhashes > 0 {
                rbytes as f64 / rhashes as f64
            } else {
                0.0
            };
            tracing::debug!(
                target: "dedup",
                hydrations = ctx.dedup.hydrations_total(),
                suppressed_partitions = ctx.dedup.suppressed_partitions(),
                resident_bytes = rbytes,
                resident_hashes = rhashes,
                bytes_per_hash = per_hash,
                "dedup summary"
            );
        }

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

// Parsed outcome of one segment within a log_push_multi_v1 result array.
enum PushOutcome {
    // baseOffset of the committed segment + its PG createdAt (epoch ms; the
    // dedup ring's expiry clock — PG-stamped so hydration rows and live pushes
    // age on the same clock).
    Queued { base: i64, created_ms: i64 },
    // (frame index within the sent segment, original absolute offset) per dup.
    Duplicate(Vec<(usize, i64)>),
}

// Parse ONE segment result object (003_log_push contract).
// {"status":"queued","baseOffset":B,"createdAt":".."} -> Queued;
// {"status":"duplicate","dups":[{"i":..,"off":..}]} -> Duplicate. None on an
// unexpected status or a queued result without a baseOffset (treated as a
// whole-bundle error by the caller — a base the broker can't parse must never
// silently skip the cache commit).
fn parse_one_outcome(v: &serde_json::Value) -> Option<PushOutcome> {
    match v.get("status").and_then(|s| s.as_str()) {
        Some("queued") => {
            let base = v.get("baseOffset").and_then(|x| x.as_i64())?;
            let created_ms = v
                .get("createdAt")
                .and_then(|c| c.as_str())
                .and_then(parse_iso_ms)
                .unwrap_or_else(now_epoch_ms);
            Some(PushOutcome::Queued { base, created_ms })
        }
        Some("duplicate") => {
            let mut dups = Vec::new();
            if let Some(arr) = v.get("dups").and_then(|d| d.as_array()) {
                for d in arr {
                    let i = d.get("i").and_then(|x| x.as_i64());
                    let off = d.get("off").and_then(|x| x.as_i64());
                    if let (Some(i), Some(off)) = (i, off) {
                        if i >= 0 {
                            dups.push((i as usize, off));
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
    use super::{
        earliest_deadline, json_escape_into, layer2_dedup, parse_multi_outcome, Floor, PushOutcome,
    };
    use std::time::{Duration, Instant};

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

    #[test]
    fn layer2_first_wins_on_hashes() {
        let a = crate::util::txn_hash128("txn-a");
        let b = crate::util::txn_hash128("txn-b");
        let (leader_idx, kept) = layer2_dedup(&[a, b, a, a]);
        assert_eq!(leader_idx, vec![0, 1, 0, 0]);
        assert_eq!(kept, vec![0, 1]);
    }

    #[test]
    fn parse_outcomes_042_contract() {
        let txt = r#"[
            {"status":"queued","baseOffset":41,"createdAt":"2026-07-22T10:00:00.123456Z"},
            {"status":"duplicate","dups":[{"i":0,"off":7},{"i":2,"off":39}]}
        ]"#;
        let out = parse_multi_outcome(txt, 2).expect("parses");
        match &out[0] {
            PushOutcome::Queued { base, created_ms } => {
                assert_eq!(*base, 41);
                // 2026-07-22T10:00:00.123Z — exact epoch math is parse_iso_ms's
                // tested concern; here just assert the fraction survived.
                assert_eq!(*created_ms % 1000, 123);
            }
            _ => panic!("expected queued"),
        }
        match &out[1] {
            PushOutcome::Duplicate(d) => assert_eq!(d, &vec![(0usize, 7i64), (2usize, 39i64)]),
            _ => panic!("expected duplicate"),
        }
        // A queued element without baseOffset must fail the WHOLE bundle (the
        // broker would otherwise skip the cache commit silently).
        assert!(parse_multi_outcome(r#"[{"status":"queued"}]"#, 1).is_none());
        // Length mismatch fails too.
        assert!(parse_multi_outcome(r#"[{"status":"queued","baseOffset":1,"createdAt":"x"}]"#, 2).is_none());
    }

    // ---- FAT-BATCH FLOOR (experiment flag) --------------------------------

    fn off() -> Floor {
        Floor { min_frames: 0, min_wait: Duration::from_millis(2) }
    }
    fn on() -> Floor {
        Floor { min_frames: 200, min_wait: Duration::from_millis(2) }
    }

    #[test]
    fn floor_off_is_inert() {
        // MIN_FRAMES=0 ⇒ every group is eligible regardless of size/age, nothing
        // is ever held, and no deadline is ever produced. This is the byte-
        // identical-when-off guarantee at the predicate level.
        let f = off();
        let now = Instant::now();
        let young = now - Duration::from_micros(1);
        let old = now - Duration::from_secs(10);
        assert!(!f.enabled());
        assert!(f.eligible(0, young, now));
        assert!(f.eligible(1, young, now));
        assert!(f.eligible(0, old, now));
        assert!(f.eligible(999_999, young, now));
        assert_eq!(f.deadline(0, young, now), None);
        assert_eq!(f.deadline(1, young, now), None);
        // The fold short-circuits to None even with held-looking inputs.
        assert_eq!(
            earliest_deadline([(10usize, young, false), (1usize, young, false)], f, now),
            None
        );
    }

    #[test]
    fn floor_on_eligibility_by_size_or_age() {
        let f = on();
        let now = Instant::now();
        let young = now - Duration::from_millis(1); // < MIN_WAIT (2ms)
        let aged = now - Duration::from_millis(3); // >= MIN_WAIT
        assert!(f.enabled());
        // Below floor AND young ⇒ HELD (not eligible).
        assert!(!f.eligible(1, young, now));
        assert!(!f.eligible(199, young, now));
        // Reached the frame floor ⇒ eligible regardless of age.
        assert!(f.eligible(200, young, now));
        assert!(f.eligible(1_000, young, now));
        // Below floor but aged past MIN_WAIT ⇒ eligible (bounded-latency escape).
        assert!(f.eligible(1, aged, now));
        assert!(f.eligible(199, aged, now));
    }

    #[test]
    fn floor_on_deadline_is_strictly_future_and_bounded() {
        let f = on();
        let now = Instant::now();
        let young = now - Duration::from_millis(1);
        let aged = now - Duration::from_millis(3);
        // Held (below floor, young): deadline = first_frame + MIN_WAIT, and it is
        // ALWAYS strictly in the future — the no-busy-spin invariant.
        let dl = f.deadline(50, young, now).expect("held group has a deadline");
        assert_eq!(dl, young + Duration::from_millis(2));
        assert!(dl > now, "held deadline must be strictly future (no busy-spin)");
        // At/above the frame floor ⇒ no hold deadline (dispatches on size).
        assert_eq!(f.deadline(200, young, now), None);
        // Already aged out ⇒ no hold deadline: it is eligible, so the shared-permit
        // backstop governs it, not the floor timer. Returning None here is exactly
        // what prevents the shard loop re-arming on an elapsed instant.
        assert_eq!(f.deadline(50, aged, now), None);
    }

    #[test]
    fn earliest_deadline_picks_soonest_held_and_skips_the_rest() {
        let f = on();
        let now = Instant::now();
        let older = now - Duration::from_millis(1); // deadline older+2ms (sooner)
        let newer = now - Duration::from_micros(200); // deadline newer+2ms (later)
        let items = [
            (10usize, newer, false),  // held, later deadline
            (10usize, older, false),  // held, SOONER deadline → wins
            (300usize, older, false), // at floor ⇒ eligible, no deadline
            (10usize, older, true),   // in-flight ⇒ skipped
            (0usize, older, false),   // empty ⇒ skipped
        ];
        assert_eq!(
            earliest_deadline(items, f, now),
            Some(older + Duration::from_millis(2))
        );
        // Nothing held (all eligible / in-flight / empty) ⇒ None.
        assert_eq!(
            earliest_deadline(
                [(300usize, older, false), (0usize, older, false), (10usize, older, true)],
                f,
                now
            ),
            None
        );
    }
}
