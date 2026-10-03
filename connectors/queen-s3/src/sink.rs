//! sink — the entry point the broker calls: one [`Sink`] per broker tenant per
//! node, running that tenant's configured queues for as long as the broker
//! runs, and one [`SinkShared`] per node that all of them share.
//!
//! The broker links this crate (feature `s3`). Per node it reads the node-wide
//! knobs once ([`crate::config::NodeKnobs::from_env_with`] — its node identity
//! as the lease instance) and builds one [`SinkShared`] from them: the node's
//! one memory budget, its one metric registry and its claim load. Per tenant it
//! builds a [`Config`] — the default tenant's from the environment
//! ([`Config::from_env`]), every other tenant's from its control-plane document
//! ([`Config::from_tenant_doc`]) — and a [`Sink`] over that tenant's in-process
//! [`QueenApi`] ([`Sink::new_shared`]), then runs [`Sink::run`] until it stops.
//! Every node of a cluster does the same, with the same configuration. Which
//! node writes which queue is decided per queue by the
//! lease ([`crate::lease`]); every node reads the log from its own applied
//! state, and each window is closed against the `safeTime` of the node that
//! reads it. The KV documents — lease, intent, commit pointer — are read
//! linearizably, so a node that takes a queue over starts from its latest
//! commit.
//!
//! What this module owns, and nothing else does:
//!
//! * **the queue set** — `QUEEN_S3_QUEUES` as a list, or `*` re-listed every ten
//!   discovery intervals so a queue created after boot gets a task;
//! * **ownership** — one lease per queue (plan §6.6), claimed by every node,
//!   re-tried after its TTL by the nodes that lost, refreshed by the one that
//!   won while it works, released when it stops. A node claims a FREE queue
//!   only after a wait that grows with the queues it already runs
//!   ([`CLAIM_STEP`] each, plus a jitter), so the least-loaded node claims
//!   first and the queues spread over the nodes instead of all landing on
//!   whichever node asked first; a queue whose live row names this node (an
//!   earlier life of it) is taken back at once;
//! * **placement** ([`crate::placement`]) — a presence row per node, the fair
//!   share `ceil(Q / N)`, and a node above its share giving queues back one at
//!   a time, so the queues spread over the nodes whatever order they started
//!   in — and stay put once they are spread;
//! * **the stop** — the future handed to [`Sink::run`] sets the signal every
//!   queue task observes, and `run` returns once each of them has drained
//!   (plan §6.7);
//! * **the bucket at boot** — probed before any queue starts, and retried
//!   behind a backoff while it does not answer, so a bucket that is down when
//!   the broker starts delays the sink and nothing else.
//!
//! It never exits the process, never installs a tracing subscriber and never
//! reads a signal: those belong to the broker. A configuration error is the
//! `Err` of [`Sink::new`] (or of the [`Config`] constructors) and names the
//! variable or the document field; everything that goes wrong after that is
//! retried, reported in [`Sink::status`], and logged under the `queen-s3`
//! target.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::task::JoinSet;
use tracing::Instrument;

use crate::config::{wire, Config, CrashAt, NodeKnobs, Queues};
use crate::driver::{
    mid_upload_abort_hook, reject_static_partitions, stop_exporting, Backoff, DriverConfig,
    MemoryBudget, QueueDriver, Runtime, Shutdown, Stop,
};
use crate::health::HealthState;
use crate::lease::{spawn_refresh, Acquired, Holder, Lease};
use crate::obs::{Metrics, Sampler};
use crate::placement::{rebalance, Placement, Presence};
use crate::queen::QueenApi;
use crate::s3::{ObjectStore, S3Client, MAX_BACKOFF_MS};
use crate::status::StatusBoard;
use crate::types::Format;
use crate::writer::{factory, WriterFactory, CRATE_VERSION};

/// A queue that stopped for a reason retrying the same step cannot fix — the
/// bucket refused a write, a status no retry changes — is tried again, slowly.
/// The operator fixes it; the sink notices without the broker being
/// restarted.
const TERMINAL_RETRY: Duration = Duration::from_secs(60);

/// A queue that does not exist (yet) is looked for again this soon: nothing
/// needs fixing, it needs creating, and a queue created right after the
/// broker started must not wait out [`TERMINAL_RETRY`].
pub const MISSING_RETRY: Duration = Duration::from_secs(5);

/// How much longer a node waits to claim a FREE queue for each queue it
/// already runs or is claiming: a node running two waits 2 s more than an idle
/// one, so when several nodes go for the same queue the least-loaded one gets
/// it. The wait is capped at half the lease TTL.
///
/// A second, not less: it must outlast [`CLAIM_RETRY`], so that a node whose
/// first read failed — no leader yet at a cold start — is back claiming before
/// a node whose read succeeded has taken its second queue.
pub const CLAIM_STEP: Duration = Duration::from_secs(1);

/// The first wait after a read or a claim of a lease that failed for a reason
/// that passes — a transport error, a 408/429/5xx, no leader yet: about a
/// second (plus the claim's jitter, or the `Retry-After` the broker named),
/// doubling with each failure in a row up to the lease TTL. Only another node
/// HOLDING the queue waits a whole TTL; a failure that will not pass by itself
/// (a refusal) waits one too.
pub const CLAIM_RETRY: Duration = Duration::from_secs(1);

/// The random part of a claim's wait — under [`CLAIM_STEP`], so it orders nodes
/// of equal load without overturning the load order.
pub const CLAIM_JITTER: Duration = Duration::from_millis(200);

/// Where a claim's jitter comes from: a queue name in, a wait in
/// `[0, CLAIM_JITTER)` out. Random by default; a test injects its own with
/// [`Sink::with_claim_jitter`] to make the order of claims deterministic.
pub type ClaimJitter = Arc<dyn Fn(&str) -> Duration + Send + Sync>;

/// What every sink of one node shares, whichever tenants they run for: the
/// node's ONE memory budget (`QUEEN_S3_MEMORY_MB` — when it is reached, the
/// largest buffer of any tenant's queue closes first), its one metric registry
/// (so the exposition has one `# HELP`/`# TYPE` per family, each sink's series
/// told apart by its tenant label), and its claim load (the queues this node
/// runs or is claiming, of every tenant, which paces a claim).
pub struct SinkShared {
    budget: Arc<MemoryBudget>,
    metrics: Arc<Metrics>,
    load: Arc<AtomicUsize>,
}

impl SinkShared {
    /// The shared resources of a node with these knobs.
    pub fn new(node: &NodeKnobs) -> SinkShared {
        SinkShared {
            budget: Arc::new(MemoryBudget::new(node.memory_bytes())),
            metrics: Arc::new(Metrics::new()),
            load: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Every sink's metric families, rendered ONCE as Prometheus text
    /// exposition (version 0.0.4): one `# HELP`/`# TYPE` per family, the
    /// series of every sink of the node under it.
    pub fn prometheus(&self) -> String {
        self.metrics.render()
    }

    /// Drop every series labelled with this tenant, counters included: its
    /// sink is gone for good — its configuration was deleted — and has
    /// stopped. A sink restarted with a new configuration keeps its counters
    /// by not calling this. The gauges of a sink that stops go on their own.
    pub fn forget_tenant(&self, tenant: &str) {
        self.metrics.forget_tenant(tenant);
    }

    /// Bytes buffered across every queue of every sink of the node.
    pub fn buffered_bytes(&self) -> usize {
        self.budget.total()
    }

    /// The node's budget (`QUEEN_S3_MEMORY_MB`), in bytes.
    pub fn memory_limit_bytes(&self) -> usize {
        self.budget.limit()
    }
}

/// One line per thirty seconds while the bucket does not answer at boot.
static BUCKET_FAIL: Sampler = Sampler::new(30_000);
/// One line per five seconds for failed lease claims, across every queue.
static CLAIM_FAIL: Sampler = Sampler::new(5_000);

/// The S3 sink of one node.
///
/// Build it once ([`Sink::new`], or [`Sink::new_shared`] beside the node's
/// other sinks), run it once ([`Sink::run`]), and read [`Sink::status`] and
/// [`Sink::prometheus`] from any thread at any time — before, during and after
/// the run.
pub struct Sink {
    cfg: Arc<Config>,
    driver_cfg: Arc<DriverConfig>,
    queen: Arc<dyn QueenApi>,
    store: Arc<dyn ObjectStore>,
    writers: Arc<dyn WriterFactory>,
    metrics: Arc<Metrics>,
    health: Arc<HealthState>,
    status: Arc<StatusBoard>,
    budget: Arc<MemoryBudget>,
    load: Arc<AtomicUsize>,
    placement: Arc<Placement>,
    claim_jitter: ClaimJitter,
    running: AtomicBool,
}

impl Sink {
    /// A sink over `queen`, the broker's in-process [`QueenApi`], alone on its
    /// node: its own [`SinkShared`], from the node knobs `cfg` carries.
    ///
    /// `store` is for tests: `None` builds the real S3 client from the
    /// configuration. Nothing is contacted here — the bucket is probed by
    /// [`Sink::run`] — so the only errors are configuration errors, each naming
    /// its variable: the broker fails its boot with that line.
    pub fn new(
        cfg: Config,
        queen: Arc<dyn QueenApi>,
        store: Option<Arc<dyn ObjectStore>>,
    ) -> Result<Sink, String> {
        let shared = Arc::new(SinkShared::new(&cfg.node_knobs()));
        Sink::new_shared(cfg, queen, store, &shared)
    }

    /// A sink over `queen` — the in-process [`QueenApi`] of the tenant `cfg`
    /// names — sharing the node's budget, metric registry and claim load with
    /// every other sink built on `shared`. Its series carry the tenant label
    /// `cfg` asks for ([`Config::with_tenant_label`]). Errors as [`Sink::new`].
    pub fn new_shared(
        cfg: Config,
        queen: Arc<dyn QueenApi>,
        store: Option<Arc<dyn ObjectStore>>,
        shared: &Arc<SinkShared>,
    ) -> Result<Sink, String> {
        reject_static_partitions(&cfg)?;
        let metrics = Arc::new(shared.metrics.with_tenant(cfg.tenant_label.clone()));
        let store: Arc<dyn ObjectStore> = match store {
            Some(store) => store,
            None => {
                let mut client = S3Client::from_config(&cfg)?;
                if cfg.crash_at == CrashAt::MidUpload {
                    // The multipart half of the fault; the single-PUT half is in
                    // the driver, between the first object and the manifest.
                    client.on_mid_upload(mid_upload_abort_hook());
                }
                Arc::new(client.with_metrics(metrics.clone()))
            }
        };
        if !cfg.ignored.is_empty() {
            tracing::warn!(
                target: "queen-s3",
                ignored = ?cfg.ignored,
                "set but ignored: the sink runs inside the broker, which serves its status and \
                 metrics and owns the log format"
            );
        }
        if cfg.crash_at.is_armed() {
            tracing::warn!(
                target: "queen-s3",
                crash_at = cfg.crash_at.as_str(),
                "QUEEN_S3_CRASH_AT is set: this node aborts at that point of the commit sequence"
            );
        }
        tracing::info!(target: "queen-s3", "{}", cfg.boot_line());
        Ok(Sink {
            driver_cfg: Arc::new(DriverConfig::from_config(&cfg)),
            writers: Arc::from(factory(&cfg.writer)),
            health: Arc::new(HealthState::new(metrics.clone(), cfg.max_window_ms)),
            budget: shared.budget.clone(),
            load: shared.load.clone(),
            placement: Arc::new(Placement::new(Duration::from_millis(cfg.lease_ttl_ms))),
            status: Arc::new(StatusBoard::new()),
            cfg: Arc::new(cfg),
            queen,
            store,
            metrics,
            claim_jitter: Arc::new(|_queue: &str| {
                Duration::from_millis(rand::random::<u64>() % CLAIM_JITTER.as_millis() as u64)
            }),
            running: AtomicBool::new(false),
        })
    }

    /// Replace the random jitter of every claim with `jitter` (queue name in,
    /// wait out; kept under [`CLAIM_JITTER`]). For tests that need the order
    /// in which several nodes claim to be the same on every run.
    pub fn with_claim_jitter(
        mut self,
        jitter: impl Fn(&str) -> Duration + Send + Sync + 'static,
    ) -> Sink {
        self.claim_jitter = Arc::new(move |queue: &str| jitter(queue).min(CLAIM_JITTER));
        self
    }

    /// Run the sink until `stop` resolves, then drain and return.
    ///
    /// Probes the bucket first, retrying behind a backoff for as long as it
    /// does not answer; then starts one task per queue, each claiming the
    /// queue's lease and running its window protocol while it holds it. When
    /// `stop` resolves, every task stops reading, finishes the window it was
    /// committing — and the window it was filling if that is worth it — gives
    /// its lease back, and `run` returns once the last one has. A second call
    /// while one is running returns at once.
    ///
    /// Every line it logs is inside a `sink` span — `sink{tenant=<id>}` when
    /// the configuration names a tenant label ([`Config::with_tenant_label`],
    /// every tenant but the broker's default one), plain `sink` otherwise — and
    /// every line of one queue's task inside a `queue{queue=<name>}` span
    /// below it: two tenants' `window committed queue=orders` lines are told
    /// apart by the span the subscriber prints in front of them.
    pub async fn run(&self, stop: impl Future<Output = ()> + Send) {
        let span = tracing::info_span!(target: "queen-s3", "sink", tenant = tracing::field::Empty);
        if self.cfg.tenant_label.is_some() {
            span.record("tenant", self.cfg.tenant.as_str());
        }
        self.run_in_span(stop).instrument(span).await
    }

    async fn run_in_span(&self, stop: impl Future<Output = ()> + Send) {
        if self.running.swap(true, Ordering::SeqCst) {
            tracing::warn!(target: "queen-s3", "the sink is already running; this call returns");
            return;
        }
        let shutdown = Shutdown::new();
        let work = self.serve(shutdown.clone());
        tokio::pin!(work);
        tokio::pin!(stop);
        let mut stopping = false;
        loop {
            tokio::select! {
                () = &mut work => break,
                () = &mut stop, if !stopping => {
                    stopping = true;
                    tracing::info!(
                        target: "queen-s3",
                        "stopping: no new reads, the window in flight is finished"
                    );
                    shutdown.trigger();
                }
            }
        }
        // Every queue took its own gauges when it stopped; whatever a task
        // that never got to finish left goes with the sink.
        self.metrics.forget_scope_gauges();
        if self.budget.queues() == 0 {
            self.metrics.forget_safe_lag();
        }
        tracing::info!(target: "queen-s3", "every queue drained; the sink has stopped");
        self.running.store(false, Ordering::SeqCst);
    }

    /// The sink as an operator reads it: the configuration that matters, the
    /// bucket, the health verdict, and one row per queue this node has a task
    /// for — owned here or held by which node, the engine's state, the last
    /// committed `k` and `tEnd`, `completeThrough` (the stamp the lake is
    /// complete through), the lag (`safeTime − completeThrough`, on this node's
    /// clock), the last error, and what this node committed for it.
    pub fn status(&self) -> Value {
        let cfg = &self.cfg;
        let verdict = self.health.verdict();
        let mut bucket = self.status.bucket_json();
        if let Value::Object(m) = &mut bucket {
            m.insert("name".into(), Value::String(cfg.bucket.clone()));
            m.insert("endpoint".into(), Value::String(cfg.endpoint.clone()));
            m.insert("region".into(), Value::String(cfg.region.clone()));
            m.insert("prefix".into(), Value::String(cfg.prefix.clone()));
        }
        let compression = match cfg.writer.format {
            Format::Jsonl => wire(&cfg.writer.compression),
            Format::Parquet => wire(&cfg.writer.parquet_codec),
        };
        json!({
            "ok": verdict.is_healthy() && self.status.bucket_reachable() != Some(false),
            "tenant": cfg.tenant,
            "sink": cfg.sink,
            "instance": cfg.instance,
            "version": CRATE_VERSION,
            "running": self.running.load(Ordering::SeqCst),
            "bucket": bucket,
            "format": wire(&cfg.writer.format),
            "compression": compression,
            "layout": wire(&cfg.layout),
            "align": wire(&cfg.align),
            "health": verdict.to_json(),
            "placement": {
                "nodes": self.placement.nodes(),
                "queues": self.placement.queues(),
                "share": self.placement.share(),
                "held": self.placement.held(),
                "givingBack": self.placement.shedding(),
            },
            "memory": {
                "bufferedBytes": self.budget.total(),
                "limitBytes": self.budget.limit(),
            },
            "queues": self.status.queues_json(),
        })
    }

    /// The metric registry this sink writes into, as Prometheus text
    /// exposition (version 0.0.4). A sink built with [`Sink::new`] has its own
    /// registry, so this is its own series; a sink built on a [`SinkShared`]
    /// renders the WHOLE shared registry — every sink of the node — so a broker
    /// that runs several renders [`SinkShared::prometheus`] once instead.
    pub fn prometheus(&self) -> String {
        self.metrics.render()
    }

    /// The bucket, then the queues.
    async fn serve(&self, shutdown: Shutdown) {
        if !self.wait_for_bucket(&shutdown).await {
            return;
        }
        let rt = Runtime {
            cfg: self.driver_cfg.clone(),
            queen: self.queen.clone(),
            store: self.store.clone(),
            writers: self.writers.clone(),
            metrics: self.metrics.clone(),
            health: self.health.clone(),
            status: self.status.clone(),
            budget: self.budget.clone(),
            shutdown,
        };
        let claims = Claims {
            load: self.load.clone(),
            jitter: self.claim_jitter.clone(),
            placement: self.placement.clone(),
        };
        supervise(rt, self.cfg.clone(), claims).await;
    }

    /// A HEAD of the prefix until it answers: 404 is a fine answer (nothing
    /// written yet), a 403 or a connection failure is the credential or the
    /// endpoint being wrong — or the bucket being down. Either way the broker
    /// keeps running and the queues wait. `false` when the stop came first.
    async fn wait_for_bucket(&self, shutdown: &Shutdown) -> bool {
        let probe = format!("{}/", self.cfg.prefix);
        let mut backoff = Backoff::new(1_000, MAX_BACKOFF_MS);
        loop {
            if shutdown.is_set() {
                return false;
            }
            match self.store.head(&probe).await {
                Ok(_) => {
                    if self.status.bucket_reachable() == Some(false) {
                        tracing::info!(target: "queen-s3", bucket = %self.cfg.bucket, "the bucket answers; starting the queues");
                    }
                    self.status.bucket_ok();
                    return true;
                }
                Err(e) => {
                    let msg = format!(
                        "cannot reach the bucket {} at {} ({e}). QUEEN_S3_ACCESS_KEY/_SECRET_KEY \
                         must be allowed s3:PutObject, s3:GetObject and s3:ListBucket on {}/*, \
                         and QUEEN_S3_PATH_STYLE=true is what most gateways need",
                        self.cfg.bucket, self.cfg.endpoint, self.cfg.prefix
                    );
                    if let Some(suppressed) = BUCKET_FAIL.tick_now() {
                        tracing::error!(target: "queen-s3", suppressed, "{msg}; retrying");
                    }
                    self.status.bucket_error(msg);
                    if sleep_or_stop(shutdown, backoff.next_delay(&e)).await {
                        return false;
                    }
                }
            }
        }
    }
}

/// Keep one task per queue alive for as long as the sink runs, and wait for
/// all of them once it stops.
///
/// With `QUEEN_S3_QUEUES=*` the set is re-listed every ten discovery intervals:
/// often enough that a queue created at ten in the morning is being sinked a
/// minute later, rarely enough that the listing is not a load: one
/// `list_queues` per node every ten discovery intervals (20 s by default, never
/// under 1 s). Queues are never removed — a task whose queue disappeared
/// discovers `UNKNOWN_TOPIC_OR_PARTITION`, stops as missing and looks again
/// every [`MISSING_RETRY`], which is also what should happen if the queue
/// comes back. A task that panics is logged,
/// reported, and started again after [`TERMINAL_RETRY`]: a bug in one queue
/// must not stop the others, nor that queue for the life of the broker.
async fn supervise(rt: Runtime, cfg: Arc<Config>, claims: Claims) {
    // The presence row and the share decision, beside the queue tasks; it
    // ends at the stop, and the row is deleted once every queue is released.
    let presence = Arc::new(Presence::new(
        rt.queen.clone(),
        &cfg.sink,
        &cfg.instance,
        cfg.lease_ttl_ms,
    ));
    let mut rebalancer = AbortOnDrop(tokio::spawn(
        rebalance(
            rt.clone(),
            cfg.clone(),
            claims.placement.clone(),
            presence.clone(),
        )
        .instrument(tracing::Span::current()),
    ));
    let mut tasks: JoinSet<()> = JoinSet::new();
    let mut names: HashMap<tokio::task::Id, String> = HashMap::new();
    let mut started: BTreeSet<String> = BTreeSet::new();
    let relist = Duration::from_millis(cfg.discovery_interval_ms.saturating_mul(10).max(1_000));
    let mut listing_due = true;

    loop {
        if rt.shutdown.is_set() {
            break;
        }
        if listing_due {
            listing_due = false;
            let queues = match &cfg.queues {
                Queues::Named(names) => names.clone(),
                Queues::All => match rt.queen.list_queues().await {
                    Ok(names) => names,
                    Err(e) => {
                        tracing::warn!(target: "queen-s3", error = %e, "cannot list the queues; keeping the set this node already has");
                        Vec::new()
                    }
                },
            };
            for queue in queues {
                if started.insert(queue.clone()) {
                    claims.placement.set_queues(started.len());
                    tracing::info!(target: "queen-s3", queue = %queue, "starting a queue task");
                    let handle = tasks.spawn(
                        own_and_run(rt.clone(), cfg.clone(), queue.clone(), claims.clone())
                            .instrument(queue_span(&queue)),
                    );
                    names.insert(handle.id(), queue);
                }
            }
        }
        let relisting = cfg.queues.is_all();
        tokio::select! {
            _ = rt.shutdown.wait() => break,
            _ = tokio::time::sleep(relist), if relisting => listing_due = true,
            Some(joined) = tasks.join_next_with_id() => {
                let (id, panicked) = match joined {
                    Ok((id, ())) => (id, None),
                    Err(e) => (e.id(), Some(e.to_string())),
                };
                let Some(queue) = names.remove(&id) else { continue };
                if let Some(why) = panicked {
                    tracing::error!(
                        target: "queen-s3",
                        queue = %queue,
                        error = %why,
                        retry_in_s = TERMINAL_RETRY.as_secs(),
                        "queue task panicked; it will be started again"
                    );
                    rt.status.error(&queue, format!("queue task panicked: {why}"));
                    rt.health.forget_queue(&queue);
                    stop_exporting(&rt, &queue);
                    let (rt2, cfg2, q2, c2) =
                        (rt.clone(), cfg.clone(), queue.clone(), claims.clone());
                    let span = queue_span(&q2);
                    let handle = tasks.spawn(
                        async move {
                            if !sleep_or_stop(&rt2.shutdown, TERMINAL_RETRY).await {
                                own_and_run(rt2, cfg2, q2, c2).await;
                            }
                        }
                        .instrument(span),
                    );
                    names.insert(handle.id(), queue);
                }
            }
        }
    }

    while let Some(joined) = tasks.join_next_with_id().await {
        if let Err(e) = joined {
            let queue = names.remove(&e.id()).unwrap_or_default();
            tracing::error!(target: "queen-s3", queue = %queue, error = %e, "queue task panicked while stopping");
        }
    }
    // Every queue is released: this node leaves, and the others count one
    // node fewer at once rather than a TTL from now.
    let _ = (&mut rebalancer.0).await;
    presence.leave().await;
}

/// A spawned task that ends with the value holding it — the rebalancer of a
/// sink whose run is dropped, as a crashed node's would be.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The span a queue's task runs in: `queue{queue=<name>}`, below the sink's.
fn queue_span(queue: &str) -> tracing::Span {
    tracing::info_span!(target: "queen-s3", "queue", queue = %queue)
}

/// What the queue tasks of one node share to claim queues in turn.
#[derive(Clone)]
struct Claims {
    /// The queues this node runs, plus the ones it is claiming this instant —
    /// of every tenant's sink on the node ([`SinkShared`]).
    load: Arc<AtomicUsize>,
    jitter: ClaimJitter,
    /// This sink's view of where its queues run ([`crate::placement`]).
    placement: Arc<Placement>,
}

/// One unit of [`Claims::load`], given back when dropped.
struct LoadSlot(Arc<AtomicUsize>);

impl LoadSlot {
    fn take(load: &Arc<AtomicUsize>) -> (LoadSlot, usize) {
        let before = load.fetch_add(1, Ordering::SeqCst);
        (LoadSlot(load.clone()), before)
    }
}

impl Drop for LoadSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// What waiting for a turn to claim a free queue came to.
enum Turn {
    /// Claim it now, holding this place in the node's load.
    Claim(LoadSlot),
    /// Another node took it in the meantime.
    Held(String),
    /// The sink is stopping.
    Stop,
}

/// Wait this node's turn to claim a FREE queue: the jitter plus
/// [`CLAIM_STEP`] for every queue this node runs or is claiming, capped at half
/// the TTL — and, on top of that ([`crate::placement`]), one more TTL when this
/// sink already holds its fair share, or until the end of the cooldown when
/// this node gave this very queue back. Everything is read again at least
/// every third of a TTL — the load, the share and the cooldown all move — and
/// the queue is looked at as often, so a node that waits sees another take it.
/// The slot is reserved BEFORE the comparison, so two tasks of one node cannot
/// both pass on the same count.
async fn wait_turn(
    claims: &Claims,
    lease: &Lease,
    queue: &str,
    ttl: Duration,
    shutdown: &Shutdown,
) -> Turn {
    let start = tokio::time::Instant::now();
    let jitter = (claims.jitter)(queue);
    let cap = ttl / 2;
    loop {
        let (slot, before) = LoadSlot::take(&claims.load);
        let step = CLAIM_STEP.saturating_mul(u32::try_from(before).unwrap_or(u32::MAX));
        let paced = start + jitter + step.min(cap);
        let due = match claims.placement.cooldown_until(queue) {
            // Given back by this node: the others' first, then anyone's.
            Some(until) => paced.max(until),
            // At its share: the nodes below theirs first.
            None if claims.placement.at_or_above_share() => paced + ttl,
            None => paced,
        };
        let now = tokio::time::Instant::now();
        if now >= due {
            return Turn::Claim(slot);
        }
        drop(slot);
        if sleep_or_stop(shutdown, (due - now).min(ttl / 3)).await {
            return Turn::Stop;
        }
        if tokio::time::Instant::now() < due {
            if let Ok(Holder::Other(owner)) = lease.holder().await {
                return Turn::Held(owner);
            }
        }
    }
}

/// One queue, for the life of the sink: claim the lease, run the driver while
/// it is held, and decide what to do with the way it stopped.
async fn own_and_run(rt: Runtime, cfg: Arc<Config>, queue: String, claims: Claims) {
    // Every node claims every queue and all but one lose, once per TTL each:
    // the owner is logged when it changes, not every time it is confirmed.
    let mut last_holder: Option<String> = None;
    let mut missing_logged = false;
    // Failed reads or claims in a row, for the backoff of a transient failure.
    let mut failures: u32 = 0;
    let ttl = Duration::from_millis(cfg.lease_ttl_ms);
    while !rt.shutdown.is_set() {
        rt.status.claiming(&queue);
        let lease = Arc::new(Lease::new(
            rt.queen.clone(),
            &cfg.sink,
            &queue,
            &cfg.instance,
            cfg.lease_ttl_ms,
        ));
        // One read first: a queue somebody else holds costs no write and no
        // wait, a queue an earlier life of this node holds is taken back at
        // once, and only a free one waits this node's turn.
        let slot = match lease.holder().await {
            Ok(Holder::Free) => match wait_turn(&claims, &lease, &queue, ttl, &rt.shutdown).await {
                Turn::Claim(slot) => slot,
                Turn::Held(owner) => {
                    failures = 0;
                    held_elsewhere(&rt, &queue, owner, &mut last_holder);
                    sleep_or_stop(&rt.shutdown, held_wait(&claims, &queue, ttl)).await;
                    continue;
                }
                Turn::Stop => break,
            },
            Ok(Holder::Mine) => LoadSlot::take(&claims.load).0,
            Ok(Holder::Other(owner)) => {
                failures = 0;
                held_elsewhere(&rt, &queue, owner, &mut last_holder);
                sleep_or_stop(&rt.shutdown, held_wait(&claims, &queue, ttl)).await;
                continue;
            }
            Err(e) => {
                failures = failures.saturating_add(1);
                claim_failed(&rt, &queue, &e);
                let wait = claim_retry_delay(failures, &e, ttl, (claims.jitter)(&queue));
                sleep_or_stop(&rt.shutdown, wait).await;
                continue;
            }
        };
        match lease.acquire().await {
            Ok(Acquired::Taken) => {}
            Ok(Acquired::HeldBy(owner)) => {
                drop(slot);
                failures = 0;
                held_elsewhere(&rt, &queue, owner, &mut last_holder);
                sleep_or_stop(&rt.shutdown, held_wait(&claims, &queue, ttl)).await;
                continue;
            }
            Err(e) => {
                drop(slot);
                failures = failures.saturating_add(1);
                claim_failed(&rt, &queue, &e);
                let wait = claim_retry_delay(failures, &e, ttl, (claims.jitter)(&queue));
                sleep_or_stop(&rt.shutdown, wait).await;
                continue;
            }
        }
        failures = 0;
        if !missing_logged {
            // Not while a missing queue is looked for every few seconds: that
            // would be this line every few seconds.
            tracing::info!(target: "queen-s3", queue = %queue, instance = %cfg.instance, "this node owns the queue");
        }
        last_holder = None;

        // The refresher lives exactly as long as the driver: its handle aborts
        // the task on drop, so a queue this node stops running is a queue it
        // stops claiming to own.
        let refresher = spawn_refresh(lease.clone());
        let handoff = claims.placement.begin_ownership(&queue);
        let driver = QueueDriver::new(rt.clone(), queue.clone(), lease.clone())
            .with_handoff(handoff.clone());
        let stop = driver.run().await;
        drop(refresher);
        lease.release().await;
        // Given back, not stopped: the drain was this queue's alone.
        let handed_off = handoff.is_set() && !rt.shutdown.is_set() && stop == Stop::Drained;
        claims.placement.end_ownership(&queue, handed_off);
        drop(slot);

        if handed_off {
            rt.metrics.queue_given_back();
            rt.status.released(&queue);
            tracing::info!(
                target: "queen-s3",
                queue = %queue,
                "gave the queue back so a node below its fair share can take it"
            );
            missing_logged = false;
            continue;
        }
        if !matches!(stop, Stop::Missing(_)) {
            missing_logged = false;
        }
        match stop {
            Stop::Drained => break,
            Stop::Crashed(_) => break,
            Stop::Fenced(_) => {
                sleep_or_stop(&rt.shutdown, ttl).await;
            }
            Stop::Missing(why) => {
                if !missing_logged {
                    missing_logged = true;
                    tracing::info!(
                        target: "queen-s3",
                        queue = %queue,
                        why,
                        retry_in_s = MISSING_RETRY.as_secs(),
                        "the queue does not exist; looking again until it does"
                    );
                }
                sleep_or_stop(&rt.shutdown, MISSING_RETRY).await;
            }
            Stop::Failed(why) => {
                tracing::error!(
                    target: "queen-s3",
                    queue = %queue,
                    why,
                    retry_in_s = TERMINAL_RETRY.as_secs(),
                    "queue stopped; it will be tried again in case the cause was fixed"
                );
                sleep_or_stop(&rt.shutdown, TERMINAL_RETRY).await;
            }
        }
    }
    // This node no longer runs the queue, whatever the last run said.
    rt.health.forget_queue(&queue);
}

/// How long to wait before looking again at a queue another node holds: a
/// third of a TTL while this sink is below its fair share — a queue given back
/// should go to a node that is short ([`crate::placement`]) — and a whole TTL
/// once it holds its share, plus the claim's jitter either way.
fn held_wait(claims: &Claims, queue: &str, ttl: Duration) -> Duration {
    let base = match claims.placement.at_or_above_share() {
        true => ttl,
        false => ttl / 3,
    };
    base + (claims.jitter)(queue)
}

/// Another node holds the queue: report it, and stop counting the queue in
/// this node's health.
fn held_elsewhere(rt: &Runtime, queue: &str, owner: String, last_holder: &mut Option<String>) {
    if last_holder.as_deref() != Some(owner.as_str()) {
        tracing::info!(
            target: "queen-s3",
            queue = %queue,
            owner = %owner,
            "another node owns this queue; claiming again after the lease TTL"
        );
    }
    rt.status.held_by(queue, &owner);
    rt.health.forget_queue(queue);
    *last_holder = Some(owner);
}

/// How long to wait after the `failures`-th failed read or claim in a row
/// ([`CLAIM_RETRY`]): a transient failure is retried soon, so that a node that
/// met no leader right after boot still claims its share; anything else waits
/// a TTL.
fn claim_retry_delay(
    failures: u32,
    e: &crate::types::SinkError,
    ttl: Duration,
    jitter: Duration,
) -> Duration {
    if !e.is_retriable() {
        return ttl;
    }
    if let crate::types::SinkError::Status {
        retry_after_ms: Some(ms),
        ..
    } = e
    {
        if *ms >= 0 {
            return Duration::from_millis(*ms as u64).min(ttl);
        }
    }
    let shift = failures.saturating_sub(1).min(16);
    CLAIM_RETRY
        .saturating_mul(1u32 << shift)
        .saturating_add(jitter)
        .min(ttl)
}

fn claim_failed(rt: &Runtime, queue: &str, e: &crate::types::SinkError) {
    if let Some(suppressed) = CLAIM_FAIL.tick_now() {
        tracing::warn!(target: "queen-s3", queue = %queue, error = %e, suppressed, "cannot claim the queue lease; retrying");
    }
    rt.status.error(queue, format!("lease claim: {e}"));
}

/// Sleep for `d` or until the stop; `true` when the stop came first.
async fn sleep_or_stop(shutdown: &Shutdown, d: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(d) => shutdown.is_set(),
        _ = shutdown.wait() => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SinkError;

    const TTL: Duration = Duration::from_secs(30);

    #[test]
    fn a_transient_claim_failure_is_retried_soon_and_backs_off_to_the_ttl() {
        let busy = SinkError::Status {
            code: 503,
            body: String::new(),
            retry_after_ms: None,
        };
        let j = Duration::from_millis(150);
        assert_eq!(
            claim_retry_delay(1, &busy, TTL, j),
            Duration::from_millis(1_150)
        );
        assert_eq!(
            claim_retry_delay(2, &busy, TTL, j),
            Duration::from_millis(2_150)
        );
        assert_eq!(
            claim_retry_delay(3, &busy, TTL, j),
            Duration::from_millis(4_150)
        );
        assert_eq!(
            claim_retry_delay(9, &busy, TTL, j),
            TTL,
            "never past the TTL"
        );
        let gone = SinkError::Transport("connection reset".into());
        assert_eq!(
            claim_retry_delay(1, &gone, TTL, j),
            Duration::from_millis(1_150)
        );
        // The broker's own Retry-After, capped at the TTL.
        let paced = SinkError::Status {
            code: 429,
            body: String::new(),
            retry_after_ms: Some(3_000),
        };
        assert_eq!(claim_retry_delay(1, &paced, TTL, j), Duration::from_secs(3));
        let long = SinkError::Status {
            code: 503,
            body: String::new(),
            retry_after_ms: Some(120_000),
        };
        assert_eq!(claim_retry_delay(1, &long, TTL, j), TTL);
        // What does not pass by itself waits a TTL.
        let refused = SinkError::Status {
            code: 403,
            body: String::new(),
            retry_after_ms: None,
        };
        assert_eq!(claim_retry_delay(1, &refused, TTL, j), TTL);
        assert_eq!(
            claim_retry_delay(1, &SinkError::Config("x".into()), TTL, j),
            TTL
        );
    }
}
