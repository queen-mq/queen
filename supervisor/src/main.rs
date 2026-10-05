mod coordination;
mod lease;
mod prefork;
mod remote_status;
mod watch;

use coordination::{CoordinationConfig, Coordinator};
use prefork::{ForkServer, WorkerProcess};
use remote_status::{RemoteStatusConfig, RemoteStatusPublisher};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CONFIG_VERSION: u32 = 2;
const STATUS_SCHEMA: &str = "queen.supervisor.status/v1";
const MAX_CONFIG_BYTES: u64 = 1_048_576;
const MAX_STATUS_BYTES: u64 = 1_048_576;
const MAX_CONTROL_BYTES: u64 = 16_384;
const MAX_TELEMETRY_BYTES: u64 = 65_536;
const MAX_TELEMETRY_FILES: usize = 4_096;
const MAX_TELEMETRY_DIRECTORY_ENTRIES: usize = 8_192;
const MAX_TELEMETRY_TOTAL_BYTES: u64 = 16_777_216;
const MAX_TELEMETRY_QUEUES_PER_FILE: usize = 256;
const MAX_PROCESS_LIMIT: usize = 4_096;
const MAX_QUEUES_PER_SUPERVISOR: usize = 1_024;
const MAX_STATUS_POOLS: usize = 256;
const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_QUEUE_BYTES: usize = 256;
const MAX_DURATION_SECONDS: u64 = 31_536_000;
const MIN_SCALING_SECONDS: f64 = 0.000_001;
const MIN_CONTROL_TTL_SECONDS: u64 = 30;
const MAX_CONTROL_TTL_SECONDS: u64 = 86_400;
const CONFIG_EXPORT_TIMEOUT_SECONDS: u64 = 60;
const CRASH_CIRCUIT_THRESHOLD: u32 = 5;
const MAX_EXIT_MARKER_BYTES: u64 = 64;
const MAX_EXIT_MARKER_ENTRIES: usize = 8_192;
const JOB_TIMEOUT_MARKER: &str = "timeout";
const MEMORY_LIMIT_MARKER: &str = "memory";
const QUEUE_RESTART_MARKER: &str = "restart";
// Laravel's Worker::EXIT_MEMORY_LIMIT.
const LARAVEL_EXIT_MEMORY_LIMIT: i32 = 12;
const MAX_DEPTH_POLL_CONCURRENCY: usize = 16;
const CONTROL_CLOCK_SKEW_SECONDS: u64 = 5;
const PROCESS_START_BUDGET_SECONDS: u64 = 5;
const TELEMETRY_SCAN_BUDGET_SECONDS: u64 = 60;
const CONTROL_LOOP_MARGIN_SECONDS: u64 = 5;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    version: u32,
    cwd: String,
    php_binary: String,
    artisan: String,
    state_directory: String,
    poll_interval: u64,
    http_timeout: u64,
    shutdown_grace: u64,
    telemetry_ttl: u64,
    #[serde(default = "default_control_ttl")]
    control_ttl: u64,
    #[serde(default = "default_heartbeat_timeout")]
    heartbeat_timeout: u64,
    process_limit: usize,
    queen: QueenConfig,
    #[serde(default)]
    connections: HashMap<String, QueenConfig>,
    supervisors: HashMap<String, SupervisorConfig>,
    // Emitted by Laravel only when enabled, so an absent key means disabled.
    #[serde(default)]
    remote_status: Option<RemoteStatusConfig>,
    // Emitted by Laravel only when enabled, so an absent key means disabled.
    #[serde(default)]
    coordination: Option<CoordinationConfig>,
    // Emitted by Laravel only when enabled.
    #[serde(default)]
    prefork: bool,
    // Emitted by Laravel only when enabled, so an absent key means disabled.
    #[serde(default)]
    event_driven: Option<watch::EventDrivenConfig>,
    // queen.supervisor.lease_service: whether the master serves lease
    // renewal on Linux. Emitted by Laravel only when false, so an absent key,
    // as in documents from earlier releases, means on.
    #[serde(default = "default_lease_service")]
    lease_service: bool,
    // The master's lease renewal socket, set when the service runs.
    #[serde(skip)]
    lease_socket: Option<String>,
    // Where workers leave exit markers, set once that directory is ready.
    #[serde(skip)]
    exit_markers: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(deny_unknown_fields)]
struct QueenConfig {
    #[serde(default)]
    url: String,
    #[serde(default)]
    urls: Vec<String>,
    #[serde(default)]
    bearer_token: Option<String>,
    #[serde(default, deserialize_with = "deserialize_headers")]
    headers: HashMap<String, String>,
}

fn deserialize_headers<'de, D>(deserializer: D) -> Result<HashMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Headers {
        Map(HashMap<String, String>),
        Sequence(Vec<serde_json::Value>),
    }

    match Headers::deserialize(deserializer)? {
        Headers::Map(headers) => Ok(headers),
        Headers::Sequence(headers) if headers.is_empty() => Ok(HashMap::new()),
        Headers::Sequence(_) => Err(serde::de::Error::custom(
            "headers must be a JSON object or an empty legacy array",
        )),
    }
}

#[derive(Deserialize)]
struct DepthResponse {
    #[serde(rename = "effectivePending")]
    effective_pending: Option<u64>,
    pending: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SupervisorConfig {
    connection: String,
    consumer_group: String,
    queues: Vec<String>,
    balance: String,
    strategy: String,
    processes: usize,
    min_processes: usize,
    max_processes: usize,
    target_jobs_per_process: usize,
    target_clear_seconds: f64,
    default_runtime_seconds: f64,
    balance_cooldown: u64,
    balance_max_shift: usize,
    #[serde(default = "default_retry_after")]
    retry_after: u64,
    // Laravel validates the renewal timing budget before exporting this
    // contract. On Linux the master serves renewal for these workers (see
    // lease.rs); the timing itself arrives from each worker.
    #[serde(default)]
    lease_renewal: bool,
    #[serde(default = "default_scale_down_delay")]
    scale_down_delay: u64,
    // Emitted by Laravel only when set; auto pools only.
    #[serde(default)]
    min_processes_per_queue: usize,
    // Emitted by Laravel only when true.
    #[serde(default)]
    fast_scale_up: bool,
    #[serde(default = "default_restart_backoff")]
    restart_backoff: u64,
    #[serde(default = "default_restart_backoff_max")]
    restart_backoff_max: u64,
    #[serde(default = "default_stable_after")]
    stable_after: u64,
    sleep: u64,
    timeout: u64,
    tries: u64,
    memory: u64,
    backoff: u64,
    max_jobs: u64,
    max_time: u64,
    rest: u64,
    force: bool,
    #[serde(default = "default_quiet")]
    quiet: bool,
}

fn default_retry_after() -> u64 {
    90
}

fn default_control_ttl() -> u64 {
    3_600
}

fn default_heartbeat_timeout() -> u64 {
    3_600
}

fn default_scale_down_delay() -> u64 {
    10
}

fn default_restart_backoff() -> u64 {
    1
}

fn default_restart_backoff_max() -> u64 {
    30
}

fn default_stable_after() -> u64 {
    60
}

fn default_quiet() -> bool {
    true
}

fn default_lease_service() -> bool {
    true
}

type PoolKey = (String, String);
type Pools = HashMap<PoolKey, Vec<Worker>>;
type RestartStates = HashMap<PoolKey, RestartGuard>;
type Draining = Vec<DrainingWorker>;
type PendingTelemetryCleanup = HashMap<String, HashSet<u32>>;

struct Worker {
    child: WorkerProcess,
    started_at: Instant,
    stability_reported: bool,
    restart_probe: bool,
}

struct DrainingWorker {
    worker: Worker,
    deadline: Instant,
    label: String,
    pool: PoolKey,
}

impl Worker {
    fn new(child: Child, restart_probe: bool) -> Self {
        Self::from_process(WorkerProcess::Spawned(child), restart_probe)
    }

    fn from_process(child: WorkerProcess, restart_probe: bool) -> Self {
        Self {
            child,
            started_at: Instant::now(),
            stability_reported: false,
            restart_probe,
        }
    }
}

#[derive(Debug, Default)]
struct ScaleGuard {
    downscale_since: Option<Instant>,
    candidate_total: Option<usize>,
}

#[derive(Debug, Default)]
struct RestartGuard {
    consecutive_failures: u32,
    phase: RestartPhase,
}

#[derive(Clone, Copy, Debug, Default)]
enum RestartPhase {
    #[default]
    Closed,
    Backoff {
        until: Instant,
    },
    Open {
        until: Instant,
    },
    Probe,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SpawnPermission {
    Blocked,
    Normal,
    Probe,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Control {
    command: ControlCommand,
    nonce: String,
    instance_id: String,
    #[serde(default, rename = "requested_at")]
    _requested_at: Option<String>,
    requested_at_epoch: u64,
    expires_at_epoch: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ControlCommand {
    Pause,
    Continue,
    Terminate,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TelemetryDocument {
    #[serde(rename = "pid")]
    _pid: u32,
    #[serde(rename = "updated_at_epoch")]
    _updated_at_epoch: u64,
    supervisor: String,
    connection: String,
    consumer_group: String,
    queues: HashMap<String, TelemetryQueue>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TelemetryQueue {
    samples: u64,
    runtime_ewma_seconds: f64,
    #[serde(default, rename = "failures")]
    _failures: u64,
}

struct TelemetryScope<'a> {
    supervisor: &'a str,
    connection: &'a str,
    consumer_group: &'a str,
}

struct State {
    directory: PathBuf,
    instance_id: String,
    hostname: Option<String>,
    _lock: File,
    #[cfg(unix)]
    directory_device: u64,
    #[cfg(unix)]
    directory_inode: u64,
}

struct StatusSnapshot<'a> {
    config: &'a Config,
    pools: &'a Pools,
    restarts: &'a RestartStates,
    draining: &'a Draining,
    desired: &'a HashMap<String, HashMap<String, usize>>,
    depths: &'a HashMap<String, HashMap<String, usize>>,
    depths_available: &'a HashMap<String, bool>,
    /// Live replicas sharing each coordinated pool's target.
    replicas: &'a HashMap<String, usize>,
}

const HELP: &str = "queen-supervisor - low-memory Laravel worker supervisor\n\n\
Usage:\n  queen-supervisor [--php <binary>] [--artisan <path>]\n  \
queen-supervisor --config <path>\n\n\
Options:\n  --php <binary>    PHP executable used to export Laravel configuration\n  \
--artisan <path>   Artisan entry point (default: artisan)\n  \
--config <path>    Read a private resolved v2 configuration file\n  \
-h, --help         Print help\n  \
-V, --version      Print version\n";

#[derive(Debug, Eq, PartialEq)]
enum CliAction {
    Run(CliOptions),
    Help,
    Version,
}

#[derive(Debug, Eq, PartialEq)]
struct CliOptions {
    config: Option<PathBuf>,
    php: String,
    artisan: String,
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let options = match parse_args(&args) {
        Ok(CliAction::Help) => {
            print!("{HELP}");
            return;
        }
        Ok(CliAction::Version) => {
            println!("queen-supervisor {}", env!("CARGO_PKG_VERSION"));
            return;
        }
        Ok(CliAction::Run(options)) => options,
        Err(error) => {
            eprintln!("queen-supervisor: {error}\n\n{HELP}");
            std::process::exit(2);
        }
    };
    if let Err(error) = run(&options) {
        eprintln!("queen-supervisor: {error}");
        std::process::exit(1);
    }
}

fn run(options: &CliOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = load_config(options)?;
    validate_config(&config)?;
    let state = State::acquire(&config.state_directory)?;
    // State::acquire validates the operator-provided spelling before
    // canonicalizing it. Every runtime consumer, including worker telemetry,
    // must use that same pinned namespace rather than resolving the alias a
    // second time.
    config.state_directory = state
        .directory
        .to_str()
        .ok_or("canonical state_directory is not valid UTF-8")?
        .to_owned();

    config.exit_markers = match prepare_exit_markers(&state.directory) {
        Ok(directory) => Some(directory.to_string_lossy().into_owned()),
        Err(error) => {
            eprintln!("exit markers disabled, a job timeout counts as a crash: {error}");
            None
        }
    };
    config.lease_socket = start_lease_service(&config, &state.directory);

    let running = Arc::new(AtomicBool::new(true));
    let signal = Arc::clone(&running);
    ctrlc::set_handler(move || signal.store(false, Ordering::SeqCst))?;

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(config.http_timeout))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut pools = Pools::new();
    let mut draining = Draining::new();
    let mut restarts = RestartStates::new();
    let mut pending_telemetry_cleanup = PendingTelemetryCleanup::new();
    let mut scale_guards: HashMap<String, ScaleGuard> = HashMap::new();
    let mut last_reconcile: HashMap<String, Instant> = HashMap::new();
    let mut last_desired: HashMap<String, HashMap<String, usize>> = HashMap::new();
    let mut last_depths: HashMap<String, HashMap<String, usize>> = HashMap::new();
    let mut depths_available: HashMap<String, bool> = HashMap::new();
    let mut last_poll = Instant::now()
        .checked_sub(Duration::from_secs(config.poll_interval))
        .unwrap_or_else(Instant::now);
    let mut paused = false;
    let mut last_command_nonce: Option<String> = None;
    let mut status_failure: Option<String> = None;
    let mut remote_status = config
        .remote_status
        .as_ref()
        .map(RemoteStatusPublisher::new);
    let mut coordinator = config
        .coordination
        .as_ref()
        .map(|settings| Coordinator::new(settings, &state.instance_id, state.hostname.as_deref()));
    let coordinated_scopes = coordinated_scopes(&config);
    let mut fork_server = if config.prefork {
        match ForkServer::start(&config, &running) {
            Ok(server) => {
                eprintln!("prefork: fork server started");
                Some(Rc::new(RefCell::new(server)))
            }
            Err(error) => {
                eprintln!("prefork disabled, spawning workers: {error}");
                None
            }
        }
    } else {
        None
    };
    // Fork servers replaced after queue:restart, open while a worker they
    // forked still runs; see refresh_fork_server.
    let mut retired_forks: Vec<Rc<RefCell<ForkServer>>> = Vec::new();
    let mut replica_counts = current_replica_counts(coordinator.as_ref(), &coordinated_scopes);
    let wakes = match config.event_driven.as_ref() {
        Some(settings) => Some(watch::start(&config, settings, Arc::clone(&running))?),
        None => None,
    };
    // Pools still climbing towards a higher target after an event-driven
    // reconcile was held back by its step budget.
    let mut climbing: HashSet<String> = HashSet::new();
    // Pools a watched queue woke, kept until they are evaluated.
    let mut woken: HashSet<String> = HashSet::new();
    let mut last_evaluated: HashMap<String, Instant> = HashMap::new();

    eprintln!(
        "queen-supervisor started ({} pool definitions)",
        config.supervisors.len()
    );
    let status = state.write_status(
        "rust",
        "running",
        StatusSnapshot {
            config: &config,
            pools: &pools,
            restarts: &restarts,
            draining: &draining,
            desired: &last_desired,
            depths: &last_depths,
            depths_available: &depths_available,
            replicas: &replica_counts,
        },
    )?;
    remote_status::publish(&mut remote_status, &client, &status);
    while running.load(Ordering::SeqCst) {
        match state.command(last_command_nonce.as_deref()) {
            Ok(Some(control)) => {
                last_command_nonce = Some(control.nonce);
                let control_state = match control.command {
                    ControlCommand::Pause => {
                        paused = true;
                        // A paused replica serves nothing; the others take
                        // over its share.
                        if let Some(coordinator) = coordinator.as_mut() {
                            coordinator.leave(&client, &scope_list(&coordinated_scopes));
                        }
                        replica_counts.clear();
                        last_depths.clear();
                        depths_available.clear();
                        // A stopped queue:work process can retain prefetched
                        // leases without renewing their tail. Drain every
                        // worker instead and keep capacity at zero while the
                        // supervisor remains paused.
                        drain_all(
                            &mut pools,
                            &mut restarts,
                            &mut draining,
                            Duration::from_secs(config.shutdown_grace),
                        );
                        "paused"
                    }
                    ControlCommand::Continue => {
                        paused = false;
                        replica_counts =
                            current_replica_counts(coordinator.as_ref(), &coordinated_scopes);
                        "running"
                    }
                    ControlCommand::Terminate => {
                        running.store(false, Ordering::SeqCst);
                        "terminating"
                    }
                };
                match state.write_status(
                    "rust",
                    control_state,
                    StatusSnapshot {
                        config: &config,
                        pools: &pools,
                        restarts: &restarts,
                        draining: &draining,
                        desired: &last_desired,
                        depths: &last_depths,
                        depths_available: &depths_available,
                        replicas: &replica_counts,
                    },
                ) {
                    Ok(status) => remote_status::publish(&mut remote_status, &client, &status),
                    Err(error) => {
                        status_failure = Some(format!("state status write failed: {error}"));
                        running.store(false, Ordering::SeqCst);
                    }
                }
            }
            Ok(None) => {}
            Err(error) => {
                // command() owns both the control document and the pinned
                // generation fence. Treat any failure as infrastructure
                // corruption: continuing could orchestrate workers after the
                // state path was replaced and another master acquired it.
                status_failure = Some(format!("state control read failed: {error}"));
                running.store(false, Ordering::SeqCst);
            }
        }
        if !running.load(Ordering::SeqCst) {
            break;
        }
        // command() verifies the pinned state generation before any reap can
        // remove telemetry or any reconcile can spawn a worker into it.
        let restarted = reap(
            &config,
            &mut pools,
            &mut restarts,
            &mut pending_telemetry_cleanup,
            fork_server.as_ref(),
        );
        reap_draining(&config, &mut draining, &mut pending_telemetry_cleanup);
        if restarted {
            fork_server =
                refresh_fork_server(&config, &running, fork_server.take(), &mut retired_forks);
        }
        close_retired_forks(&mut retired_forks);
        observe_stable_workers(&config, &mut pools, &mut restarts);
        let poll_due = last_poll.elapsed() >= Duration::from_secs(config.poll_interval);
        let event_due = match (&wakes, paused) {
            (Some(wakes), false) => {
                woken.extend(woken_pools(&config, &wakes.take()));
                // A pool at its maximum has nothing to gain from a wake.
                woken.retain(|name| has_room(&config, &pools, name));
                event_due_pools(
                    &config,
                    &pools,
                    &woken,
                    &climbing,
                    &last_evaluated,
                    Instant::now(),
                )
            }
            (Some(wakes), true) => {
                wakes.take();
                woken.clear();
                climbing.clear();
                HashSet::new()
            }
            (None, _) => HashSet::new(),
        };
        if poll_due || !event_due.is_empty() {
            if poll_due {
                if let (Some(coordinator), false) = (coordinator.as_mut(), paused) {
                    coordinator.heartbeat(&client, &scope_list(&coordinated_scopes));
                }
                if !paused {
                    replica_counts =
                        current_replica_counts(coordinator.as_ref(), &coordinated_scopes);
                }
            }
            let mut names: Vec<_> = config.supervisors.keys().cloned().collect();
            names.sort_unstable();
            let mut unavailable_connections = HashSet::new();
            for name in names {
                if !running.load(Ordering::SeqCst) {
                    break;
                }
                if !poll_due && !event_due.contains(&name) {
                    continue;
                }
                let options = &config.supervisors[&name];
                let ready = last_reconcile
                    .get(&name)
                    .map(|at| at.elapsed() >= Duration::from_secs(options.balance_cooldown))
                    .unwrap_or(true);
                if paused {
                    last_depths.remove(&name);
                    depths_available.insert(name.clone(), false);
                    continue;
                }
                // Woken or climbing inside the cooldown: this evaluation may
                // only add workers. Anything else waits for the cooldown.
                let grow_only = !ready && event_due.contains(&name);
                if !ready && !grow_only {
                    continue;
                }
                // Counted as evaluated even when skipped below, so a skipped
                // pool is due again only after a wake interval.
                last_evaluated.insert(name.clone(), Instant::now());
                if !poll_due {
                    // A replica that just resumed has no view of the others
                    // and would size itself alone: keep the wake for the
                    // poll's heartbeat.
                    if let (Some(coordinator), Some(scope)) =
                        (coordinator.as_ref(), coordinated_scopes.get(&name))
                    {
                        if !coordinator.has_view(scope) {
                            continue;
                        }
                    }
                }
                woken.remove(&name);

                // Scan independently of broker health so a short-lived
                // worker's final sample is ingested and reclaimed even when
                // the depth endpoint is unavailable.
                let runtimes =
                    supervisor_runtimes(&config, &name, options, &mut pending_telemetry_cleanup);
                let mut depth_failed = unavailable_connections.contains(&options.connection);
                let mut depths = HashMap::new();
                if !depth_failed {
                    let queen = connection_config(&config, &options.connection)
                        .expect("validated supervisor connection is available");
                    match poll_queue_depths(
                        &options.queues,
                        running.as_ref(),
                        MAX_DEPTH_POLL_CONCURRENCY,
                        |queue| {
                            queue_depth(&client, queen, queue, &options.consumer_group)
                                .map_err(|error| error.to_string())
                        },
                    ) {
                        Ok(ordered_depths) => depths.extend(ordered_depths),
                        Err(DepthPollError::Request { queue, error }) => {
                            eprintln!("[{name}:{queue}] depth failed: {error}");
                            depth_failed = true;
                            unavailable_connections.insert(options.connection.clone());
                        }
                        Err(DepthPollError::Interrupted) => {
                            depth_failed = true;
                        }
                    }
                }
                if !running.load(Ordering::SeqCst) {
                    break;
                }
                if let Err(error) = state.verify_directory() {
                    status_failure = Some(format!("state generation verification failed: {error}"));
                    running.store(false, Ordering::SeqCst);
                    break;
                }
                if depth_failed {
                    climbing.remove(&name);
                    last_depths.remove(&name);
                    depths_available.insert(name.clone(), false);
                }
                if depth_failed && grow_only {
                    continue;
                }
                if depth_failed {
                    scale_guards.entry(name.clone()).or_default().reset();
                    let fallback = last_desired
                        .get(&name)
                        .cloned()
                        .unwrap_or_else(|| fail_open_desired(options));
                    last_desired.insert(name.clone(), fallback.clone());
                    if let Err(error) = reconcile(
                        &Launcher {
                            config: &config,
                            forks: fork_server.as_ref(),
                        },
                        &name,
                        options,
                        fallback,
                        &mut pools,
                        &mut restarts,
                        &mut draining,
                    ) {
                        eprintln!("[{name}] fallback reconcile failed: {error}");
                    }
                    continue;
                }
                last_depths.insert(name.clone(), depths.clone());
                depths_available.insert(name.clone(), true);
                let (replica, replicas) =
                    match (coordinator.as_ref(), coordinated_scopes.get(&name)) {
                        (Some(coordinator), Some(scope)) => coordinator.position(scope),
                        _ => (0, 1),
                    };
                let mut raw = desired_share(options, &depths, &runtimes, replica, replicas);
                let current = current_allocation(&pools, &name, options);
                let active: usize = current.values().sum();
                if grow_only {
                    match grow_only_target(options, &raw, &current) {
                        Some(grown) => raw = grown,
                        None => {
                            // Not an increase: leave it, and the scale-down
                            // window, to the regular cadence.
                            climbing.remove(&name);
                            continue;
                        }
                    }
                }
                let target = stabilize_desired(
                    options,
                    raw,
                    Some(&current),
                    scale_guards.entry(name.clone()).or_default(),
                    Instant::now(),
                );
                last_desired.insert(name.clone(), target.clone());
                if let Err(error) = reconcile(
                    &Launcher {
                        config: &config,
                        forks: fork_server.as_ref(),
                    },
                    &name,
                    options,
                    target,
                    &mut pools,
                    &mut restarts,
                    &mut draining,
                ) {
                    eprintln!("[{name}] reconcile failed: {error}");
                    continue;
                }
                if wakes.is_some() {
                    let started: usize = current_allocation(&pools, &name, options).values().sum();
                    let wanted: usize = last_desired[&name].values().sum();
                    // Held back by the step budget, not by a failing restart:
                    // keep climbing at the wake cadence.
                    if started > active && started < wanted {
                        climbing.insert(name.clone());
                    } else {
                        climbing.remove(&name);
                    }
                }
                last_reconcile.insert(name, Instant::now());
            }
            match state.write_status(
                "rust",
                if paused { "paused" } else { "running" },
                StatusSnapshot {
                    config: &config,
                    pools: &pools,
                    restarts: &restarts,
                    draining: &draining,
                    desired: &last_desired,
                    depths: &last_depths,
                    depths_available: &depths_available,
                    replicas: &replica_counts,
                },
            ) {
                Ok(status) => remote_status::publish(&mut remote_status, &client, &status),
                Err(error) => {
                    status_failure = Some(format!("state status write failed: {error}"));
                    running.store(false, Ordering::SeqCst);
                }
            }
            if poll_due {
                last_poll = Instant::now();
            }
        }
        if status_failure.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(200));
    }

    replica_counts.clear();
    // The fence against new controls, before any worker is touched.
    let terminating = match state.write_status(
        "rust",
        "terminating",
        StatusSnapshot {
            config: &config,
            pools: &pools,
            restarts: &restarts,
            draining: &draining,
            desired: &last_desired,
            depths: &last_depths,
            depths_available: &depths_available,
            replicas: &replica_counts,
        },
    ) {
        Ok(status) => Some(status),
        Err(error) => {
            let error = format!("state status write failed: {error}");
            eprintln!("{error}");
            if status_failure.is_none() {
                status_failure = Some(error);
            }
            None
        }
    };
    // The broker calls wait until every worker has its SIGTERM: on a slow or
    // unreachable broker each may take http_timeout per endpoint, and the
    // platform's stop deadline does not wait.
    shutdown(
        &config,
        &mut pools,
        &mut draining,
        Duration::from_secs(config.shutdown_grace),
        &mut pending_telemetry_cleanup,
        || {
            if let Some(status) = &terminating {
                remote_status::publish(&mut remote_status, &client, status);
            }
            // The other replicas take over this share while it drains.
            if let Some(coordinator) = coordinator.as_mut() {
                coordinator.leave(&client, &scope_list(&coordinated_scopes));
            }
        },
    );
    for server in retired_forks.iter().chain(fork_server.iter()) {
        // Every worker has drained: the server exits, fencing any straggler.
        server.borrow_mut().close(Duration::from_secs(5));
    }
    match state.write_status(
        "rust",
        "stopped",
        StatusSnapshot {
            config: &config,
            pools: &pools,
            restarts: &restarts,
            draining: &draining,
            desired: &last_desired,
            depths: &last_depths,
            depths_available: &depths_available,
            replicas: &replica_counts,
        },
    ) {
        Ok(status) => remote_status::publish(&mut remote_status, &client, &status),
        Err(error) => {
            let error = format!("state status write failed: {error}");
            eprintln!("{error}");
            if status_failure.is_none() {
                status_failure = Some(error);
            }
        }
    }
    match status_failure {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

fn load_config(options: &CliOptions) -> Result<Config, Box<dyn std::error::Error>> {
    let json = if let Some(path) = &options.config {
        read_private_config(path)?
    } else {
        export_artisan_config(options, Duration::from_secs(CONFIG_EXPORT_TIMEOUT_SECONDS))?
    };
    Ok(serde_json::from_str(&json)?)
}

fn export_artisan_config(
    options: &CliOptions,
    timeout: Duration,
) -> Result<String, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut child = Command::new(&options.php)
        .arg(&options.artisan)
        .arg("queen:supervisor-config")
        .arg("--for-engine")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or("could not capture Artisan configuration")?;
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let result = read_text_limited(stdout, "Artisan configuration", MAX_CONFIG_BYTES)
            .map_err(|error| error.to_string());
        let _ = sender.send(result);
    });

    let json = match receiver.recv_timeout(timeout) {
        Ok(Ok(json)) => json,
        Ok(Err(error)) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error.into());
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!(
                "Artisan configuration export exceeded {} seconds",
                timeout.as_secs_f64()
            )
            .into());
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err("Artisan configuration reader stopped unexpectedly".into());
        }
    };
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!(
                "Artisan configuration export exceeded {} seconds",
                timeout.as_secs_f64()
            )
            .into());
        }
        thread::sleep(Duration::from_millis(10));
    };
    if !status.success() {
        return Err(format!("Artisan configuration exporter exited with {status}").into());
    }
    Ok(json)
}

fn parse_args(args: &[String]) -> Result<CliAction, String> {
    if args
        .iter()
        .any(|arg| matches!(arg.as_str(), "-h" | "--help"))
    {
        return Ok(CliAction::Help);
    }
    if args
        .iter()
        .any(|arg| matches!(arg.as_str(), "-V" | "--version"))
    {
        return Ok(CliAction::Version);
    }

    let mut config = None;
    let mut php = None;
    let mut artisan = None;
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        let destination = match flag.as_str() {
            "--config" => &mut config,
            "--php" => &mut php,
            "--artisan" => &mut artisan,
            _ => return Err(format!("unknown argument: {flag}")),
        };
        let value = args
            .get(index + 1)
            .filter(|value| !value.starts_with('-'))
            .ok_or_else(|| format!("{flag} needs a value"))?;
        if destination.replace(value.clone()).is_some() {
            return Err(format!("{flag} may only be specified once"));
        }
        index += 2;
    }
    if config.is_some() && (php.is_some() || artisan.is_some()) {
        return Err("--config cannot be combined with --php or --artisan".into());
    }
    Ok(CliAction::Run(CliOptions {
        config: config.map(PathBuf::from),
        php: php.unwrap_or_else(|| "php".into()),
        artisan: artisan.unwrap_or_else(|| "artisan".into()),
    }))
}

fn read_private_config(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(format!("configuration {} must be a regular file", path.display()).into());
    }
    #[cfg(unix)]
    {
        if metadata.uid() != unsafe { libc::geteuid() } {
            return Err(format!(
                "configuration {} must be owned by the supervisor user",
                path.display()
            )
            .into());
        }
        if metadata.mode() & 0o7777 & !0o600 != 0 {
            return Err(format!(
                "configuration {} must use mode 0600 or stricter",
                path.display()
            )
            .into());
        }
    }
    read_file_limited(file, path, MAX_CONFIG_BYTES)
}

fn read_limited(path: &Path, limit: u64) -> Result<String, Box<dyn std::error::Error>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    let file = options.open(path)?;
    if !file.metadata()?.file_type().is_file() {
        return Err(format!("{} must be a regular file", path.display()).into());
    }
    read_file_limited(file, path, limit)
}

fn read_file_limited(
    file: File,
    path: &Path,
    limit: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    read_text_limited(file, &path.display().to_string(), limit)
}

fn read_text_limited(
    reader: impl Read,
    label: &str,
    limit: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    reader.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(format!("{label} is larger than {limit} bytes").into());
    }
    Ok(String::from_utf8(bytes)?)
}

fn validate_config(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    if config.version != CONFIG_VERSION {
        return Err(format!("unsupported configuration version {}", config.version).into());
    }
    if config.cwd.trim().is_empty()
        || config.php_binary.trim().is_empty()
        || config.artisan.trim().is_empty()
        || config.state_directory.trim().is_empty()
    {
        return Err("cwd, php_binary, artisan and state_directory must not be empty".into());
    }
    let state_directory = Path::new(&config.state_directory);
    if !state_directory.is_absolute()
        || state_directory
            .components()
            .any(|component| component == Component::ParentDir)
        || !state_directory
            .components()
            .any(|component| matches!(component, Component::Normal(_)))
    {
        return Err("state_directory must be an absolute, non-root path without '..'".into());
    }
    if config.poll_interval == 0
        || config.poll_interval > MAX_DURATION_SECONDS
        || config.http_timeout == 0
        || config.http_timeout > MAX_DURATION_SECONDS
        || config.shutdown_grace == 0
        || config.shutdown_grace > MAX_DURATION_SECONDS
        || config.telemetry_ttl == 0
        || config.telemetry_ttl > MAX_DURATION_SECONDS
    {
        return Err("supervisor timing values are outside their supported range".into());
    }
    if config.process_limit == 0 || config.process_limit > MAX_PROCESS_LIMIT {
        return Err(format!("process_limit must be between 1 and {MAX_PROCESS_LIMIT}").into());
    }
    if !(MIN_CONTROL_TTL_SECONDS..=MAX_CONTROL_TTL_SECONDS).contains(&config.control_ttl) {
        return Err(format!(
            "control_ttl must be between {MIN_CONTROL_TTL_SECONDS} and {MAX_CONTROL_TTL_SECONDS}"
        )
        .into());
    }
    if config.heartbeat_timeout == 0 || config.heartbeat_timeout > MAX_CONTROL_TTL_SECONDS {
        return Err(
            format!("heartbeat_timeout must be between 1 and {MAX_CONTROL_TTL_SECONDS}").into(),
        );
    }
    if config.supervisors.is_empty() {
        return Err("configuration has no supervisors".into());
    }
    validate_connection("queen", &config.queen)?;
    for (name, connection) in &config.connections {
        validate_identifier(name, "connection name")?;
        validate_connection(name, connection)?;
    }
    let mut total_max = 0usize;
    let mut total_process_budget = 0usize;
    let mut total_pools = 0usize;
    for (name, options) in &config.supervisors {
        validate_identifier(name, "supervisor name")?;
        validate_identifier(
            &options.connection,
            &format!("supervisor [{name}] connection"),
        )?;
        if options.connection != "queen" && !config.connections.contains_key(&options.connection) {
            return Err(format!(
                "supervisor [{name}] connection [{}] is missing from the resolved v2 contract",
                options.connection
            )
            .into());
        }
        validate_identifier(
            &options.consumer_group,
            &format!("supervisor [{name}] consumer_group"),
        )?;
        if options.queues.is_empty() || options.queues.len() > MAX_QUEUES_PER_SUPERVISOR {
            return Err(format!(
                "supervisor [{name}] must have between 1 and {MAX_QUEUES_PER_SUPERVISOR} queues"
            )
            .into());
        }
        total_pools = total_pools
            .checked_add(options.queues.len())
            .ok_or("sum of supervisor queues overflowed")?;
        let mut queues = HashSet::new();
        for queue in &options.queues {
            if queue.is_empty()
                || queue.len() > MAX_QUEUE_BYTES
                || queue.contains(',')
                || queue.chars().any(char::is_control)
                || !queues.insert(queue)
            {
                return Err(format!("supervisor [{name}] has invalid or duplicate queues").into());
            }
        }
        if !matches!(options.balance.as_str(), "auto" | "simple" | "off") {
            return Err(format!("supervisor [{name}] has invalid balance").into());
        }
        if !matches!(options.strategy.as_str(), "size" | "time") {
            return Err(format!("supervisor [{name}] has invalid strategy").into());
        }
        if options.min_processes_per_queue > 0
            && (options.balance != "auto"
                || options.min_processes_per_queue > options.max_processes / options.queues.len())
        {
            return Err(format!(
                "supervisor [{name}] min_processes_per_queue needs balance auto and must fit max_processes for every queue"
            )
            .into());
        }
        if (options.balance == "auto" && options.max_processes < options.queues.len())
            || (options.balance == "simple" && options.processes < options.queues.len())
        {
            return Err(format!(
                "supervisor [{name}] process bounds cannot cover every configured queue"
            )
            .into());
        }
        if options.max_processes == 0
            || options.min_processes > options.max_processes
            || options.processes < options.min_processes
            || options.processes > options.max_processes
            || options.max_processes > config.process_limit
        {
            return Err(format!("supervisor [{name}] has invalid process bounds").into());
        }
        if options.target_jobs_per_process == 0
            || !options.target_clear_seconds.is_finite()
            || !(MIN_SCALING_SECONDS..=MAX_DURATION_SECONDS as f64)
                .contains(&options.target_clear_seconds)
            || !options.default_runtime_seconds.is_finite()
            || !(MIN_SCALING_SECONDS..=MAX_DURATION_SECONDS as f64)
                .contains(&options.default_runtime_seconds)
        {
            return Err(format!("supervisor [{name}] has invalid scaling targets").into());
        }
        if options.balance_cooldown == 0
            || options.balance_max_shift == 0
            || options.balance_max_shift > options.max_processes
            || options.timeout == 0
            || options.timeout > MAX_DURATION_SECONDS
            || options.retry_after <= options.timeout
            || options.retry_after > MAX_DURATION_SECONDS
            || options.memory == 0
            || options.restart_backoff > options.restart_backoff_max
            || options.restart_backoff_max > MAX_DURATION_SECONDS
            || options.stable_after == 0
            || options.stable_after > MAX_DURATION_SECONDS
            || options.scale_down_delay > MAX_DURATION_SECONDS
        {
            return Err(format!("supervisor [{name}] has invalid runtime policy").into());
        }
        if config.shutdown_grace <= options.timeout {
            return Err(format!("shutdown_grace must exceed timeout for [{name}]").into());
        }
        total_max = total_max
            .checked_add(options.max_processes)
            .ok_or("sum of supervisor max_processes overflowed")?;
        let reserved = options
            .max_processes
            .checked_mul(process_cost(options))
            .ok_or("supervisor child-process reservation overflowed")?;
        if reserved > config.process_limit {
            return Err(format!(
                "supervisor [{name}] reserves {reserved} child processes and exceeds process_limit [{}]",
                config.process_limit
            )
            .into());
        }
        total_process_budget = total_process_budget
            .checked_add(reserved)
            .ok_or("sum of supervisor child-process reservations overflowed")?;
    }
    if total_max > config.process_limit {
        return Err("sum of supervisor max_processes exceeds process_limit".into());
    }
    if total_process_budget > config.process_limit {
        return Err(format!(
            "supervisors reserve {total_process_budget} child processes and exceed process_limit [{}]",
            config.process_limit
        )
        .into());
    }
    if total_pools > MAX_STATUS_POOLS {
        return Err(format!(
            "configuration defines {total_pools} pools; at most {MAX_STATUS_POOLS} are supported"
        )
        .into());
    }
    if let Some(remote_status) = &config.remote_status {
        remote_status.validate(config.heartbeat_timeout)?;
    }
    let loop_budget = control_loop_budget(config)?;
    if let Some(coordination) = &config.coordination {
        coordination.validate(loop_budget)?;
    }
    if let Some(event_driven) = &config.event_driven {
        event_driven.validate()?;
    }
    if config.control_ttl <= loop_budget {
        return Err(format!(
            "control_ttl must exceed the conservative control-loop budget of {loop_budget} seconds"
        )
        .into());
    }
    if config.heartbeat_timeout <= loop_budget {
        return Err(format!(
            "heartbeat_timeout must exceed the conservative control-loop budget of {loop_budget} seconds"
        )
        .into());
    }
    Ok(())
}

fn validate_identifier(value: &str, label: &str) -> Result<(), Box<dyn std::error::Error>> {
    if value.is_empty() || value.len() > MAX_IDENTIFIER_BYTES || value.chars().any(char::is_control)
    {
        return Err(format!(
            "{label} must be non-empty, at most {MAX_IDENTIFIER_BYTES} bytes and contain no control characters"
        )
        .into());
    }
    Ok(())
}

fn validate_connection(
    name: &str,
    connection: &QueenConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let endpoints = connection_endpoints(connection);
    if endpoints.is_empty() {
        return Err(format!("connection [{name}] has no Queen URL").into());
    }
    for endpoint in endpoints {
        let url = reqwest::Url::parse(endpoint)?;
        if !matches!(url.scheme(), "http" | "https") || url.cannot_be_a_base() {
            return Err(format!("connection [{name}] has an invalid Queen URL").into());
        }
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(format!(
                "connection [{name}] Queen URLs must be credential-free base URLs"
            )
            .into());
        }
    }
    if connection.bearer_token.as_deref() == Some("") {
        return Err(format!("connection [{name}] has an empty bearer token").into());
    }
    for (header, value) in &connection.headers {
        header.parse::<reqwest::header::HeaderName>()?;
        value.parse::<reqwest::header::HeaderValue>()?;
    }
    Ok(())
}

fn connection_endpoints(connection: &QueenConfig) -> Vec<&str> {
    if connection.urls.is_empty() {
        (!connection.url.is_empty())
            .then_some(connection.url.as_str())
            .into_iter()
            .collect()
    } else {
        connection.urls.iter().map(String::as_str).collect()
    }
}

fn connection_config<'a>(config: &'a Config, connection: &str) -> Option<&'a QueenConfig> {
    config
        .connections
        .get(connection)
        .or_else(|| (connection == "queen").then_some(&config.queen))
}

fn control_loop_budget(config: &Config) -> Result<u64, Box<dyn std::error::Error>> {
    let mut budget = config
        .poll_interval
        .checked_add(CONTROL_LOOP_MARGIN_SECONDS)
        .ok_or("control-loop timing budget overflowed")?;

    for options in config.supervisors.values() {
        let queue_count = u64::try_from(options.queues.len())?;
        let depth_batches = queue_count
            .checked_add(u64::try_from(MAX_DEPTH_POLL_CONCURRENCY - 1)?)
            .ok_or("control-loop depth batch count overflowed")?
            / u64::try_from(MAX_DEPTH_POLL_CONCURRENCY)?;
        let connection = connection_config(config, &options.connection)
            .ok_or("control-loop supervisor connection is unavailable")?;
        let endpoint_count = u64::try_from(connection_endpoints(connection).len())?;
        if endpoint_count == 0 {
            return Err("control-loop supervisor connection has no endpoints".into());
        }
        let depth_budget = depth_batches
            .checked_mul(endpoint_count)
            .and_then(|value| value.checked_mul(config.http_timeout))
            .ok_or("control-loop depth timing budget overflowed")?;
        let process_budget = u64::try_from(options.max_processes)?
            .checked_mul(PROCESS_START_BUDGET_SECONDS)
            .ok_or("control-loop process timing budget overflowed")?;

        budget = budget
            .checked_add(depth_budget)
            .and_then(|value| value.checked_add(process_budget))
            .ok_or("control-loop timing budget overflowed")?;
        if options.strategy == "time" && options.balance != "simple" {
            budget = budget
                .checked_add(TELEMETRY_SCAN_BUDGET_SECONDS)
                .ok_or("control-loop telemetry timing budget overflowed")?;
        }
    }
    if let Some(remote_status) = &config.remote_status {
        budget = budget
            .checked_add(remote_status.publish_budget(config.http_timeout)?)
            .ok_or("control-loop remote status timing budget overflowed")?;
    }
    if let Some(coordination) = &config.coordination {
        let scopes = scope_list(&coordinated_scopes(config)).len();
        budget = budget
            .checked_add(coordination.heartbeat_budget(scopes, config.http_timeout)?)
            .ok_or("control-loop coordination timing budget overflowed")?;
    }

    Ok(budget)
}

/// The coordination scope of every autoscaling pool; fixed pools run their
/// `processes` on every replica and do not coordinate.
fn coordinated_scopes(config: &Config) -> HashMap<String, String> {
    config
        .supervisors
        .iter()
        .filter(|(_, options)| options.balance != "simple")
        .map(|(name, options)| {
            let endpoints = connection_config(config, &options.connection)
                .map(connection_endpoints)
                .unwrap_or_default();
            (
                name.clone(),
                coordination::scope(&endpoints, &options.consumer_group, &options.queues),
            )
        })
        .collect()
}

/// Live replicas each coordinated pool currently shares its target with; one
/// until the first listing, as in the PHP engine.
fn current_replica_counts(
    coordinator: Option<&Coordinator<'_>>,
    scopes: &HashMap<String, String>,
) -> HashMap<String, usize> {
    coordinator
        .map(|coordinator| {
            scopes
                .iter()
                .map(|(name, scope)| (name.clone(), coordinator.position(scope).1))
                .collect()
        })
        .unwrap_or_default()
}

fn scope_list(scopes: &HashMap<String, String>) -> Vec<String> {
    let mut list: Vec<String> = scopes.values().cloned().collect();
    list.sort_unstable();
    list.dedup();
    list
}

fn status_configuration(config: &Config) -> serde_json::Value {
    let mut names: Vec<_> = config.supervisors.keys().collect();
    names.sort_unstable();
    let supervisors = names
        .into_iter()
        .map(|name| {
            let options = &config.supervisors[name];
            serde_json::json!({
                "name": name,
                "connection": &options.connection,
                "consumer_group": &options.consumer_group,
                "queues": &options.queues,
                "balance": &options.balance,
                "strategy": &options.strategy,
                "processes": options.processes,
                "min_processes": options.min_processes,
                "max_processes": options.max_processes,
                "timeout": options.timeout,
                "retry_after": options.retry_after,
                "lease_renewal": options.lease_renewal,
                "process_cost_per_worker": process_cost(options),
                "tries": options.tries,
                "memory": options.memory,
            })
        })
        .collect::<Vec<_>>();

    serde_json::json!({
        "poll_interval": config.poll_interval,
        "http_timeout": config.http_timeout,
        "control_ttl": config.control_ttl,
        "heartbeat_timeout": config.heartbeat_timeout,
        "shutdown_grace": config.shutdown_grace,
        "telemetry_ttl": config.telemetry_ttl,
        "process_limit": config.process_limit,
        "supervisors": supervisors,
    })
}

fn state_directory_paths(directory: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    if !directory.is_absolute() {
        return Err("state_directory must be absolute".into());
    }
    let mut paths = vec![PathBuf::from(std::path::MAIN_SEPARATOR.to_string())];
    let mut current = PathBuf::from(std::path::MAIN_SEPARATOR.to_string());
    for component in directory.components() {
        match component {
            Component::RootDir | Component::CurDir | Component::Prefix(_) => {}
            Component::Normal(component) => {
                current.push(component);
                paths.push(current.clone());
            }
            Component::ParentDir => {
                return Err("state_directory must not contain '..'".into());
            }
        }
    }
    if paths.len() == 1 {
        return Err("state_directory must not be a filesystem root".into());
    }
    Ok(paths)
}

#[cfg(unix)]
fn trusted_directory_component(
    path: &Path,
    metadata: &fs::Metadata,
    state_leaf: bool,
    parent_was_sticky: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    let effective_uid = unsafe { libc::geteuid() };
    let owner = metadata.uid();
    if parent_was_sticky && owner != 0 && owner != effective_uid {
        return Err(format!(
            "state_directory child {} below a sticky directory must be owned by the supervisor user",
            path.display()
        )
        .into());
    }
    // A foreign owner can chmod a nominally read-only directory and then
    // rename its children. Root and the effective supervisor user are the
    // only trusted owners in the state path.
    if owner != 0 && owner != effective_uid {
        return Err(format!(
            "state_directory ancestor {} must be owned by root or the supervisor user",
            path.display()
        )
        .into());
    }
    let mode = metadata.mode();
    // `MetadataExt::mode()` is always `u32`, while libc exposes `S_ISVTX`
    // as different integer types across Unix targets (for example, `u16` on
    // macOS and `u32` on Linux). The POSIX sticky-mode bit itself is 0o1000.
    let sticky = mode & 0o1000 != 0;
    if !state_leaf && mode & 0o022 != 0 && !sticky {
        return Err(format!(
            "state_directory ancestor {} must not be group/world-writable unless it is a trusted sticky directory",
            path.display()
        )
        .into());
    }
    Ok(sticky)
}

#[cfg(not(unix))]
fn trusted_directory_component(
    _path: &Path,
    _metadata: &fs::Metadata,
    _state_leaf: bool,
    _parent_was_sticky: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    Ok(false)
}

fn normalize_absolute_state_path(path: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if !path.is_absolute() {
        return Err("state_directory symbolic-link target must resolve to an absolute path".into());
    }
    let mut normalized = PathBuf::from(std::path::MAIN_SEPARATOR.to_string());
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir | Component::Prefix(_) => {}
            Component::Normal(component) => normalized.push(component),
            Component::ParentDir => {
                normalized.pop();
            }
        }
    }
    Ok(normalized)
}

fn validate_requested_state_path(directory: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut visited_links = HashSet::new();
    validate_requested_state_path_inner(directory, &mut visited_links, true).map(|_| ())
}

fn validate_requested_state_path_inner(
    directory: &Path,
    visited_links: &mut HashSet<PathBuf>,
    state_leaf: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    let paths = state_directory_paths(directory)?;
    let last = paths.len() - 1;
    let mut parent_was_sticky = false;
    for (index, path) in paths.iter().enumerate() {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound && index == last && state_leaf =>
            {
                return Ok(parent_was_sticky)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(format!(
                    "every parent of state_directory must already exist: {}",
                    path.display()
                )
                .into())
            }
            Err(error) => return Err(error.into()),
        };
        if metadata.file_type().is_symlink() {
            if index == last && state_leaf {
                return Err("state_directory must not be a symbolic link".into());
            }
            #[cfg(unix)]
            if parent_was_sticky
                && metadata.uid() != 0
                && metadata.uid() != unsafe { libc::geteuid() }
            {
                return Err(format!(
                    "state_directory child {} below a sticky directory must be owned by the supervisor user",
                    path.display()
                )
                .into());
            }
            if !visited_links.insert(path.clone()) {
                return Err(format!(
                    "state_directory ancestor {} contains a symbolic-link loop",
                    path.display()
                )
                .into());
            }
            let link = fs::read_link(path)?;
            let target = if link.is_absolute() {
                link
            } else {
                path.parent()
                    .ok_or("state_directory symlink has no parent")?
                    .join(link)
            };
            let target = normalize_absolute_state_path(&target)?;
            parent_was_sticky = validate_requested_state_path_inner(&target, visited_links, false)?;
            visited_links.remove(path);
            continue;
        }
        if !metadata.file_type().is_dir() {
            return Err(format!(
                "state_directory ancestor {} must be a directory",
                path.display()
            )
            .into());
        }
        parent_was_sticky = trusted_directory_component(
            path,
            &metadata,
            state_leaf && index == last,
            parent_was_sticky,
        )?;
    }
    Ok(parent_was_sticky)
}

fn canonical_state_directory(directory: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    validate_requested_state_path(directory)?;
    let canonical = match fs::symlink_metadata(directory) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
                return Err("state_directory must be a real directory".into());
            }
            fs::canonicalize(directory)?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = directory
                .parent()
                .ok_or("state_directory must have an existing parent")?;
            let name = directory
                .file_name()
                .ok_or("state_directory must have a final component")?;
            fs::canonicalize(parent)?.join(name)
        }
        Err(error) => return Err(error.into()),
    };
    validate_canonical_state_path(&canonical)?;
    Ok(canonical)
}

fn validate_canonical_state_path(directory: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let paths = state_directory_paths(directory)?;
    let last = paths.len() - 1;
    let mut parent_was_sticky = false;
    for (index, path) in paths.iter().enumerate() {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && index == last => break,
            Err(error) => return Err(error.into()),
        };
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err(format!(
                "canonical state_directory ancestor {} must be a real directory",
                path.display()
            )
            .into());
        }
        parent_was_sticky =
            trusted_directory_component(path, &metadata, index == last, parent_was_sticky)?;
    }
    Ok(())
}

fn assert_same_state_directory(
    directory: &Path,
    expected: &fs::Metadata,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_canonical_state_path(directory)?;
    let current = fs::symlink_metadata(directory)
        .map_err(|_| "state_directory changed after generation acquisition")?;
    if !current.file_type().is_dir() || current.file_type().is_symlink() {
        return Err("state_directory changed after generation acquisition".into());
    }
    #[cfg(unix)]
    if current.uid() != unsafe { libc::geteuid() }
        || current.mode() & 0o7777 != 0o700
        || current.dev() != expected.dev()
        || current.ino() != expected.ino()
    {
        return Err("state_directory changed after generation acquisition".into());
    }
    #[cfg(not(unix))]
    if fs::canonicalize(directory)? != directory {
        return Err("state_directory changed after generation acquisition".into());
    }
    Ok(())
}

impl State {
    fn acquire(directory: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let directory = canonical_state_directory(Path::new(directory))?;
        let created = match fs::symlink_metadata(&directory) {
            Ok(_) => false,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // All ancestors were validated above. Never recursively create
                // a hierarchy across an untrusted or misspelled component.
                fs::create_dir(&directory)?;
                true
            }
            Err(error) => return Err(error.into()),
        };
        if created {
            #[cfg(unix)]
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
        }
        validate_canonical_state_path(&directory)?;
        let metadata = fs::symlink_metadata(&directory)?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err("state_directory must be a real directory".into());
        }
        #[cfg(unix)]
        {
            if metadata.uid() != unsafe { libc::geteuid() } {
                return Err("state_directory must be owned by the supervisor user".into());
            }
            if metadata.mode() & 0o7777 != 0o700 {
                return Err("an existing state_directory must use mode 0700".into());
            }
        }

        // Generation changes and dashboard control requests share this lock.
        // Holding it through supervisor.lock acquisition and owner publication
        // prevents a command from being fenced against a half-published owner.
        let _generation_lock = control_lock_for(&directory)?;
        assert_same_state_directory(&directory, &metadata)?;
        let path = directory.join("supervisor.lock");
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        let mut lock = options.open(&path)?;
        #[cfg(unix)]
        {
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
            let opened = lock.metadata()?;
            let current = fs::symlink_metadata(&path)?;
            if !opened.file_type().is_file()
                || !current.file_type().is_file()
                || opened.uid() != unsafe { libc::geteuid() }
                || opened.mode() & 0o777 != 0o600
                || opened.dev() != current.dev()
                || opened.ino() != current.ino()
            {
                return Err("supervisor lock must be a private owned regular file".into());
            }
        }
        #[cfg(unix)]
        unsafe {
            if libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) != 0 {
                return Err("another Queen supervisor owns the state directory".into());
            }
        }
        assert_same_state_directory(&directory, &metadata)?;
        let instance_id = new_instance_id();
        let started_at_epoch = now_epoch();
        lock.set_len(0)?;
        write!(
            lock,
            "{}",
            serde_json::to_string(&serde_json::json!({
                "pid": std::process::id(),
                "instance_id": &instance_id,
                "started_at": iso8601_from_epoch(started_at_epoch),
                "started_at_epoch": started_at_epoch,
            }))?
        )?;
        lock.flush()?;
        assert_same_state_directory(&directory, &metadata)?;
        Ok(Self {
            directory,
            instance_id,
            hostname: hostname(),
            _lock: lock,
            #[cfg(unix)]
            directory_device: metadata.dev(),
            #[cfg(unix)]
            directory_inode: metadata.ino(),
        })
    }

    fn command(
        &self,
        last_nonce: Option<&str>,
    ) -> Result<Option<Control>, Box<dyn std::error::Error>> {
        let _control_lock = self.control_lock()?;
        let result = (|| -> Result<Option<Control>, Box<dyn std::error::Error>> {
            let path = self.directory.join("control.json");
            let metadata = match fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            if !metadata.file_type().is_file()
                || metadata.len() == 0
                || metadata.len() > MAX_CONTROL_BYTES
            {
                let _ = fs::remove_file(&path);
                return Err("control command must be a small regular file".into());
            }
            #[cfg(unix)]
            if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o777 != 0o600 {
                let _ = fs::remove_file(&path);
                return Err("control command must be a private owned regular file".into());
            }
            let control: Control = match read_limited(&path, MAX_CONTROL_BYTES)
                .and_then(|body| Ok(serde_json::from_str(&body)?))
            {
                Ok(control) => control,
                Err(error) => {
                    let _ = fs::remove_file(&path);
                    return Err(error);
                }
            };
            if control.nonce.is_empty()
                || control.nonce.len() > 128
                || control.nonce.chars().any(char::is_control)
                || control.instance_id.is_empty()
                || control.instance_id.len() > 128
                || control.instance_id.chars().any(char::is_control)
            {
                let _ = fs::remove_file(&path);
                return Err("control nonce is invalid".into());
            }
            let now = now_epoch();
            if control.requested_at_epoch > now.saturating_add(CONTROL_CLOCK_SKEW_SECONDS)
                || control.expires_at_epoch < control.requested_at_epoch
                || control.expires_at_epoch < now
            {
                let _ = fs::remove_file(&path);
                return Err("control command is expired or has an invalid timestamp".into());
            }
            if Some(control.nonce.as_str()) == last_nonce || control.instance_id != self.instance_id
            {
                let _ = fs::remove_file(&path);
                return Ok(None);
            }
            fs::remove_file(path)?;
            Ok(Some(control))
        })();
        self.verify_directory()?;
        result
    }

    fn control_lock(&self) -> Result<File, Box<dyn std::error::Error>> {
        self.verify_directory()?;
        let lock = control_lock_for(&self.directory)?;
        self.verify_directory()?;
        Ok(lock)
    }

    fn verify_directory(&self) -> Result<(), Box<dyn std::error::Error>> {
        validate_canonical_state_path(&self.directory)?;
        let metadata = fs::symlink_metadata(&self.directory)
            .map_err(|_| "state_directory changed after generation acquisition")?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err("state_directory changed after generation acquisition".into());
        }
        #[cfg(unix)]
        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o7777 != 0o700
            || metadata.dev() != self.directory_device
            || metadata.ino() != self.directory_inode
        {
            return Err("state_directory changed after generation acquisition".into());
        }
        #[cfg(not(unix))]
        if fs::canonicalize(&self.directory)? != self.directory {
            return Err("state_directory changed after generation acquisition".into());
        }
        Ok(())
    }

    fn write_status(
        &self,
        engine: &str,
        state: &str,
        snapshot: StatusSnapshot<'_>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        self.verify_directory()?;
        let StatusSnapshot {
            config,
            pools,
            restarts,
            draining,
            desired: last_desired,
            depths: last_depths,
            depths_available,
            replicas,
        } = snapshot;
        let now = Instant::now();
        let mut draining_by_pool: HashMap<PoolKey, (usize, Vec<u32>)> = HashMap::new();
        for worker in draining {
            let entry = draining_by_pool
                .entry(worker.pool.clone())
                .or_insert_with(|| (0, Vec::new()));
            entry.0 += 1;
            entry.1.push(worker.worker.child.id());
        }

        let mut entries = serde_json::Map::new();
        let mut active_workers = 0usize;
        let draining_workers = draining.len();
        let mut renewal_helpers_reserved = 0usize;
        let mut all_pools_ready = true;
        let mut all_capacity_satisfied = true;
        let mut pool_count = 0usize;
        let mut names: Vec<_> = config.supervisors.keys().collect();
        names.sort_unstable();
        let mut pool_status = Vec::new();
        for supervisor in names {
            let options = &config.supervisors[supervisor];
            let mut supervisor_entries = serde_json::Map::new();
            for queue in &options.queues {
                let key = (supervisor.clone(), queue.clone());
                let children = pools.get(&key);
                let running = children.map(Vec::len).unwrap_or(0);
                let restart = restarts.get(&key);
                let restart_state = restart.map(RestartGuard::state_name).unwrap_or("closed");
                let restart_failures = restart.map(|guard| guard.consecutive_failures).unwrap_or(0);
                let draining_entry = draining_by_pool.get(&key);
                let depth_available = depths_available.get(supervisor).copied().unwrap_or(false);
                let depth = depth_available
                    .then(|| {
                        last_depths
                            .get(supervisor)
                            .and_then(|depths| depths.get(queue))
                            .copied()
                    })
                    .flatten();
                let desired = if matches!(state, "terminating" | "stopped") {
                    0
                } else {
                    last_desired
                        .get(supervisor)
                        .and_then(|queues| queues.get(queue))
                        .copied()
                        .unwrap_or(running)
                };
                let worker_process_cost = process_cost(options);
                let draining_count = draining_entry.map(|entry| entry.0).unwrap_or(0);
                active_workers = active_workers.saturating_add(running);
                if worker_process_cost > 1 {
                    renewal_helpers_reserved = renewal_helpers_reserved
                        .saturating_add(running.saturating_add(draining_count));
                }
                let healthy = restart_state == "closed" && restart_failures == 0;
                let ready = state == "running"
                    && depth_available
                    && depth.is_some()
                    && (desired == 0 || running > 0);
                all_pools_ready &= ready;
                all_capacity_satisfied &= running >= desired;
                pool_count += 1;
                let pids = children
                    .map(|workers| {
                        workers
                            .iter()
                            .map(|worker| worker.child.id())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                pool_status.push(serde_json::json!({
                    "supervisor": supervisor,
                    "queue": queue,
                    "desired": desired,
                    "running": running,
                    "draining": draining_count,
                    "pids": &pids,
                    "draining_pids": draining_entry.map(|entry| entry.1.clone()).unwrap_or_default(),
                    "restart_state": restart_state,
                    "restart_failures": restart_failures,
                    "restart_in_seconds": restart.and_then(|guard| guard.retry_in_seconds(now)),
                    "healthy": healthy,
                    "ready": ready,
                    "capacity_satisfied": running >= desired,
                    "depth": depth,
                    "depth_available": depth_available,
                    "process_cost_per_worker": worker_process_cost,
                    "reserved_processes": running.saturating_add(draining_count).saturating_mul(worker_process_cost),
                    "renewal_helpers_reserved": running.saturating_add(draining_count).saturating_mul(worker_process_cost.saturating_sub(1)),
                    // Live replicas sharing this pool's target; null when it is not coordinated.
                    "replicas": if state == "running" { replicas.get(supervisor).copied() } else { None },
                }));
                supervisor_entries.insert(
                    queue.clone(),
                    serde_json::json!({
                        "processes": running,
                        "pids": pids,
                        "desired": desired,
                        "draining": draining_count,
                        "restart_state": restart_state,
                        "restart_failures": restart_failures,
                        "restart_in_seconds": restart.and_then(|guard| guard.retry_in_seconds(now)),
                        "ready": ready,
                        "capacity_satisfied": running >= desired,
                        "depth": depth,
                        "depth_available": depth_available,
                        "process_cost_per_worker": worker_process_cost,
                        "reserved_processes": running.saturating_add(draining_count).saturating_mul(worker_process_cost),
                    }),
                );
            }
            entries.insert(
                supervisor.clone(),
                serde_json::Value::Object(supervisor_entries),
            );
        }
        let process_limit = config.process_limit;
        let used_process_budget = active_workers
            .saturating_add(draining_workers)
            .saturating_add(renewal_helpers_reserved);
        let ready = state == "running" && pool_count > 0 && all_pools_ready;
        let capacity_satisfied = pool_count > 0 && all_capacity_satisfied;
        let updated_at_epoch = now_epoch();
        let status = serde_json::json!({
            "schema": STATUS_SCHEMA,
            "engine": engine,
            "state": state,
            "pid": std::process::id(),
            "hostname": &self.hostname,
            "instance_id": &self.instance_id,
            "updated_at": iso8601_from_epoch(updated_at_epoch),
            "updated_at_epoch": updated_at_epoch,
            "paused": state == "paused",
            "stopping": state == "terminating",
            "ready": ready,
            "capacity_satisfied": capacity_satisfied,
            "draining": draining.len(),
            "process_budget": {
                "limit": process_limit,
                "used": used_process_budget,
                "available": process_limit.saturating_sub(used_process_budget),
                "active_worker_processes": active_workers,
                "draining_worker_processes": draining_workers,
                "renewal_helpers_reserved": renewal_helpers_reserved,
            },
            "pools": entries,
            "pool_status": pool_status,
            "configuration": status_configuration(config),
        });
        if serde_json::to_vec(&status)?.len() as u64 > MAX_STATUS_BYTES {
            return Err(format!("supervisor status exceeds {MAX_STATUS_BYTES} bytes").into());
        }
        atomic_json(&self.directory.join("status.json"), &status)?;
        self.verify_directory()?;
        Ok(status)
    }
}

fn control_lock_for(directory: &Path) -> Result<File, Box<dyn std::error::Error>> {
    let path = directory.join("control.lock");
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    options
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    let lock = options.open(&path)?;
    let metadata = lock.metadata()?;
    if !metadata.file_type().is_file() {
        return Err("control lock must be a regular file".into());
    }
    #[cfg(unix)]
    {
        if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o777 != 0o600 {
            return Err("control lock must be a private owned regular file".into());
        }
        unsafe {
            if libc::flock(lock.as_raw_fd(), libc::LOCK_EX) != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
    }

    Ok(lock)
}

fn atomic_json(path: &Path, value: &serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("state path has no UTF-8 filename")?;
    let temporary = path.with_file_name(format!(".{filename}.{}.tmp", new_instance_id()));
    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        let mut file = options.open(&temporary)?;
        file.write_all(&serde_json::to_vec(value)?)?;
        file.flush()?;
        #[cfg(unix)]
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn iso8601_from_epoch(timestamp: u64) -> String {
    let days = (timestamp / 86_400) as i64;
    let seconds = timestamp % 86_400;
    let hour = seconds / 3_600;
    let minute = seconds % 3_600 / 60;
    let second = seconds % 60;

    // Gregorian civil date conversion for days since 1970-01-01.
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Tells apart the hosts or pods that publish to one remote status key.
fn hostname() -> Option<String> {
    let mut buffer = [0 as libc::c_char; 256];
    // SAFETY: the pointer and length describe `buffer`, which outlives the call.
    if unsafe { libc::gethostname(buffer.as_mut_ptr(), buffer.len()) } != 0 {
        return None;
    }
    // POSIX leaves a truncated name unterminated; the scan stops at the buffer end.
    let name: Vec<u8> = buffer
        .iter()
        .take_while(|&&byte| byte != 0)
        // c_char is i8 on x86_64 and u8 on aarch64 Linux.
        .map(|&byte| u8::from_ne_bytes(byte.to_ne_bytes()))
        .collect();
    String::from_utf8(name).ok().filter(|name| !name.is_empty())
}

fn new_instance_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{nanos:032x}{:08x}", std::process::id())
}

#[derive(Debug, PartialEq, Eq)]
enum DepthPollError {
    Interrupted,
    Request { queue: String, error: String },
}

fn poll_queue_depths<F>(
    queues: &[String],
    running: &AtomicBool,
    max_concurrency: usize,
    fetch: F,
) -> Result<Vec<(String, usize)>, DepthPollError>
where
    F: Fn(&str) -> Result<usize, String> + Sync,
{
    if !running.load(Ordering::SeqCst) {
        return Err(DepthPollError::Interrupted);
    }

    let worker_count = queues.len().min(max_concurrency);
    if worker_count == 0 {
        return Ok(Vec::new());
    }

    let next = AtomicUsize::new(0);
    let cancelled = AtomicBool::new(false);
    let results = Mutex::new(
        (0..queues.len())
            .map(|_| None)
            .collect::<Vec<Option<Result<usize, String>>>>(),
    );

    thread::scope(|scope| {
        for _ in 0..worker_count {
            let next = &next;
            let cancelled = &cancelled;
            let results = &results;
            let fetch = &fetch;
            scope.spawn(move || loop {
                if !running.load(Ordering::SeqCst) || cancelled.load(Ordering::SeqCst) {
                    break;
                }
                let index = next.fetch_add(1, Ordering::SeqCst);
                if index >= queues.len() {
                    break;
                }
                if !running.load(Ordering::SeqCst) || cancelled.load(Ordering::SeqCst) {
                    break;
                }

                let result = fetch(&queues[index]);
                let failed = result.is_err();
                results.lock().expect("depth result lock poisoned")[index] = Some(result);
                if failed {
                    cancelled.store(true, Ordering::SeqCst);
                    break;
                }
            });
        }
    });

    let mut results = results.into_inner().expect("depth result lock poisoned");
    if !running.load(Ordering::SeqCst) {
        return Err(DepthPollError::Interrupted);
    }
    if let Some((index, error)) = results.iter().enumerate().find_map(|(index, result)| {
        result
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .map(|error| (index, error.clone()))
    }) {
        return Err(DepthPollError::Request {
            queue: queues[index].clone(),
            error,
        });
    }

    queues
        .iter()
        .enumerate()
        .map(|(index, queue)| match results[index].take() {
            Some(Ok(depth)) => Ok((queue.clone(), depth)),
            _ => Err(DepthPollError::Interrupted),
        })
        .collect()
}

fn queue_depth(
    client: &reqwest::blocking::Client,
    queen: &QueenConfig,
    queue: &str,
    group: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut last_error = None;
    for endpoint in connection_endpoints(queen) {
        let result = (|| -> Result<usize, Box<dyn std::error::Error>> {
            let mut url = reqwest::Url::parse(endpoint)?;
            url.path_segments_mut()
                .map_err(|_| "Queen URL cannot be a base URL")?
                .extend(["api", "v1", "resources", "queues", queue, "depth"]);
            url.query_pairs_mut().append_pair("group", group);
            let mut request = client.get(url);
            for (name, value) in &queen.headers {
                request = request.header(name, value);
            }
            if let Some(token) = &queen.bearer_token {
                request = request.bearer_auth(token);
            }
            let response = request.send()?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                let body = read_response_limited(response, MAX_CONFIG_BYTES)?;
                if serde_json::from_slice::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|value| {
                        value
                            .get("code")
                            .and_then(|code| code.as_str())
                            .map(str::to_owned)
                    })
                    .as_deref()
                    == Some("no_such_route")
                {
                    return Err("Queen broker does not expose the depth endpoint".into());
                }
                return Ok(0);
            }
            let response = response.error_for_status()?;
            let body: DepthResponse =
                serde_json::from_slice(&read_response_limited(response, MAX_CONFIG_BYTES)?)?;
            body.effective_pending
                .or(body.pending)
                .map(|depth| usize::try_from(depth).unwrap_or(usize::MAX))
                .ok_or_else(|| "Queen depth response omitted pending fields".into())
        })();
        match result {
            Ok(depth) => return Ok(depth),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| "Queen has no configured URL".into()))
}

fn read_response_limited(
    response: reqwest::blocking::Response,
    limit: u64,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if response.content_length().unwrap_or(0) > limit {
        return Err(format!("Queen depth response is larger than {limit} bytes").into());
    }
    let mut body = Vec::new();
    response.take(limit + 1).read_to_end(&mut body)?;
    if body.len() as u64 > limit {
        return Err(format!("Queen depth response is larger than {limit} bytes").into());
    }
    Ok(body)
}

fn zero_depths(options: &SupervisorConfig) -> HashMap<String, usize> {
    options
        .queues
        .iter()
        .cloned()
        .map(|queue| (queue, 0))
        .collect()
}

fn fail_open_desired(options: &SupervisorConfig) -> HashMap<String, usize> {
    if options.balance != "auto" {
        return desired(options, &zero_depths(options), &HashMap::new());
    }
    let weights = options
        .queues
        .iter()
        .cloned()
        .map(|queue| (queue, 1.0))
        .collect();
    allocation_for_target(
        options,
        &weights,
        options.min_processes.max(options.queues.len()),
        0,
    )
}

fn supervisor_runtimes(
    config: &Config,
    name: &str,
    options: &SupervisorConfig,
    pending: &mut PendingTelemetryCleanup,
) -> HashMap<String, f64> {
    if options.strategy != "time" || options.balance == "simple" {
        return HashMap::new();
    }

    let directory = Path::new(&config.state_directory).join("telemetry");
    let runtimes = read_runtimes(
        &directory,
        config.telemetry_ttl,
        TelemetryScope {
            supervisor: name,
            connection: &options.connection,
            consumer_group: &options.consumer_group,
        },
    );
    flush_telemetry_cleanup(config, name, pending);
    runtimes
}

fn read_runtimes(directory: &Path, ttl: u64, scope: TelemetryScope<'_>) -> HashMap<String, f64> {
    let mut totals: HashMap<String, f64> = HashMap::new();
    let mut samples: HashMap<String, u64> = HashMap::new();
    let Ok(directory_metadata) = fs::symlink_metadata(directory) else {
        return HashMap::new();
    };
    if !directory_metadata.file_type().is_dir() || directory_metadata.file_type().is_symlink() {
        return HashMap::new();
    }
    #[cfg(unix)]
    if directory_metadata.uid() != unsafe { libc::geteuid() }
        || directory_metadata.mode() & 0o7777 != 0o700
    {
        return HashMap::new();
    }

    let Ok(files) = fs::read_dir(directory) else {
        return HashMap::new();
    };
    let now = SystemTime::now();
    let mut candidates = Vec::new();
    let mut overflow = false;
    for (entry_count, entry) in files.enumerate() {
        if entry_count >= MAX_TELEMETRY_DIRECTORY_ENTRIES {
            overflow = true;
            break;
        }
        let Ok(entry) = entry else {
            overflow = true;
            continue;
        };
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let is_document = path.extension().and_then(|extension| extension.to_str()) == Some("json");
        let is_temporary = is_telemetry_temporary_name(name);
        if !is_document && !is_temporary {
            continue;
        }
        let Ok(metadata) = fs::symlink_metadata(&path) else {
            continue;
        };
        let stale = metadata
            .modified()
            .ok()
            .and_then(|at| now.duration_since(at).ok())
            .map(|age| age.as_secs() > ttl)
            .unwrap_or(false);
        if stale {
            remove_telemetry_if_same_file(&path, &metadata);
            continue;
        }
        if !is_document
            || !metadata.file_type().is_file()
            || metadata.len() == 0
            || metadata.len() > MAX_TELEMETRY_BYTES
        {
            continue;
        }
        #[cfg(unix)]
        if metadata.uid() != directory_metadata.uid() || metadata.mode() & 0o7777 != 0o600 {
            continue;
        }
        candidates.push((path, metadata));
    }

    // Keep the newest bounded reservoir and unlink older entries only when
    // their inode still matches the one inspected above. This turns PID-file
    // churn into progressive recovery instead of a permanent fail-closed
    // state while preserving the latest max-jobs=1 samples.
    candidates.sort_by(|(left_path, left), (right_path, right)| {
        right
            .modified()
            .unwrap_or(UNIX_EPOCH)
            .cmp(&left.modified().unwrap_or(UNIX_EPOCH))
            .then_with(|| right_path.cmp(left_path))
    });
    let mut paths = Vec::new();
    let mut selected_bytes = 0u64;
    for (path, metadata) in candidates {
        let next_bytes = selected_bytes.checked_add(metadata.len());
        if paths.len() < MAX_TELEMETRY_FILES
            && matches!(next_bytes, Some(total) if total <= MAX_TELEMETRY_TOTAL_BYTES)
        {
            selected_bytes = next_bytes.expect("checked above");
            paths.push((path, metadata));
        } else {
            remove_telemetry_if_same_file(&path, &metadata);
        }
    }
    if overflow {
        return HashMap::new();
    }

    let mut total_bytes = 0u64;
    for (path, inspected) in paths {
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        let Ok(file) = options.open(&path) else {
            continue;
        };
        let Ok(metadata) = file.metadata() else {
            continue;
        };
        if !metadata.file_type().is_file()
            || metadata.len() == 0
            || metadata.len() > MAX_TELEMETRY_BYTES
        {
            continue;
        }
        #[cfg(unix)]
        if metadata.uid() != directory_metadata.uid()
            || metadata.mode() & 0o7777 != 0o600
            || metadata.dev() != inspected.dev()
            || metadata.ino() != inspected.ino()
        {
            continue;
        }
        let stale = metadata
            .modified()
            .ok()
            .and_then(|at| now.duration_since(at).ok())
            .map(|age| age.as_secs() > ttl)
            .unwrap_or(false);
        if stale {
            remove_telemetry_if_same_file(&path, &metadata);
            continue;
        }

        let body = match read_file_limited(file, &path, MAX_TELEMETRY_BYTES) {
            Ok(body) => body,
            Err(_) => continue,
        };
        total_bytes = match total_bytes.checked_add(body.len() as u64) {
            Some(total) if total <= MAX_TELEMETRY_TOTAL_BYTES => total,
            _ => return HashMap::new(),
        };
        let document: TelemetryDocument = match serde_json::from_str(&body) {
            Ok(document) => document,
            Err(_) => continue,
        };
        if document.queues.len() > MAX_TELEMETRY_QUEUES_PER_FILE {
            continue;
        }
        if document.supervisor != scope.supervisor
            || document.connection != scope.connection
            || document.consumer_group != scope.consumer_group
        {
            continue;
        }
        for (queue, stats) in document.queues {
            let count = stats.samples.min(100);
            if queue.is_empty()
                || queue.len() > MAX_QUEUE_BYTES
                || queue.contains(',')
                || queue.chars().any(char::is_control)
                || count == 0
                || !stats.runtime_ewma_seconds.is_finite()
                || stats.runtime_ewma_seconds <= 0.0
            {
                continue;
            }
            let weighted = stats.runtime_ewma_seconds * count as f64;
            let next_total = totals.get(&queue).copied().unwrap_or(0.0) + weighted;
            if !weighted.is_finite() || !next_total.is_finite() {
                continue;
            }
            totals.insert(queue.clone(), next_total);
            *samples.entry(queue).or_default() += count;
        }
    }
    totals
        .into_iter()
        .filter_map(|(queue, total)| {
            let count = samples.get(&queue).copied().unwrap_or(0);
            let average = total / count.max(1) as f64;
            (count > 0 && average.is_finite() && average > 0.0).then_some((queue, average))
        })
        .collect()
}

fn is_telemetry_temporary_name(name: &str) -> bool {
    let Some((_, suffix)) = name.rsplit_once(".json.") else {
        return false;
    };
    let Some(token) = suffix.strip_suffix(".tmp") else {
        return false;
    };
    token.len() == 16 && token.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn schedule_telemetry_cleanup(
    config: &Config,
    supervisor: &str,
    pid: u32,
    pending: &mut PendingTelemetryCleanup,
) {
    let Some(options) = config.supervisors.get(supervisor) else {
        return;
    };
    if options.strategy == "time" && options.balance != "simple" {
        pending
            .entry(supervisor.to_owned())
            .or_default()
            .insert(pid);
    } else {
        remove_telemetry_pid(config, pid);
    }
}

fn flush_telemetry_cleanup(
    config: &Config,
    supervisor: &str,
    pending: &mut PendingTelemetryCleanup,
) {
    let Some(pids) = pending.remove(supervisor) else {
        return;
    };
    for pid in pids {
        remove_telemetry_pid(config, pid);
    }
}

fn remove_telemetry_pid(config: &Config, pid: u32) {
    if pid == 0 {
        return;
    }
    let directory = Path::new(&config.state_directory).join("telemetry");
    let Ok(directory_metadata) = fs::symlink_metadata(&directory) else {
        return;
    };
    if !directory_metadata.file_type().is_dir() || directory_metadata.file_type().is_symlink() {
        return;
    }
    #[cfg(unix)]
    if directory_metadata.uid() != unsafe { libc::geteuid() }
        || directory_metadata.mode() & 0o7777 != 0o700
    {
        return;
    }
    let path = directory.join(format!("{pid}.json"));
    let Ok(metadata) = fs::symlink_metadata(&path) else {
        return;
    };
    if !metadata.file_type().is_file() {
        return;
    }
    #[cfg(unix)]
    if metadata.uid() != directory_metadata.uid() || metadata.mode() & 0o7777 != 0o600 {
        return;
    }
    remove_telemetry_if_same_file(&path, &metadata);
}

fn remove_telemetry_if_same_file(path: &Path, opened: &fs::Metadata) {
    let Ok(current) = fs::symlink_metadata(path) else {
        return;
    };
    if !current.file_type().is_file() {
        return;
    }
    #[cfg(unix)]
    if current.dev() != opened.dev() || current.ino() != opened.ino() {
        return;
    }
    let _ = fs::remove_file(path);
}

fn desired(
    options: &SupervisorConfig,
    depths: &HashMap<String, usize>,
    runtimes: &HashMap<String, f64>,
) -> HashMap<String, usize> {
    desired_share(options, depths, runtimes, 0, 1)
}

/// The workers one supervisor should run per queue.
///
/// Coordinated replicas of the same pool each pass their position among the
/// live replicas. The backlog then sizes one fleet target, each replica takes
/// an even share of it (the remainder going to the lowest positions, and at
/// least its slice of the queues with backlog), and min_processes and
/// max_processes bound every replica. Fixed pools are not
/// split. The Laravel package's `AutoScaler::desired` is the same rule.
fn desired_share(
    options: &SupervisorConfig,
    depths: &HashMap<String, usize>,
    runtimes: &HashMap<String, f64>,
    replica: usize,
    replicas: usize,
) -> HashMap<String, usize> {
    let replicas = replicas.max(1);
    let replica = replica.min(replicas - 1);
    if options.balance == "simple" {
        return spread(
            empty_allocation(options),
            options.processes,
            &options.queues.iter().cloned().map(|q| (q, 1.0)).collect(),
            &options.queues,
            0,
            0,
        );
    }

    let weights = scaling_weights(options, depths, runtimes);
    let total_pressure: f64 = options
        .queues
        .iter()
        .map(|queue| weights.get(queue).copied().unwrap_or(0.0))
        .sum();
    let mut target = if !total_pressure.is_finite() {
        // Invalid/overflowing pressure must never under-provision backlog.
        // Saturate conservatively and keep Rust/PHP parity.
        options.max_processes
    } else if total_pressure <= 0.0 {
        options.min_processes
    } else {
        (total_pressure / scaling_divisor(options)).ceil() as usize
    };
    let mut offset = 0;
    if total_pressure > 0.0 && total_pressure.is_finite() {
        let active_queues = options
            .queues
            .iter()
            .filter(|queue| weights.get(*queue).copied().unwrap_or(0.0) > 0.0)
            .count();
        if options.balance == "auto" {
            // A positive backlog must not be made permanently unreachable by
            // rounding a small global target onto the first declared queue.
            target = target.max(active_queues);
        }
        // The fleet target is shared, the remainder going to the lowest
        // positions.
        let mut share = target / replicas + usize::from(replica < target % replicas);
        if options.balance == "auto" {
            // Replicas start covering the queues with backlog at evenly
            // spaced positions, which move only with the replica or queue
            // count. A share of at least that spacing closes every gap that
            // max_processes leaves room to close.
            share = share.max(active_queues.div_ceil(replicas));
            offset = replica * active_queues / replicas;
        }
        target = share;
    }
    target = target.clamp(options.min_processes, options.max_processes);
    allocation_for_target(options, &weights, target, offset)
}

fn scaling_weights(
    options: &SupervisorConfig,
    depths: &HashMap<String, usize>,
    runtimes: &HashMap<String, f64>,
) -> HashMap<String, f64> {
    options
        .queues
        .iter()
        .map(|queue| {
            let depth = depths.get(queue).copied().unwrap_or(0) as f64;
            let weight = if options.strategy == "time" {
                depth
                    * runtimes
                        .get(queue)
                        .copied()
                        .unwrap_or(options.default_runtime_seconds)
            } else {
                depth
            };
            (queue.clone(), weight)
        })
        .collect()
}

fn scaling_divisor(options: &SupervisorConfig) -> f64 {
    if options.strategy == "time" {
        options.target_clear_seconds
    } else {
        options.target_jobs_per_process as f64
    }
}

fn empty_allocation(options: &SupervisorConfig) -> HashMap<String, usize> {
    options.queues.iter().cloned().map(|q| (q, 0)).collect()
}

/// The pools one of whose watched queues grew, from the watcher's
/// `connection\0queue` keys.
fn woken_pools(config: &Config, keys: &HashSet<String>) -> HashSet<String> {
    config
        .supervisors
        .iter()
        .filter(|(_, options)| {
            watch::follows_backlog(options)
                && options
                    .queues
                    .iter()
                    .any(|queue| keys.contains(&watch::key(&options.connection, queue)))
        })
        .map(|(name, _)| name.clone())
        .collect()
}

fn has_room(config: &Config, pools: &Pools, name: &str) -> bool {
    config.supervisors.get(name).is_some_and(|options| {
        current_allocation(pools, name, options)
            .values()
            .sum::<usize>()
            < options.max_processes
    })
}

/// The pools to evaluate before the next regular poll: woken or still
/// climbing, with room to grow, a wake interval after their last evaluation.
fn event_due_pools(
    config: &Config,
    pools: &Pools,
    woken: &HashSet<String>,
    climbing: &HashSet<String>,
    last_evaluated: &HashMap<String, Instant>,
    now: Instant,
) -> HashSet<String> {
    config
        .supervisors
        .iter()
        .filter(|(name, options)| {
            watch::follows_backlog(options)
                && has_room(config, pools, name)
                && watch::due(
                    woken.contains(*name),
                    climbing.contains(*name),
                    last_evaluated.get(*name).copied(),
                    now,
                )
        })
        .map(|(name, _)| name.clone())
        .collect()
}

/// Inside the cooldown a woken pool may only add workers: no queue loses
/// one, and the pool grows by what the target adds overall, filling the
/// queues short of their target in declared order. None when that adds
/// nothing.
fn grow_only_target(
    options: &SupervisorConfig,
    raw: &HashMap<String, usize>,
    current: &HashMap<String, usize>,
) -> Option<HashMap<String, usize>> {
    let active: usize = current.values().sum();
    let mut room = raw.values().sum::<usize>().saturating_sub(active);
    if room == 0 {
        return None;
    }
    let mut target = current.clone();
    for queue in &options.queues {
        let short = raw
            .get(queue)
            .copied()
            .unwrap_or(0)
            .saturating_sub(current.get(queue).copied().unwrap_or(0));
        let added = short.min(room);
        *target.entry(queue.clone()).or_default() += added;
        room -= added;
    }
    (target.values().sum::<usize>() > active).then_some(target)
}

fn current_allocation(
    pools: &Pools,
    supervisor: &str,
    options: &SupervisorConfig,
) -> HashMap<String, usize> {
    options
        .queues
        .iter()
        .map(|queue| {
            let key = (supervisor.to_owned(), queue.clone());
            (queue.clone(), pools.get(&key).map(Vec::len).unwrap_or(0))
        })
        .collect()
}

fn allocation_for_target(
    options: &SupervisorConfig,
    weights: &HashMap<String, f64>,
    target: usize,
    offset: usize,
) -> HashMap<String, usize> {
    let mut allocation = empty_allocation(options);
    if options.balance == "off" {
        allocation.insert(options.queues[0].clone(), target);
        return allocation;
    }
    let floor = if options.balance == "auto" {
        options.min_processes_per_queue
    } else {
        0
    };
    let target = target.max(floor * options.queues.len());
    spread(allocation, target, weights, &options.queues, offset, floor)
}

fn stabilize_desired(
    options: &SupervisorConfig,
    raw: HashMap<String, usize>,
    previous: Option<&HashMap<String, usize>>,
    guard: &mut ScaleGuard,
    now: Instant,
) -> HashMap<String, usize> {
    let Some(previous) = previous else {
        guard.reset();
        return raw;
    };
    let previous_total: usize = previous.values().sum();
    let raw_total: usize = raw.values().sum();
    if raw_total >= previous_total || options.scale_down_delay == 0 {
        guard.reset();
        return raw;
    }

    if guard.candidate_total != Some(raw_total) {
        guard.candidate_total = Some(raw_total);
        guard.downscale_since = Some(now);
        return previous.clone();
    }
    let since = guard.downscale_since.get_or_insert(now);
    if now.duration_since(*since) >= Duration::from_secs(options.scale_down_delay) {
        return raw;
    }
    previous.clone()
}

impl ScaleGuard {
    fn reset(&mut self) {
        self.downscale_since = None;
        self.candidate_total = None;
    }
}

/// `floor` workers on every queue, then one worker per queue with backlog still
/// without one, starting at `offset` among those queues, then the rest in
/// proportion to the backlog.
fn spread(
    mut allocation: HashMap<String, usize>,
    target: usize,
    weights: &HashMap<String, f64>,
    queues: &[String],
    offset: usize,
    floor: usize,
) -> HashMap<String, usize> {
    for queue in queues {
        *allocation.get_mut(queue).unwrap() += floor;
    }
    let target = target - floor * queues.len();
    let active: Vec<&String> = queues
        .iter()
        .filter(|queue| {
            weights.get(*queue).copied().unwrap_or(0.0) > 0.0 && allocation[*queue] == 0
        })
        .collect();
    let covered = target.min(active.len());
    for index in 0..covered {
        *allocation
            .get_mut(active[(offset + index) % active.len()])
            .unwrap() += 1;
    }
    let remaining = target - covered;
    for _ in 0..remaining {
        let mut selected = &queues[0];
        let mut best = -1.0_f64;
        for queue in queues {
            let score = weights.get(queue).copied().unwrap_or(0.0).max(1.0)
                / (allocation[queue] + 1) as f64;
            if score > best {
                selected = queue;
                best = score;
            }
        }
        *allocation.get_mut(selected).unwrap() += 1;
    }
    allocation
}

impl RestartGuard {
    fn spawn_permission(&self, now: Instant) -> SpawnPermission {
        match self.phase {
            RestartPhase::Closed => SpawnPermission::Normal,
            RestartPhase::Backoff { until } | RestartPhase::Open { until } if now >= until => {
                SpawnPermission::Probe
            }
            RestartPhase::Backoff { .. } | RestartPhase::Open { .. } | RestartPhase::Probe => {
                SpawnPermission::Blocked
            }
        }
    }

    fn mark_spawned(&mut self, permission: SpawnPermission) {
        if permission == SpawnPermission::Probe {
            self.phase = RestartPhase::Probe;
        }
    }

    fn record_failure(&mut self, options: &SupervisorConfig, now: Instant) -> (Duration, bool) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if self.consecutive_failures >= CRASH_CIRCUIT_THRESHOLD {
            let delay = Duration::from_secs(options.restart_backoff_max);
            self.phase = RestartPhase::Open { until: now + delay };
            return (delay, true);
        }

        let exponent = self.consecutive_failures.saturating_sub(1).min(63);
        let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
        let seconds = options
            .restart_backoff
            .saturating_mul(multiplier)
            .min(options.restart_backoff_max);
        let delay = Duration::from_secs(seconds);
        self.phase = RestartPhase::Backoff { until: now + delay };
        (delay, false)
    }

    fn record_healthy(&mut self) {
        self.consecutive_failures = 0;
        self.phase = RestartPhase::Closed;
    }

    fn cancel_probe(&mut self) {
        if matches!(self.phase, RestartPhase::Probe) {
            self.record_healthy();
        }
    }

    /// Admit the next probe at once, with the failure count unchanged.
    fn release_probe(&mut self, now: Instant) {
        if matches!(self.phase, RestartPhase::Probe) {
            self.phase = if self.consecutive_failures >= CRASH_CIRCUIT_THRESHOLD {
                RestartPhase::Open { until: now }
            } else {
                RestartPhase::Backoff { until: now }
            };
        }
    }

    fn state_name(&self) -> &'static str {
        match self.phase {
            RestartPhase::Closed => "closed",
            RestartPhase::Backoff { .. } => "backoff",
            RestartPhase::Open { .. } => "open",
            RestartPhase::Probe => "probe",
        }
    }

    fn retry_in_seconds(&self, now: Instant) -> Option<u64> {
        let until = match self.phase {
            RestartPhase::Backoff { until } | RestartPhase::Open { until } => until,
            RestartPhase::Closed | RestartPhase::Probe => return None,
        };
        let remaining = until.saturating_duration_since(now);
        Some(remaining.as_secs() + u64::from(remaining.subsec_nanos() > 0))
    }
}

/// How workers start: the resolved configuration, and the fork server when
/// prefork is on.
struct Launcher<'a> {
    config: &'a Config,
    forks: Option<&'a Rc<RefCell<ForkServer>>>,
}

fn reconcile(
    launcher: &Launcher<'_>,
    name: &str,
    options: &SupervisorConfig,
    desired: HashMap<String, usize>,
    pools: &mut Pools,
    restarts: &mut RestartStates,
    draining: &mut Draining,
) -> Result<(), Box<dyn std::error::Error>> {
    let Launcher { config, forks } = *launcher;
    let supervised_processes = options.queues.iter().fold(0usize, |total, queue| {
        total.saturating_add(
            pools
                .get(&(name.to_owned(), queue.clone()))
                .map(Vec::len)
                .unwrap_or(0),
        )
    });
    let desired_total = desired.values().sum();
    let mut budget = reconcile_budget(options, supervised_processes, desired_total);
    let used_process_budget = process_budget_used(config, pools, draining);
    let mut process_slots = remaining_process_slots(config.process_limit, used_process_budget);
    let worker_process_cost = process_cost(options);
    for queue in &options.queues {
        let target = desired.get(queue).copied().unwrap_or(0);
        let key = (name.to_owned(), queue.clone());
        let pool = pools.entry(key).or_default();
        while budget > 0 && target < pool.len() {
            if let Some(worker) = pool.pop() {
                if worker.restart_probe {
                    restarts
                        .entry((name.to_owned(), queue.clone()))
                        .or_default()
                        .cancel_probe();
                }
                begin_termination(
                    worker,
                    Duration::from_secs(config.shutdown_grace),
                    (name.to_owned(), queue.clone()),
                    draining,
                );
            }
            budget -= 1;
        }
    }
    for queue in &options.queues {
        let target = desired.get(queue).copied().unwrap_or(0);
        let key = (name.to_owned(), queue.clone());
        let pool = pools.entry(key).or_default();
        while budget > 0 && process_slots >= worker_process_cost && target > pool.len() {
            let restart = restarts
                .entry((name.to_owned(), queue.clone()))
                .or_default();
            let permission = restart.spawn_permission(Instant::now());
            if permission == SpawnPermission::Blocked {
                break;
            }
            match spawn_worker(
                config,
                name,
                queue,
                options,
                permission == SpawnPermission::Probe,
                forks,
            ) {
                Ok(worker) => {
                    restart.mark_spawned(permission);
                    pool.push(worker);
                    budget -= 1;
                    process_slots -= worker_process_cost;
                }
                Err(error) => {
                    let (delay, circuit_open) = restart.record_failure(options, Instant::now());
                    return Err(format!(
                        "could not start {name}:{queue}: {error}; {} for {}s",
                        if circuit_open {
                            "circuit open"
                        } else {
                            "backoff"
                        },
                        delay.as_secs(),
                    )
                    .into());
                }
            }
        }
    }
    Ok(())
}

fn reconcile_budget(options: &SupervisorConfig, active: usize, desired: usize) -> usize {
    // balance_max_shift bounds elastic changes, but baseline capacity must be
    // established and restored without waiting through several cooldowns.
    let baseline = match options.balance.as_str() {
        "simple" => options.processes,
        "auto" => options
            .min_processes
            .max(options.min_processes_per_queue * options.queues.len()),
        _ => options.min_processes,
    };
    let budget = options
        .balance_max_shift
        .max(baseline.saturating_sub(active));
    if options.fast_scale_up && desired > active {
        // A burst closes half of the remaining gap every cycle instead of one
        // balance_max_shift step; scaling down stays gradual.
        return budget.max((desired - active).div_ceil(2));
    }
    budget
}

fn process_cost(options: &SupervisorConfig) -> usize {
    if options.lease_renewal {
        2
    } else {
        1
    }
}

fn process_budget_used(config: &Config, pools: &Pools, draining: &Draining) -> usize {
    let active = pools.iter().fold(0usize, |total, ((supervisor, _), pool)| {
        let cost = config
            .supervisors
            .get(supervisor)
            .map(process_cost)
            .unwrap_or(1);
        total.saturating_add(pool.len().saturating_mul(cost))
    });
    draining.iter().fold(active, |total, entry| {
        let cost = config
            .supervisors
            .get(&entry.pool.0)
            .map(process_cost)
            .unwrap_or(1);
        total.saturating_add(cost)
    })
}

/// Serve lease renewal from the master when a pool renews leases. Linux only:
/// there a worker dies with its master (PDEATHSIG, or the fork server) and a
/// pidfd pins it for fencing. Elsewhere, or with queen.supervisor.lease_service
/// false, each worker starts a PHP helper. A helper still counts in
/// process_cost, since a worker falls back to one when the service refuses it.
fn start_lease_service(config: &Config, directory: &Path) -> Option<String> {
    if !serves_lease_renewal(config, cfg!(target_os = "linux")) {
        return None;
    }
    match lease::start(directory, lease::signal_fence()) {
        Ok(path) => {
            eprintln!("lease renewal: served by the supervisor");
            Some(path)
        }
        Err(error) => {
            eprintln!("lease renewal: one helper process per worker, the service failed: {error}");
            None
        }
    }
}

/// Whether the master serves lease renewal, on `linux`, for this
/// configuration. The configuration alone decides: the environment variable
/// QUEEN_SUPERVISOR_LEASE_SERVICE of earlier releases is no longer read. When
/// queen.supervisor.lease_service turns the service off for pools that renew
/// leases, this says so.
fn serves_lease_renewal(config: &Config, linux: bool) -> bool {
    if !linux || !config.supervisors.values().any(|o| o.lease_renewal) {
        return false;
    }
    if !config.lease_service {
        eprintln!(
            "lease renewal: off in the master, queen.supervisor.lease_service is false: \
             one helper process per worker"
        );
        return false;
    }
    true
}

fn remaining_process_slots(limit: usize, used: usize) -> usize {
    limit.saturating_sub(used)
}

fn spawn_worker(
    config: &Config,
    name: &str,
    queue: &str,
    o: &SupervisorConfig,
    restart_probe: bool,
    forks: Option<&Rc<RefCell<ForkServer>>>,
) -> Result<Worker, std::io::Error> {
    if let Some(server) = forks.filter(|server| !server.borrow().disabled) {
        let (arguments, environment) = worker_invocation(config, name, queue, o);
        let forked = server.borrow_mut().fork(&arguments, &environment);
        match forked {
            Ok(pid) => {
                eprintln!("started {name}:{queue} pid={pid} (forked)");
                forget_exit_marker(config, pid);
                return Ok(Worker::from_process(
                    WorkerProcess::Forked {
                        pid,
                        server: Rc::clone(server),
                        status: None,
                    },
                    restart_probe,
                ));
            }
            // The server stays open to report the workers it already forked.
            Err(error) => {
                eprintln!("[{name}:{queue}] prefork failed, spawning workers from now on: {error}")
            }
        }
    }
    let mut command = worker_command(config, name, queue, o);
    #[cfg(unix)]
    unsafe {
        #[cfg(target_os = "linux")]
        let supervisor_pid = libc::getpid();
        command.pre_exec(move || {
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            #[cfg(target_os = "linux")]
            {
                // A master crash is not a graceful drain. Hard-fence the PHP
                // handler before a replacement generation can start; normal
                // supervisor shutdown still sends TERM and honors its grace.
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::getppid() != supervisor_pid {
                    return Err(std::io::Error::other(
                        "supervisor exited while the worker was starting",
                    ));
                }
            }
            Ok(())
        });
    }
    let child = command.spawn()?;
    eprintln!("started {name}:{queue} pid={}", child.id());
    forget_exit_marker(config, child.id());
    Ok(Worker::new(child, restart_probe))
}

/// The `queue:work` arguments after the command name, and the environment a
/// worker gets on top of the master's (None removes a variable). A spawned
/// and a forked worker receive exactly the same.
fn worker_invocation(
    config: &Config,
    name: &str,
    queue: &str,
    o: &SupervisorConfig,
) -> (Vec<String>, Vec<(String, Option<String>)>) {
    let worker_queues = if o.balance == "off" {
        o.queues.join(",")
    } else {
        queue.to_owned()
    };
    let mut arguments = vec![
        o.connection.clone(),
        format!("--queue={worker_queues}"),
        format!("--sleep={}", o.sleep),
        format!("--timeout={}", o.timeout),
        format!("--tries={}", o.tries),
        format!("--memory={}", o.memory),
        format!("--backoff={}", o.backoff),
        format!("--max-jobs={}", o.max_jobs),
        format!("--max-time={}", o.max_time),
        format!("--rest={}", o.rest),
    ];
    if o.force {
        arguments.push("--force".to_owned());
    }
    if o.quiet {
        arguments.push("--quiet".to_owned());
    }
    let telemetry = (o.strategy == "time" && o.balance != "simple").then(|| {
        Path::new(&config.state_directory)
            .join("telemetry")
            .to_string_lossy()
            .into_owned()
    });
    let mut environment = vec![
        (
            "QUEEN_LARAVEL_CONSUMER_GROUP".to_owned(),
            Some(o.consumer_group.clone()),
        ),
        (
            "QUEEN_LARAVEL_CONNECTION".to_owned(),
            Some(o.connection.clone()),
        ),
        ("QUEEN_LARAVEL_SUPERVISOR".to_owned(), Some(name.to_owned())),
        (
            "QUEEN_LARAVEL_RETRY_AFTER".to_owned(),
            Some(o.retry_after.to_string()),
        ),
        ("QUEEN_SUPERVISOR_TELEMETRY_DIR".to_owned(), telemetry),
        (
            "QUEEN_SUPERVISOR_LEASE_SOCKET".to_owned(),
            config.lease_socket.clone().filter(|_| o.lease_renewal),
        ),
        // Where the worker says why it exits; see prepare_exit_markers.
        (
            "QUEEN_SUPERVISOR_EXITS_DIR".to_owned(),
            config.exit_markers.clone(),
        ),
    ];
    if o.balance == "off" {
        // A blocking reserve on the first queue in Laravel's ordered list
        // would prevent lower-priority queues from ever being checked.
        environment.push(("QUEEN_LARAVEL_BLOCK_FOR".to_owned(), Some("0".to_owned())));
    }
    (arguments, environment)
}

fn worker_command(config: &Config, name: &str, queue: &str, o: &SupervisorConfig) -> Command {
    let (arguments, environment) = worker_invocation(config, name, queue, o);
    let mut command = Command::new(&config.php_binary);
    command
        .current_dir(&config.cwd)
        .arg(&config.artisan)
        .arg("queue:work")
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    for (variable, value) in environment {
        match value {
            Some(value) => command.env(variable, value),
            None => command.env_remove(variable),
        };
    }
    command
}

/// Returns whether a worker forked by `forks`, the current fork server,
/// stopped for `php artisan queue:restart`.
fn reap(
    config: &Config,
    pools: &mut Pools,
    restarts: &mut RestartStates,
    pending: &mut PendingTelemetryCleanup,
    forks: Option<&Rc<RefCell<ForkServer>>>,
) -> bool {
    let mut restarted = false;
    for (key, pool) in pools.iter_mut() {
        let Some(options) = config.supervisors.get(&key.0) else {
            continue;
        };
        let restart = restarts.entry(key.clone()).or_default();
        pool.retain_mut(|worker| match worker.child.try_wait() {
            Ok(None) => true,
            Ok(Some(status)) => {
                let pid = worker.child.id();
                let announced = announced_exit(config, pid, status);
                if announced == Some(AnnouncedExit::QueueRestart) && worker.child.forked_by(forks) {
                    restarted = true;
                }
                record_worker_exit(key, worker, status, announced, options, restart);
                schedule_telemetry_cleanup(config, &key.0, pid, pending);
                false
            }
            Err(error) => {
                eprintln!(
                    "[{}:{}] pid={} wait failed: {error}",
                    key.0,
                    key.1,
                    worker.child.id()
                );
                true
            }
        });
    }
    restarted
}

/// After `queue:restart`, boot a new fork server, so the workers forked from
/// then on run the code on disk, as spawned ones do. The old server stays
/// open while a worker it forked still runs (each holds a reference to it):
/// closing it would SIGKILL that worker. If the new one cannot start, workers
/// are spawned from then on.
fn refresh_fork_server(
    config: &Config,
    running: &AtomicBool,
    current: Option<Rc<RefCell<ForkServer>>>,
    retired: &mut Vec<Rc<RefCell<ForkServer>>>,
) -> Option<Rc<RefCell<ForkServer>>> {
    retired.extend(current);
    eprintln!("prefork: queue:restart received, starting a new fork server");
    match ForkServer::start(config, running) {
        Ok(server) => {
            eprintln!("prefork: fork server started");
            Some(Rc::new(RefCell::new(server)))
        }
        Err(error) => {
            eprintln!("prefork disabled, spawning workers: {error}");
            None
        }
    }
}

/// Close each replaced fork server once no worker it forked is left.
fn close_retired_forks(retired: &mut Vec<Rc<RefCell<ForkServer>>>) {
    retired.retain(|server| {
        if Rc::strong_count(server) > 1 {
            return true;
        }
        server.borrow_mut().close(Duration::from_secs(5));
        false
    });
}

/// `announced`: what the worker said about this exit; see announced_exit.
fn record_worker_exit(
    key: &PoolKey,
    worker: &Worker,
    status: ExitStatus,
    announced: Option<AnnouncedExit>,
    options: &SupervisorConfig,
    restart: &mut RestartGuard,
) {
    let uptime = worker.started_at.elapsed();
    if matches!(restart.phase, RestartPhase::Probe) && !worker.restart_probe {
        eprintln!(
            "[{}:{}] pid={} exited with {status} after {:.3}s while restart probe is active; circuit unchanged",
            key.0,
            key.1,
            worker.child.id(),
            uptime.as_secs_f64(),
        );
        return;
    }
    if status.success() {
        restart.record_healthy();
        eprintln!(
            "[{}:{}] pid={} exited normally after {:.3}s",
            key.0,
            key.1,
            worker.child.id(),
            uptime.as_secs_f64(),
        );
        return;
    }
    if announced == Some(AnnouncedExit::MemoryLimit) {
        // A deliberate stop after a job, like --max-jobs: it made progress.
        restart.record_healthy();
        eprintln!(
            "[{}:{}] pid={} stopped at Laravel's --memory limit after a job; restarting",
            key.0,
            key.1,
            worker.child.id(),
        );
        return;
    }
    // Backoff is for short-lived exits. A worker that ran this long is not
    // crash-looping: queue:work exits 12 at --memory, and a job timeout kills
    // the worker.
    if !worker.restart_probe && uptime >= Duration::from_secs(options.stable_after) {
        restart.record_healthy();
        eprintln!(
            "[{}:{}] pid={} exited with {status} after {:.3}s; restarting",
            key.0,
            key.1,
            worker.child.id(),
            uptime.as_secs_f64(),
        );
        return;
    }
    if announced == Some(AnnouncedExit::JobTimeout) {
        // The job failed, not the worker, however soon it came. A probe
        // proved nothing either way: the next one starts at once.
        if worker.restart_probe {
            restart.release_probe(Instant::now());
        }
        eprintln!(
            "[{}:{}] pid={} exited after Laravel's job timeout; restarted without backoff",
            key.0,
            key.1,
            worker.child.id(),
        );
        return;
    }
    let (delay, circuit_open) = restart.record_failure(options, Instant::now());
    eprintln!(
        "[{}:{}] pid={} exited with {status} after {:.3}s; {} for {}s",
        key.0,
        key.1,
        worker.child.id(),
        uptime.as_secs_f64(),
        if circuit_open {
            "circuit open"
        } else {
            "restart backoff"
        },
        delay.as_secs(),
    );
}

/// Laravel SIGKILLs a worker whose job outlives its timeout, which looks like
/// any other SIGKILL from here, the OOM killer's included. So the worker first
/// leaves `exits/<pid>` holding `timeout` (the Laravel package's
/// WorkerExitMarker) in this private directory of the state directory. A
/// worker that passes --memory exits 12 after an idle sleep as well as after a
/// job; it writes `memory` only after a job. The directory is emptied for
/// every generation: a marker an earlier one left explains no exit of this one.
fn prepare_exit_markers(state_directory: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let directory = state_directory.join("exits");
    match fs::create_dir(&directory) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let metadata = fs::symlink_metadata(&directory)?;
    if !metadata.file_type().is_dir() {
        return Err(format!("{} must be a real directory", directory.display()).into());
    }
    #[cfg(unix)]
    {
        if metadata.uid() != unsafe { libc::geteuid() } {
            return Err(format!(
                "{} must be owned by the supervisor user",
                directory.display()
            )
            .into());
        }
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
    }
    // A leftover only matters for its pid, and spawn_worker removes it when a
    // worker starts with that pid.
    let names: Vec<_> = fs::read_dir(&directory)?
        .take(MAX_EXIT_MARKER_ENTRIES)
        .filter_map(|entry| entry.ok().map(|entry| entry.file_name()))
        .collect();
    for name in names {
        remove_exit_marker_entry(&directory.join(name));
    }
    Ok(directory)
}

/// What a worker said about its exit before it exited; see prepare_exit_markers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AnnouncedExit {
    /// Laravel's job timeout SIGKILLed it.
    JobTimeout,
    /// It passed --memory after handling a job.
    MemoryLimit,
    /// It stopped for `php artisan queue:restart` (Laravel 12 says why).
    QueueRestart,
}

/// What the worker announced before this exit: Laravel's job timeout, when
/// SIGKILL ended it, its memory limit after a job, when it exited 12, or
/// queue:restart, when it exited 0. A marker explains no other exit. Either
/// way the marker is consumed.
fn announced_exit(config: &Config, pid: u32, status: ExitStatus) -> Option<AnnouncedExit> {
    let directory = config.exit_markers.as_deref()?;
    match take_exit_marker(Path::new(directory), pid)?.as_str() {
        JOB_TIMEOUT_MARKER if killed_by_sigkill(status) => Some(AnnouncedExit::JobTimeout),
        MEMORY_LIMIT_MARKER if status.code() == Some(LARAVEL_EXIT_MEMORY_LIMIT) => {
            Some(AnnouncedExit::MemoryLimit)
        }
        QUEUE_RESTART_MARKER if status.success() => Some(AnnouncedExit::QueueRestart),
        _ => None,
    }
}

#[cfg(unix)]
fn killed_by_sigkill(status: ExitStatus) -> bool {
    use std::os::unix::process::ExitStatusExt;
    status.signal() == Some(libc::SIGKILL)
}

#[cfg(not(unix))]
fn killed_by_sigkill(_status: ExitStatus) -> bool {
    false
}

fn forget_exit_marker(config: &Config, pid: u32) {
    if let Some(directory) = config.exit_markers.as_deref() {
        remove_exit_marker(Path::new(directory), pid);
    }
}

/// Read and remove the marker a worker left before it exited: at most a few
/// bytes, from a private regular file, never through a symbolic link. None
/// when there is none. Anything else under that name is removed as well.
fn take_exit_marker(directory: &Path, pid: u32) -> Option<String> {
    let path = exit_marker_path(directory, pid)?;
    remove_exit_marker_entry(&directory.join(format!("{pid}.tmp")));
    let inspected = fs::symlink_metadata(&path).ok()?;
    let marker = read_exit_marker(&path, &inspected);
    remove_exit_marker_entry(&path);
    marker
}

/// Remove a worker's marker, and its temporary file, unread.
fn remove_exit_marker(directory: &Path, pid: u32) {
    if let Some(path) = exit_marker_path(directory, pid) {
        remove_exit_marker_entry(&path);
        remove_exit_marker_entry(&directory.join(format!("{pid}.tmp")));
    }
}

/// `<directory>/<pid>`, when the directory is the private one the master made.
fn exit_marker_path(directory: &Path, pid: u32) -> Option<PathBuf> {
    if pid == 0 {
        return None;
    }
    let metadata = fs::symlink_metadata(directory).ok()?;
    if !metadata.file_type().is_dir() {
        return None;
    }
    #[cfg(unix)]
    if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o7777 != 0o700 {
        return None;
    }
    Some(directory.join(pid.to_string()))
}

fn read_exit_marker(path: &Path, inspected: &fs::Metadata) -> Option<String> {
    if !inspected.file_type().is_file()
        || inspected.len() == 0
        || inspected.len() > MAX_EXIT_MARKER_BYTES
    {
        return None;
    }
    #[cfg(unix)]
    if inspected.uid() != unsafe { libc::geteuid() } || inspected.mode() & 0o7777 != 0o600 {
        return None;
    }
    let mut options = OpenOptions::new();
    options.read(true);
    // O_NONBLOCK: a FIFO swapped in after the check cannot stall the loop.
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options.open(path).ok()?;
    let opened = file.metadata().ok()?;
    if !opened.file_type().is_file() {
        return None;
    }
    #[cfg(unix)]
    if opened.dev() != inspected.dev() || opened.ino() != inspected.ino() {
        return None;
    }
    let body = read_file_limited(file, path, MAX_EXIT_MARKER_BYTES).ok()?;
    let marker = body.trim();
    (!marker.is_empty()).then(|| marker.to_owned())
}

/// remove_file() removes a symbolic link itself, never its target; a
/// directory stays.
fn remove_exit_marker_entry(path: &Path) {
    if fs::symlink_metadata(path).is_ok_and(|metadata| !metadata.file_type().is_dir()) {
        let _ = fs::remove_file(path);
    }
}

fn observe_stable_workers(config: &Config, pools: &mut Pools, restarts: &mut RestartStates) {
    for (key, pool) in pools {
        let Some(options) = config.supervisors.get(&key.0) else {
            continue;
        };
        let restart = restarts.entry(key.clone()).or_default();
        for worker in pool {
            if !worker.stability_reported
                && worker.started_at.elapsed() >= Duration::from_secs(options.stable_after)
            {
                worker.stability_reported = true;
                let authoritative =
                    !matches!(restart.phase, RestartPhase::Probe) || worker.restart_probe;
                worker.restart_probe = false;
                if authoritative {
                    restart.record_healthy();
                }
            }
        }
    }
}

/// SIGTERM every worker, wait for the grace, then SIGKILL. `while_draining`
/// runs once every worker is signalled, inside the grace.
fn shutdown(
    config: &Config,
    pools: &mut Pools,
    draining: &mut Draining,
    grace: Duration,
    pending: &mut PendingTelemetryCleanup,
    while_draining: impl FnOnce(),
) {
    for pool in pools.values_mut() {
        for worker in pool.iter_mut() {
            signal_worker(&mut worker.child, libc::SIGTERM);
        }
    }
    for entry in draining.iter_mut() {
        signal_worker(&mut entry.worker.child, libc::SIGTERM);
    }
    let deadline = Instant::now() + grace;
    while_draining();
    loop {
        reap_for_shutdown(config, pools, pending);
        reap_draining(config, draining, pending);
        if (pools.values().all(Vec::is_empty) && draining.is_empty()) || Instant::now() >= deadline
        {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    // A successful kill(2) is not proof that the child has exited, and wait
    // itself can fail transiently. Retain ownership and retry KILL until every
    // worker is observed reaped; only then may run() publish stopped and drop
    // the generation lock.
    let mut next_kill = Instant::now();
    while !pools.values().all(Vec::is_empty) || !draining.is_empty() {
        reap_for_shutdown(config, pools, pending);
        reap_draining(config, draining, pending);
        if pools.values().all(Vec::is_empty) && draining.is_empty() {
            break;
        }
        if Instant::now() >= next_kill {
            for pool in pools.values_mut() {
                for worker in pool {
                    signal_process_group(&mut worker.child, libc::SIGKILL);
                }
            }
            for entry in draining.iter_mut() {
                signal_process_group(&mut entry.worker.child, libc::SIGKILL);
            }
            next_kill = Instant::now() + Duration::from_secs(1);
        }
        thread::sleep(Duration::from_millis(50));
    }
    pools.retain(|_, pool| !pool.is_empty());
    let supervisors = pending.keys().cloned().collect::<Vec<_>>();
    for supervisor in supervisors {
        flush_telemetry_cleanup(config, &supervisor, pending);
    }
}

fn reap_for_shutdown(config: &Config, pools: &mut Pools, pending: &mut PendingTelemetryCleanup) {
    for (key, pool) in pools.iter_mut() {
        pool.retain_mut(|worker| match worker.child.try_wait() {
            Ok(Some(_)) => {
                schedule_telemetry_cleanup(config, &key.0, worker.child.id(), pending);
                false
            }
            Ok(None) | Err(_) => true,
        });
    }
}

fn drain_all(
    pools: &mut Pools,
    restarts: &mut RestartStates,
    draining: &mut Draining,
    grace: Duration,
) {
    for (pool, workers) in pools.iter_mut() {
        for worker in workers.drain(..) {
            if worker.restart_probe {
                restarts.entry(pool.clone()).or_default().cancel_probe();
            }
            begin_termination(worker, grace, pool.clone(), draining);
        }
    }
}

fn begin_termination(worker: Worker, grace: Duration, pool: PoolKey, draining: &mut Draining) {
    let mut entry = DrainingWorker {
        worker,
        deadline: Instant::now() + grace,
        label: format!("{}:{}", pool.0, pool.1),
        pool,
    };
    // Signal only queue:work during graceful drain. A lease-renewal helper in
    // the worker's process group must remain alive until the worker finishes;
    // the whole group is reserved for the forced-kill deadline.
    signal_worker(&mut entry.worker.child, libc::SIGTERM);
    draining.push(entry);
}

fn reap_draining(config: &Config, draining: &mut Draining, pending: &mut PendingTelemetryCleanup) {
    let now = Instant::now();
    draining.retain_mut(|entry| match entry.worker.child.try_wait() {
        Ok(Some(_)) => {
            schedule_telemetry_cleanup(config, &entry.pool.0, entry.worker.child.id(), pending);
            // A drained worker's exit is not classified: drop its marker.
            forget_exit_marker(config, entry.worker.child.id());
            false
        }
        Ok(None) if now >= entry.deadline => {
            eprintln!(
                "[{}] pid={} exceeded shutdown grace; sending SIGKILL",
                entry.label,
                entry.worker.child.id()
            );
            signal_process_group(&mut entry.worker.child, libc::SIGKILL);
            // Sending KILL is not observing exit. Keep the worker charged to
            // process_limit and poll with try_wait so an uninterruptible child
            // cannot freeze heartbeat/control processing.
            entry.deadline = now + Duration::from_secs(1);
            true
        }
        Ok(None) => true,
        Err(error) => {
            eprintln!(
                "[{}] pid={} wait failed during termination: {error}",
                entry.label,
                entry.worker.child.id()
            );
            if now >= entry.deadline {
                signal_process_group(&mut entry.worker.child, libc::SIGKILL);
                entry.deadline = now + Duration::from_secs(1);
            }
            true
        }
    });
}

fn signal_worker(child: &mut WorkerProcess, signal: i32) {
    #[cfg(unix)]
    unsafe {
        libc::kill(child.id() as i32, signal);
    }
    #[cfg(not(unix))]
    let _ = child.kill();
}

fn signal_process_group(child: &mut WorkerProcess, signal: i32) {
    #[cfg(unix)]
    unsafe {
        let pid = child.id() as i32;
        // Every worker starts as the leader of its own process group. Signal
        // both the group and the leader: a worker may later change group while
        // descendants remain in the original one, and shutdown must reach both.
        libc::kill(-pid, signal);
        libc::kill(pid, signal);
    }
    #[cfg(not(unix))]
    let _ = child.kill();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serve_http_once(status: &str, body: &str) -> (String, std::thread::JoinHandle<String>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let status = status.to_owned();
        let body = body.to_owned();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
            String::from_utf8(request).unwrap()
        });
        (format!("http://{address}"), handle)
    }

    fn options(balance: &str) -> SupervisorConfig {
        SupervisorConfig {
            connection: "queen".into(),
            consumer_group: "workers".into(),
            queues: vec!["high".into(), "default".into()],
            balance: balance.into(),
            strategy: "size".into(),
            processes: 6,
            min_processes: 2,
            max_processes: 10,
            target_jobs_per_process: 10,
            target_clear_seconds: 60.0,
            default_runtime_seconds: 1.0,
            balance_cooldown: 3,
            balance_max_shift: 1,
            retry_after: 90,
            lease_renewal: false,
            scale_down_delay: 10,
            restart_backoff: 1,
            restart_backoff_max: 8,
            stable_after: 60,
            sleep: 1,
            timeout: 60,
            tries: 3,
            memory: 128,
            backoff: 0,
            max_jobs: 0,
            max_time: 0,
            rest: 0,
            force: false,
            quiet: true,
            min_processes_per_queue: 0,
            fast_scale_up: false,
        }
    }

    #[test]
    fn a_woken_or_climbing_pool_is_due_before_the_next_poll_once_a_second() {
        let config = config(options("auto"));
        let pools = Pools::new();
        let now = Instant::now();
        let keys = HashSet::from([watch::key("queen", "default")]);
        let woken = woken_pools(&config, &keys);
        let none = HashSet::new();
        let fresh = HashMap::from([("default".to_owned(), now)]);

        assert_eq!(woken, HashSet::from(["default".to_owned()]));
        assert_eq!(
            event_due_pools(&config, &pools, &woken, &none, &HashMap::new(), now),
            woken
        );
        assert!(event_due_pools(&config, &pools, &woken, &none, &fresh, now).is_empty());
        assert!(event_due_pools(&config, &pools, &none, &none, &HashMap::new(), now).is_empty());
        let climbing = HashSet::from(["default".to_owned()]);
        assert_eq!(
            event_due_pools(&config, &pools, &none, &climbing, &HashMap::new(), now).len(),
            1
        );
        let other = HashSet::from([watch::key("other", "default")]);
        assert!(woken_pools(&config, &other).is_empty());
        let fixed = self::config(options("simple"));
        assert!(woken_pools(&fixed, &keys).is_empty());
    }

    #[test]
    fn a_grow_only_evaluation_never_takes_a_worker_from_a_queue() {
        let options = options("auto");
        let current = HashMap::from([("high".to_owned(), 4), ("default".to_owned(), 0)]);

        // The backlog moved from high to default: grow default by what the
        // target adds overall, and keep high until the cooldown.
        let raw = HashMap::from([("high".to_owned(), 0), ("default".to_owned(), 5)]);
        assert_eq!(
            grow_only_target(&options, &raw, &current),
            Some(HashMap::from([
                ("high".to_owned(), 4),
                ("default".to_owned(), 1)
            ]))
        );
        let raw = HashMap::from([("high".to_owned(), 6), ("default".to_owned(), 3)]);
        assert_eq!(
            grow_only_target(&options, &raw, &current),
            Some(HashMap::from([
                ("high".to_owned(), 6),
                ("default".to_owned(), 3)
            ]))
        );
        let raw = HashMap::from([("high".to_owned(), 1), ("default".to_owned(), 3)]);
        assert_eq!(grow_only_target(&options, &raw, &current), None);
    }

    fn config(options: SupervisorConfig) -> Config {
        Config {
            version: CONFIG_VERSION,
            cwd: "/app".into(),
            php_binary: "/usr/bin/php".into(),
            artisan: "/app/artisan".into(),
            state_directory: "/tmp/queen-supervisor-test".into(),
            poll_interval: 3,
            http_timeout: 5,
            shutdown_grace: 75,
            telemetry_ttl: 300,
            control_ttl: 3_600,
            heartbeat_timeout: 3_600,
            process_limit: 256,
            queen: QueenConfig {
                url: "http://127.0.0.1:6632".into(),
                urls: vec!["http://127.0.0.1:6632".into()],
                bearer_token: None,
                headers: HashMap::new(),
            },
            connections: HashMap::new(),
            supervisors: HashMap::from([("default".into(), options)]),
            remote_status: None,
            coordination: None,
            prefork: false,
            event_driven: None,
            lease_service: true,
            lease_socket: None,
            exit_markers: None,
        }
    }

    #[cfg(unix)]
    fn exited_worker(exit_code: i32, restart_probe: bool) -> (Worker, ExitStatus) {
        let mut child = Command::new("/bin/sh")
            .args(["-c", &format!("exit {exit_code}")])
            .spawn()
            .unwrap();
        let status = child.wait().unwrap();
        (Worker::new(child, restart_probe), status)
    }

    #[cfg(unix)]
    fn sleeping_worker(restart_probe: bool) -> Worker {
        let child = Command::new("/bin/sh")
            .args(["-c", "exec sleep 30"])
            .spawn()
            .unwrap();
        Worker::new(child, restart_probe)
    }

    fn temporary_directory(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "queen-supervisor-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        #[cfg(unix)]
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        path
    }

    fn write_telemetry(
        directory: &Path,
        file: &str,
        supervisor: &str,
        connection: &str,
        group: &str,
        samples: u64,
        ewma: f64,
    ) {
        let path = directory.join(file);
        write_private_file(
            &path,
            &serde_json::to_vec(&serde_json::json!({
                "pid": 123,
                "updated_at_epoch": now_epoch(),
                "supervisor": supervisor,
                "connection": connection,
                "consumer_group": group,
                "queues": {
                    "high": {
                        "samples": samples,
                        "runtime_ewma_seconds": ewma,
                        "failures": 0
                    }
                }
            }))
            .unwrap(),
        );
    }

    fn write_private_file(path: &Path, contents: &[u8]) {
        fs::write(path, contents).unwrap();
        #[cfg(unix)]
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    fn read_web_telemetry(directory: &Path) -> HashMap<String, f64> {
        read_runtimes(
            directory,
            60,
            TelemetryScope {
                supervisor: "web",
                connection: "queen",
                consumer_group: "workers",
            },
        )
    }

    #[test]
    fn auto_prefers_the_busy_queue() {
        let got = desired(
            &options("auto"),
            &HashMap::from([("high".into(), 95), ("default".into(), 5)]),
            &HashMap::new(),
        );
        assert_eq!(got.values().sum::<usize>(), 10);
        assert!(got["high"] > got["default"]);
    }

    #[test]
    fn auto_assigns_every_queue_with_positive_pressure() {
        let mut options = options("auto");
        options.queues = vec!["high".into(), "default".into(), "low".into()];
        options.processes = 3;
        options.min_processes = 1;
        options.max_processes = 3;
        let got = desired(
            &options,
            &HashMap::from([("high".into(), 1), ("default".into(), 1), ("low".into(), 1)]),
            &HashMap::new(),
        );
        assert_eq!(got.values().sum::<usize>(), 3);
        assert!(options.queues.iter().all(|queue| got[queue] == 1));
    }

    #[test]
    fn startup_depth_outage_keeps_every_auto_queue_reachable() {
        let mut options = options("auto");
        options.queues = vec!["high".into(), "default".into(), "low".into()];
        options.processes = 3;
        options.min_processes = 1;
        options.max_processes = 3;
        let got = fail_open_desired(&options);
        assert_eq!(got.values().sum::<usize>(), 3);
        assert!(options.queues.iter().all(|queue| got[queue] == 1));
    }

    #[test]
    fn draining_workers_consume_the_global_process_limit() {
        assert_eq!(remaining_process_slots(10, 9), 1);
        assert_eq!(remaining_process_slots(10, 10), 0);
        assert_eq!(remaining_process_slots(10, 12), 0);
    }

    #[test]
    fn simple_is_even() {
        let got = desired(&options("simple"), &HashMap::new(), &HashMap::new());
        assert_eq!(got["high"], 3);
        assert_eq!(got["default"], 3);
    }

    #[test]
    fn reconcile_budget_immediately_establishes_baseline_capacity() {
        let simple = options("simple");
        assert_eq!(reconcile_budget(&simple, 0, 0), 6);
        assert_eq!(reconcile_budget(&simple, 3, 0), 3);
        assert_eq!(reconcile_budget(&simple, 6, 0), 1);

        let auto = options("auto");
        assert_eq!(reconcile_budget(&auto, 0, 0), 2);
        assert_eq!(reconcile_budget(&auto, 2, 0), 1);
    }

    #[test]
    fn ordered_queue_workers_disable_blocking_reserves() {
        let resolved = config(options("off"));
        let command = worker_command(
            &resolved,
            "default",
            "high",
            &resolved.supervisors["default"],
        );
        let block_for = command
            .get_envs()
            .find(|(name, _)| *name == std::ffi::OsStr::new("QUEEN_LARAVEL_BLOCK_FOR"))
            .and_then(|(_, value)| value);
        assert_eq!(block_for, Some(std::ffi::OsStr::new("0")));
        assert!(command
            .get_args()
            .any(|argument| argument == std::ffi::OsStr::new("--queue=high,default")));

        let auto_config = config(options("auto"));
        let auto_command = worker_command(
            &auto_config,
            "default",
            "high",
            &auto_config.supervisors["default"],
        );
        assert!(auto_command
            .get_envs()
            .all(|(name, _)| name != std::ffi::OsStr::new("QUEEN_LARAVEL_BLOCK_FOR")));
    }

    #[test]
    fn worker_telemetry_is_only_enabled_for_time_strategy() {
        let size_config = config(options("simple"));
        let size_command = worker_command(
            &size_config,
            "default",
            "high",
            &size_config.supervisors["default"],
        );
        assert!(size_command.get_envs().any(|(name, value)| {
            name == std::ffi::OsStr::new("QUEEN_SUPERVISOR_TELEMETRY_DIR") && value.is_none()
        }));

        let mut time_options = options("auto");
        time_options.strategy = "time".into();
        let time_config = config(time_options);
        let time_command = worker_command(
            &time_config,
            "default",
            "high",
            &time_config.supervisors["default"],
        );
        let telemetry = time_command
            .get_envs()
            .find(|(name, _)| *name == std::ffi::OsStr::new("QUEEN_SUPERVISOR_TELEMETRY_DIR"))
            .and_then(|(_, value)| value);
        let expected = Path::new(&time_config.state_directory).join("telemetry");
        assert_eq!(telemetry, Some(expected.as_os_str()));

        let mut fixed_time_options = options("simple");
        fixed_time_options.strategy = "time".into();
        let fixed_time_config = config(fixed_time_options);
        let fixed_time_command = worker_command(
            &fixed_time_config,
            "default",
            "high",
            &fixed_time_config.supervisors["default"],
        );
        assert!(fixed_time_command.get_envs().any(|(name, value)| {
            name == std::ffi::OsStr::new("QUEEN_SUPERVISOR_TELEMETRY_DIR") && value.is_none()
        }));
    }

    #[test]
    fn only_workers_that_renew_leases_get_the_lease_socket() {
        let socket = |renewal: bool, served: bool| {
            let mut options = options("auto");
            options.lease_renewal = renewal;
            let mut config = config(options);
            config.lease_socket = served.then(|| "/state/lease.sock".to_owned());
            let (_, environment) =
                worker_invocation(&config, "default", "high", &config.supervisors["default"]);
            environment
                .into_iter()
                .find(|(name, _)| name == "QUEEN_SUPERVISOR_LEASE_SOCKET")
                .expect("the variable is always set or removed")
                .1
        };
        assert_eq!(socket(true, true).as_deref(), Some("/state/lease.sock"));
        // None removes a value inherited from the master's environment.
        assert_eq!(socket(false, true), None);
        assert_eq!(socket(true, false), None);
    }

    #[test]
    fn time_strategy_uses_runtime_pressure() {
        let mut options = options("auto");
        options.strategy = "time".into();
        let got = desired(
            &options,
            &HashMap::from([("high".into(), 30), ("default".into(), 30)]),
            &HashMap::from([("high".into(), 10.0), ("default".into(), 2.0)]),
        );
        assert_eq!(got.values().sum::<usize>(), 6);
        assert!(got["high"] > got["default"]);

        let overflowing = desired(
            &options,
            &HashMap::from([("high".into(), usize::MAX), ("default".into(), 0)]),
            &HashMap::from([("high".into(), 1.0e308)]),
        );
        assert_eq!(overflowing.values().sum::<usize>(), options.max_processes);
    }

    #[test]
    fn telemetry_uses_scoped_sample_weighted_ewma() {
        let directory = temporary_directory("telemetry");
        write_telemetry(&directory, "1.json", "web", "queen", "workers", 2, 4.0);
        write_telemetry(&directory, "2.json", "web", "queen", "workers", 200, 10.0);
        write_telemetry(
            &directory, "3.json", "other", "queen", "workers", 100, 1_000.0,
        );
        write_telemetry(
            &directory,
            "4.json",
            "web",
            "secondary",
            "workers",
            100,
            1_000.0,
        );
        write_telemetry(
            &directory,
            "5.json",
            "web",
            "queen",
            "another-group",
            100,
            1_000.0,
        );

        let runtimes = read_runtimes(
            &directory,
            60,
            TelemetryScope {
                supervisor: "web",
                connection: "queen",
                consumer_group: "workers",
            },
        );

        let expected = (4.0 * 2.0 + 10.0 * 100.0) / 102.0;
        assert!((runtimes["high"] - expected).abs() < 1e-12);
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn telemetry_requires_private_real_directories_and_files() {
        let directory = temporary_directory("telemetry-permissions");
        let path = directory.join("worker.json");
        write_telemetry(&directory, "worker.json", "web", "queen", "workers", 2, 4.0);
        assert_eq!(read_web_telemetry(&directory)["high"], 4.0);

        fs::set_permissions(&directory, fs::Permissions::from_mode(0o750)).unwrap();
        assert!(read_web_telemetry(&directory).is_empty());
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();

        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(read_web_telemetry(&directory).is_empty());
        fs::remove_file(&path).unwrap();

        write_telemetry(&directory, "worker.data", "web", "queen", "workers", 2, 4.0);
        std::os::unix::fs::symlink(directory.join("worker.data"), &path).unwrap();
        assert!(read_web_telemetry(&directory).is_empty());

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn telemetry_bounds_and_validates_each_queue_document() {
        let directory = temporary_directory("telemetry-queue-bound");
        let queues = (0..=MAX_TELEMETRY_QUEUES_PER_FILE)
            .map(|index| {
                (
                    format!("queue-{index}"),
                    serde_json::json!({
                        "samples": 1,
                        "runtime_ewma_seconds": 1.0,
                        "failures": 0
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let oversized = serde_json::json!({
            "pid": 123,
            "updated_at_epoch": now_epoch(),
            "supervisor": "web",
            "connection": "queen",
            "consumer_group": "workers",
            "queues": queues
        });
        write_private_file(
            &directory.join("worker.json"),
            &serde_json::to_vec(&oversized).unwrap(),
        );
        assert!(read_web_telemetry(&directory).is_empty());

        let validated = serde_json::json!({
            "pid": 123,
            "updated_at_epoch": now_epoch(),
            "supervisor": "web",
            "connection": "queen",
            "consumer_group": "workers",
            "queues": {
                "high": {"samples": 2, "runtime_ewma_seconds": 4.0, "failures": 0},
                "bad\nqueue": {"samples": 100, "runtime_ewma_seconds": 999.0, "failures": 0},
                "comma,queue": {"samples": 100, "runtime_ewma_seconds": 999.0, "failures": 0}
            }
        });
        write_private_file(
            &directory.join("worker.json"),
            &serde_json::to_vec(&validated).unwrap(),
        );
        assert_eq!(
            read_web_telemetry(&directory),
            HashMap::from([("high".into(), 4.0)])
        );

        let overflowing = serde_json::json!({
            "pid": 123,
            "updated_at_epoch": now_epoch(),
            "supervisor": "web",
            "connection": "queen",
            "consumer_group": "workers",
            "queues": {
                "high": {"samples": 100, "runtime_ewma_seconds": 1.0e308, "failures": 0}
            }
        });
        write_private_file(
            &directory.join("worker.json"),
            &serde_json::to_vec(&overflowing).unwrap(),
        );
        assert!(read_web_telemetry(&directory).is_empty());

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn telemetry_prunes_file_churn_and_recovers_after_directory_overflow() {
        let file_directory = temporary_directory("telemetry-file-cap");
        for index in 0..(MAX_TELEMETRY_FILES - 1) {
            write_private_file(&file_directory.join(format!("{index:04}.json")), b"{}");
        }
        write_telemetry(
            &file_directory,
            "zzzz.json",
            "web",
            "queen",
            "workers",
            1,
            4.0,
        );
        assert_eq!(read_web_telemetry(&file_directory)["high"], 4.0);
        write_private_file(&file_directory.join("overflow.json"), b"{}");
        assert_eq!(read_web_telemetry(&file_directory)["high"], 4.0);
        assert!(fs::read_dir(&file_directory).unwrap().count() <= MAX_TELEMETRY_FILES);
        fs::remove_dir_all(file_directory).unwrap();

        let entry_directory = temporary_directory("telemetry-entry-cap");
        for index in 0..MAX_TELEMETRY_DIRECTORY_ENTRIES {
            write_private_file(&entry_directory.join(format!("{index:04}.json")), b"{}");
        }
        write_telemetry(
            &entry_directory,
            "latest.json",
            "web",
            "queen",
            "workers",
            1,
            4.0,
        );
        assert!(read_web_telemetry(&entry_directory).is_empty());
        assert_eq!(read_web_telemetry(&entry_directory)["high"], 4.0);
        assert!(fs::read_dir(&entry_directory).unwrap().count() <= MAX_TELEMETRY_FILES);
        fs::remove_dir_all(entry_directory).unwrap();
    }

    #[test]
    fn telemetry_fails_closed_above_the_aggregate_byte_cap() {
        let directory = temporary_directory("telemetry-byte-cap");
        let document = serde_json::to_vec(&serde_json::json!({
            "pid": 123,
            "updated_at_epoch": now_epoch(),
            "supervisor": "web",
            "connection": "queen",
            "consumer_group": "workers",
            "queues": {
                "high": {"samples": 1, "runtime_ewma_seconds": 4.0, "failures": 0}
            }
        }))
        .unwrap();
        let mut padded = document;
        padded.resize(MAX_TELEMETRY_BYTES as usize, b' ');
        for index in 0..(MAX_TELEMETRY_TOTAL_BYTES / MAX_TELEMETRY_BYTES) {
            write_private_file(&directory.join(format!("{index:03}.json")), &padded);
        }
        assert_eq!(read_web_telemetry(&directory)["high"], 4.0);
        write_private_file(&directory.join("overflow.json"), &padded);
        assert_eq!(read_web_telemetry(&directory)["high"], 4.0);
        assert!(fs::read_dir(&directory).unwrap().count() <= 256);

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn supervisor_only_reads_runtime_telemetry_for_time_strategy() {
        let state_directory = temporary_directory("telemetry-strategy");
        let telemetry_directory = state_directory.join("telemetry");
        fs::create_dir(&telemetry_directory).unwrap();
        #[cfg(unix)]
        fs::set_permissions(&telemetry_directory, fs::Permissions::from_mode(0o700)).unwrap();
        write_telemetry(
            &telemetry_directory,
            "worker.json",
            "default",
            "queen",
            "workers",
            2,
            4.0,
        );

        let mut size_config = config(options("simple"));
        size_config.state_directory = state_directory.to_string_lossy().into_owned();
        let mut pending = PendingTelemetryCleanup::new();
        assert!(supervisor_runtimes(
            &size_config,
            "default",
            &size_config.supervisors["default"],
            &mut pending,
        )
        .is_empty());

        let mut time_options = options("auto");
        time_options.strategy = "time".into();
        let mut time_config = config(time_options);
        time_config.state_directory = state_directory.to_string_lossy().into_owned();
        let runtimes = supervisor_runtimes(
            &time_config,
            "default",
            &time_config.supervisors["default"],
            &mut pending,
        );
        assert_eq!(runtimes, HashMap::from([("high".into(), 4.0)]));

        fs::remove_dir_all(state_directory).unwrap();
    }

    #[test]
    fn time_telemetry_preserves_a_final_sample_until_one_scan() {
        let state_directory = temporary_directory("telemetry-final-sample");
        let telemetry_directory = state_directory.join("telemetry");
        fs::create_dir(&telemetry_directory).unwrap();
        #[cfg(unix)]
        fs::set_permissions(&telemetry_directory, fs::Permissions::from_mode(0o700)).unwrap();

        let mut time_options = options("auto");
        time_options.strategy = "time".into();
        let mut resolved = config(time_options);
        resolved.state_directory = state_directory.to_string_lossy().into_owned();
        write_telemetry(
            &telemetry_directory,
            "12345.json",
            "default",
            "queen",
            "workers",
            1,
            4.0,
        );
        let mut pending = PendingTelemetryCleanup::new();
        schedule_telemetry_cleanup(&resolved, "default", 12345, &mut pending);
        assert!(telemetry_directory.join("12345.json").exists());
        let runtimes = supervisor_runtimes(
            &resolved,
            "default",
            &resolved.supervisors["default"],
            &mut pending,
        );
        assert_eq!(runtimes, HashMap::from([("high".into(), 4.0)]));
        assert!(!telemetry_directory.join("12345.json").exists());

        let mut simple = config(options("simple"));
        simple.state_directory = state_directory.to_string_lossy().into_owned();
        write_telemetry(
            &telemetry_directory,
            "12346.json",
            "default",
            "queen",
            "workers",
            1,
            8.0,
        );
        schedule_telemetry_cleanup(&simple, "default", 12346, &mut pending);
        assert!(!telemetry_directory.join("12346.json").exists());

        fs::remove_dir_all(state_directory).unwrap();
    }

    #[test]
    fn downscale_requires_a_stable_low_demand_window() {
        let options = options("auto");
        let high = HashMap::from([("high".into(), 100), ("default".into(), 0)]);
        let low = HashMap::from([("high".into(), 1), ("default".into(), 0)]);
        let previous = desired(&options, &high, &HashMap::new());
        let raw = desired(&options, &low, &HashMap::new());
        let now = Instant::now();
        let mut guard = ScaleGuard::default();

        let held = stabilize_desired(&options, raw.clone(), Some(&previous), &mut guard, now);
        assert_eq!(held.values().sum::<usize>(), 10);

        let still_held = stabilize_desired(
            &options,
            raw.clone(),
            Some(&held),
            &mut guard,
            now + Duration::from_secs(9),
        );
        assert_eq!(still_held.values().sum::<usize>(), 10);

        let released = stabilize_desired(
            &options,
            raw,
            Some(&still_held),
            &mut guard,
            now + Duration::from_secs(10),
        );
        assert_eq!(released.values().sum::<usize>(), 2);
        assert_eq!(guard.candidate_total, Some(2));

        let partially_drained = HashMap::from([("high".into(), 8), ("default".into(), 1)]);
        let continued = stabilize_desired(
            &options,
            desired(&options, &low, &HashMap::new()),
            Some(&partially_drained),
            &mut guard,
            now + Duration::from_secs(11),
        );
        assert_eq!(continued.values().sum::<usize>(), 2);
    }

    #[test]
    fn demand_rebound_cancels_pending_downscale_immediately() {
        let options = options("auto");
        let high = HashMap::from([("high".into(), 100), ("default".into(), 0)]);
        let low = HashMap::from([("high".into(), 1), ("default".into(), 0)]);
        let previous = desired(&options, &high, &HashMap::new());
        let now = Instant::now();
        let mut guard = ScaleGuard::default();
        let raw_low = desired(&options, &low, &HashMap::new());
        let _ = stabilize_desired(&options, raw_low, Some(&previous), &mut guard, now);
        assert!(guard.downscale_since.is_some());

        let raw_high = desired(&options, &high, &HashMap::new());
        let rebounded = stabilize_desired(
            &options,
            raw_high,
            Some(&previous),
            &mut guard,
            now + Duration::from_secs(1),
        );
        assert_eq!(rebounded.values().sum::<usize>(), 10);
        assert!(guard.downscale_since.is_none());
    }

    #[test]
    fn a_changed_downscale_target_restarts_the_stability_window() {
        let options = options("auto");
        let previous = HashMap::from([("high".into(), 9), ("default".into(), 1)]);
        let target_two = HashMap::from([("high".into(), 2), ("default".into(), 0)]);
        let target_five = HashMap::from([("high".into(), 5), ("default".into(), 0)]);
        let now = Instant::now();
        let mut guard = ScaleGuard::default();

        let _ = stabilize_desired(&options, target_two, Some(&previous), &mut guard, now);
        let changed = stabilize_desired(
            &options,
            target_five.clone(),
            Some(&previous),
            &mut guard,
            now + Duration::from_secs(9),
        );
        assert_eq!(changed, previous);
        assert_eq!(guard.candidate_total, Some(5));

        let still_held = stabilize_desired(
            &options,
            target_five,
            Some(&changed),
            &mut guard,
            now + Duration::from_secs(10),
        );
        assert_eq!(still_held.values().sum::<usize>(), 10);
    }

    #[test]
    fn restart_guard_backs_off_then_opens_and_half_opens() {
        let options = options("auto");
        let now = Instant::now();
        let mut guard = RestartGuard::default();

        for expected in [1, 2, 4, 8] {
            let (delay, opened) = guard.record_failure(&options, now);
            assert_eq!(delay, Duration::from_secs(expected));
            assert!(!opened);
        }
        let (delay, opened) = guard.record_failure(&options, now);
        assert_eq!(delay, Duration::from_secs(8));
        assert!(opened);
        assert_eq!(guard.state_name(), "open");
        assert_eq!(
            guard.spawn_permission(now + Duration::from_secs(7)),
            SpawnPermission::Blocked
        );
        assert_eq!(
            guard.spawn_permission(now + Duration::from_secs(8)),
            SpawnPermission::Probe
        );
        guard.mark_spawned(SpawnPermission::Probe);
        assert_eq!(guard.spawn_permission(now), SpawnPermission::Blocked);
        guard.record_healthy();
        assert_eq!(guard.spawn_permission(now), SpawnPermission::Normal);
        assert_eq!(guard.consecutive_failures, 0);
    }

    #[cfg(unix)]
    #[test]
    fn active_restart_probe_ignores_non_probe_sibling_exits() {
        let options = options("auto");
        let key = ("default".to_owned(), "high".to_owned());
        let mut guard = RestartGuard {
            consecutive_failures: CRASH_CIRCUIT_THRESHOLD,
            phase: RestartPhase::Probe,
        };

        let (successful_sibling, success) = exited_worker(0, false);
        record_worker_exit(
            &key,
            &successful_sibling,
            success,
            None,
            &options,
            &mut guard,
        );
        assert_eq!(guard.state_name(), "probe");
        assert_eq!(guard.consecutive_failures, CRASH_CIRCUIT_THRESHOLD);

        let (failed_sibling, failure) = exited_worker(1, false);
        record_worker_exit(&key, &failed_sibling, failure, None, &options, &mut guard);
        assert_eq!(guard.state_name(), "probe");
        assert_eq!(guard.consecutive_failures, CRASH_CIRCUIT_THRESHOLD);
        assert_eq!(
            guard.spawn_permission(Instant::now() + Duration::from_secs(3_600)),
            SpawnPermission::Blocked
        );
    }

    #[cfg(unix)]
    #[test]
    fn exact_restart_probe_exit_controls_the_circuit() {
        let options = options("auto");
        let key = ("default".to_owned(), "high".to_owned());
        let mut failed_guard = RestartGuard {
            consecutive_failures: CRASH_CIRCUIT_THRESHOLD,
            phase: RestartPhase::Probe,
        };
        let (mut failed_probe, failure) = exited_worker(1, true);
        failed_probe.started_at = Instant::now() - Duration::from_secs(options.stable_after + 1);

        record_worker_exit(
            &key,
            &failed_probe,
            failure,
            None,
            &options,
            &mut failed_guard,
        );
        assert_eq!(failed_guard.state_name(), "open");
        assert_eq!(
            failed_guard.consecutive_failures,
            CRASH_CIRCUIT_THRESHOLD + 1
        );

        let mut successful_guard = RestartGuard {
            consecutive_failures: CRASH_CIRCUIT_THRESHOLD,
            phase: RestartPhase::Probe,
        };
        let (successful_probe, success) = exited_worker(0, true);
        record_worker_exit(
            &key,
            &successful_probe,
            success,
            None,
            &options,
            &mut successful_guard,
        );
        assert_eq!(successful_guard.state_name(), "closed");
        assert_eq!(successful_guard.consecutive_failures, 0);
    }

    /// queue:work exits 12 at --memory, and a job timeout kills the worker:
    /// routine for a worker that ran for a while, and no reason to hold its
    /// pool at one probe for stable_after.
    #[cfg(unix)]
    #[test]
    fn a_long_lived_workers_exit_is_not_a_crash() {
        let options = options("auto");
        let key = ("default".to_owned(), "high".to_owned());
        let Some(long_ago) =
            Instant::now().checked_sub(Duration::from_secs(options.stable_after + 1))
        else {
            // A host up for less than a minute cannot date such a worker.
            return;
        };
        let (memory_limit, exited_12) = exited_worker(12, false);
        let mut killed_child = Command::new("/bin/sh")
            .args(["-c", "kill -9 $$"])
            .spawn()
            .unwrap();
        let killed_status = killed_child.wait().unwrap();
        let timed_out = Worker::new(killed_child, false);

        for (mut worker, status) in [(memory_limit, exited_12), (timed_out, killed_status)] {
            worker.started_at = long_ago;
            let mut guard = RestartGuard::default();
            record_worker_exit(&key, &worker, status, None, &options, &mut guard);
            assert_eq!(
                guard.state_name(),
                "closed",
                "{status} after a minute throttled the pool"
            );
            assert_eq!(
                guard.spawn_permission(Instant::now()),
                SpawnPermission::Normal
            );
        }

        let (failed_at_once, status) = exited_worker(1, false);
        let mut guard = RestartGuard::default();
        record_worker_exit(&key, &failed_at_once, status, None, &options, &mut guard);
        assert_eq!(guard.state_name(), "backoff");
    }

    /// A configuration whose exit-marker directory is ready, as run() makes it.
    fn exit_marker_config(label: &str) -> (Config, PathBuf) {
        let state_directory = temporary_directory(label);
        let mut resolved = config(options("auto"));
        resolved.state_directory = state_directory.to_string_lossy().into_owned();
        let exits = prepare_exit_markers(&state_directory).unwrap();
        resolved.exit_markers = Some(exits.to_string_lossy().into_owned());
        (resolved, state_directory)
    }

    /// Reap until the pool's worker has exited and been classified.
    #[cfg(unix)]
    fn reap_exit(config: &Config, pools: &mut Pools, restarts: &mut RestartStates, key: &PoolKey) {
        let mut pending = PendingTelemetryCleanup::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !pools[key].is_empty() {
            assert!(Instant::now() < deadline, "the worker never exited");
            reap(config, pools, restarts, &mut pending, None);
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn entries(directory: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    /// `queue:restart` stops a worker with status 0; the marker explains
    /// nothing else.
    #[cfg(unix)]
    #[test]
    fn a_queue_restart_is_announced_only_for_a_clean_exit() {
        let (resolved, state_directory) = exit_marker_config("queue-restart");
        let exits = state_directory.join("exits");

        let (clean, status) = exited_worker(0, false);
        write_private_file(&exits.join(clean.child.id().to_string()), b"restart");
        assert_eq!(
            announced_exit(&resolved, clean.child.id(), status),
            Some(AnnouncedExit::QueueRestart)
        );

        let (failed, status) = exited_worker(1, false);
        write_private_file(&exits.join(failed.child.id().to_string()), b"restart");
        assert_eq!(announced_exit(&resolved, failed.child.id(), status), None);
        assert!(entries(&exits).is_empty());
        fs::remove_dir_all(state_directory).unwrap();
    }

    /// After queue:restart the master boots a new fork server, and closes the
    /// old one with the last worker it forked.
    #[cfg(unix)]
    #[test]
    fn a_queue_restart_replaces_the_fork_server_and_retires_the_old_one() {
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../clients/client-php/tests/Fixtures/Prefork/fork_server.php");
        let autoload =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../clients/client-php/vendor/autoload.php");
        let php = Command::new("php")
            .args([
                "-r",
                "exit(function_exists('pcntl_fork') && function_exists('posix_setsid') ? 0 : 1);",
            ])
            .status()
            .is_ok_and(|status| status.success());
        if !php || !fixture.exists() || !autoload.exists() {
            eprintln!("skipped: PHP with pcntl/posix and the Laravel package are required");
            return;
        }
        let (mut resolved, state_directory) = exit_marker_config("fork-server-restart");
        resolved.prefork = true;
        resolved.php_binary = "php".to_owned();
        resolved.artisan = fixture.to_string_lossy().into_owned();
        resolved.cwd = fixture.parent().unwrap().to_string_lossy().into_owned();
        let exits = state_directory.join("exits");
        let report = state_directory
            .join("report")
            .to_string_lossy()
            .into_owned();
        let running = AtomicBool::new(true);
        let first = Rc::new(RefCell::new(
            ForkServer::start(&resolved, &running).unwrap(),
        ));
        let fork = |server: &Rc<RefCell<ForkServer>>, mode: &str| {
            let argv = vec![mode.to_owned(), report.clone(), "0".to_owned()];
            let pid = server.borrow_mut().fork(&argv, &[]).unwrap();
            Worker::from_process(
                WorkerProcess::Forked {
                    pid,
                    server: Rc::clone(server),
                    status: None,
                },
                false,
            )
        };
        let restarted = fork(&first, "exit");
        write_private_file(&exits.join(restarted.child.id().to_string()), b"restart");
        let busy = fork(&first, "sleep");
        let busy_pid = busy.child.id();
        let key = ("default".to_owned(), "high".to_owned());
        let mut pools = Pools::from([(key.clone(), vec![restarted, busy])]);
        let mut restarts = RestartStates::new();
        let mut pending = PendingTelemetryCleanup::new();

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut announced = false;
        while pools[&key].len() > 1 {
            assert!(
                Instant::now() < deadline,
                "the restarted worker never exited"
            );
            announced |= reap(
                &resolved,
                &mut pools,
                &mut restarts,
                &mut pending,
                Some(&first),
            );
            thread::sleep(Duration::from_millis(10));
        }
        assert!(announced);

        let old = Rc::downgrade(&first);
        let mut retired = Vec::new();
        let second = refresh_fork_server(&resolved, &running, Some(first), &mut retired).unwrap();
        close_retired_forks(&mut retired);
        assert_eq!(retired.len(), 1, "the old server stays open for its worker");
        pools.get_mut(&key).unwrap().push(fork(&second, "sleep"));

        // SAFETY: plain signal delivery to the worker this test forked.
        unsafe { libc::kill(busy_pid as i32, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(10);
        while pools[&key]
            .iter()
            .any(|worker| worker.child.id() == busy_pid)
        {
            assert!(Instant::now() < deadline, "the old worker never exited");
            assert!(!reap(
                &resolved,
                &mut pools,
                &mut restarts,
                &mut pending,
                Some(&second)
            ));
            thread::sleep(Duration::from_millis(10));
        }
        close_retired_forks(&mut retired);

        assert!(retired.is_empty());
        assert!(
            old.upgrade().is_none(),
            "the old server was closed and dropped"
        );
        pools.clear();
        second.borrow_mut().close(Duration::from_secs(5));
        fs::remove_dir_all(state_directory).unwrap();
    }

    /// Laravel SIGKILLs a worker whose job outlives its timeout, which the
    /// worker announces first in exits/<pid>.
    #[cfg(unix)]
    #[test]
    fn a_worker_killed_after_its_job_timeout_restarts_without_backoff() {
        let (resolved, state_directory) = exit_marker_config("job-timeout");
        let exits = state_directory.join("exits");
        let key = ("default".to_owned(), "high".to_owned());
        let mut restarts = RestartStates::new();

        let timed_out = sleeping_worker(false);
        let pid = timed_out.child.id();
        write_private_file(&exits.join(pid.to_string()), b"timeout");
        // SAFETY: plain signal delivery to the child this test spawned.
        unsafe { libc::kill(pid as i32, libc::SIGKILL) };
        let mut pools = Pools::from([(key.clone(), vec![timed_out])]);
        reap_exit(&resolved, &mut pools, &mut restarts, &key);

        assert_eq!(restarts[&key].state_name(), "closed");
        assert_eq!(restarts[&key].consecutive_failures, 0);
        assert!(entries(&exits).is_empty());

        // The OOM killer, a lease fence or kill -9 leave no marker.
        let killed = sleeping_worker(false);
        // SAFETY: as above.
        unsafe { libc::kill(killed.child.id() as i32, libc::SIGKILL) };
        pools.insert(key.clone(), vec![killed]);
        reap_exit(&resolved, &mut pools, &mut restarts, &key);

        assert_eq!(restarts[&key].state_name(), "backoff");
        assert_eq!(restarts[&key].consecutive_failures, 1);
        fs::remove_dir_all(state_directory).unwrap();
    }

    /// queue:work exits 12 at --memory after an idle sleep too: only a worker
    /// that handled a job announces it, and only that one is not a crash.
    #[cfg(unix)]
    #[test]
    fn a_worker_stopped_at_its_memory_limit_after_a_job_restarts_like_a_clean_exit() {
        let (resolved, state_directory) = exit_marker_config("memory-limit");
        let exits = state_directory.join("exits");
        let options = options("auto");
        let key = ("default".to_owned(), "high".to_owned());
        let mut guard = RestartGuard::default();

        let (worker, status) = exited_worker(12, false);
        write_private_file(&exits.join(worker.child.id().to_string()), b"memory");
        let announced = announced_exit(&resolved, worker.child.id(), status);
        assert_eq!(announced, Some(AnnouncedExit::MemoryLimit));
        record_worker_exit(&key, &worker, status, announced, &options, &mut guard);
        assert_eq!(guard.state_name(), "closed");
        assert_eq!(guard.consecutive_failures, 0);
        assert!(entries(&exits).is_empty());

        // No marker: no job ran before the limit, so the boot footprint is
        // above --memory and every restart stops the same way.
        let (idle, status) = exited_worker(12, false);
        let announced = announced_exit(&resolved, idle.child.id(), status);
        assert_eq!(announced, None);
        record_worker_exit(&key, &idle, status, announced, &options, &mut guard);
        assert_eq!(guard.state_name(), "backoff");

        // A memory marker explains exit 12 alone.
        let (failed, status) = exited_worker(1, false);
        write_private_file(&exits.join(failed.child.id().to_string()), b"memory");
        assert_eq!(announced_exit(&resolved, failed.child.id(), status), None);
        fs::remove_dir_all(state_directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn an_exit_marker_only_explains_a_sigkill() {
        let (resolved, state_directory) = exit_marker_config("marker-without-kill");
        let exits = state_directory.join("exits");
        let (failed, status) = exited_worker(1, false);
        let pid = failed.child.id();
        write_private_file(&exits.join(pid.to_string()), b"timeout");

        assert_eq!(announced_exit(&resolved, pid, status), None);
        assert!(entries(&exits).is_empty());

        let mut disabled = resolved;
        disabled.exit_markers = None;
        let mut child = Command::new("/bin/sh")
            .args(["-c", "kill -9 $$"])
            .spawn()
            .unwrap();
        let killed = child.wait().unwrap();
        write_private_file(&exits.join(child.id().to_string()), b"timeout");
        assert_eq!(announced_exit(&disabled, child.id(), killed), None);
        fs::remove_dir_all(state_directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_timed_out_probe_is_replaced_at_once_and_leaves_the_circuit_as_it_was() {
        let options = options("auto");
        let key = ("default".to_owned(), "high".to_owned());
        let mut child = Command::new("/bin/sh")
            .args(["-c", "kill -9 $$"])
            .spawn()
            .unwrap();
        let status = child.wait().unwrap();
        let probe = Worker::new(child, true);
        let mut guard = RestartGuard {
            consecutive_failures: CRASH_CIRCUIT_THRESHOLD,
            phase: RestartPhase::Probe,
        };

        record_worker_exit(
            &key,
            &probe,
            status,
            Some(AnnouncedExit::JobTimeout),
            &options,
            &mut guard,
        );

        assert_eq!(guard.state_name(), "open");
        assert_eq!(guard.consecutive_failures, CRASH_CIRCUIT_THRESHOLD);
        assert_eq!(
            guard.spawn_permission(Instant::now()),
            SpawnPermission::Probe
        );
    }

    #[cfg(unix)]
    #[test]
    fn exit_markers_are_read_once_and_only_from_a_private_regular_file() {
        let state_directory = temporary_directory("exit-markers");
        let exits = prepare_exit_markers(&state_directory).unwrap();
        let elsewhere = temporary_directory("exit-marker-target");
        let outside = elsewhere.join("target");
        write_private_file(&outside, b"timeout");

        std::os::unix::fs::symlink(&outside, exits.join("1")).unwrap();
        write_private_file(&exits.join("2"), &[b'x'; 65]);
        write_private_file(&exits.join("3"), b"timeout");
        fs::set_permissions(exits.join("3"), fs::Permissions::from_mode(0o644)).unwrap();
        write_private_file(&exits.join("4"), b"timeout\n");
        write_private_file(&exits.join("5.tmp"), b"timeout");
        fs::create_dir(exits.join("6")).unwrap();

        assert_eq!(take_exit_marker(&exits, 1), None);
        assert_eq!(fs::read(&outside).unwrap(), b"timeout");
        assert_eq!(take_exit_marker(&exits, 2), None);
        assert_eq!(take_exit_marker(&exits, 3), None);
        assert_eq!(take_exit_marker(&exits, 4).as_deref(), Some("timeout"));
        assert_eq!(take_exit_marker(&exits, 4), None);
        assert_eq!(take_exit_marker(&exits, 5), None);
        assert_eq!(take_exit_marker(&exits, 6), None);
        assert_eq!(entries(&exits), vec!["6".to_owned()]);

        write_private_file(&exits.join("7"), b"timeout");
        write_private_file(&exits.join("7.tmp"), b"timeout");
        remove_exit_marker(&exits, 7);
        assert_eq!(entries(&exits), vec!["6".to_owned()]);
        fs::remove_dir_all(state_directory).unwrap();
        fs::remove_dir_all(elsewhere).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn the_master_empties_the_exit_markers_and_passes_their_directory_to_workers() {
        let state_directory = temporary_directory("exit-marker-reset");
        let exits = state_directory.join("exits");
        fs::create_dir(&exits).unwrap();
        fs::set_permissions(&exits, fs::Permissions::from_mode(0o755)).unwrap();
        write_private_file(&exits.join("123"), b"timeout");
        write_private_file(&exits.join("123.tmp"), b"timeout");
        std::os::unix::fs::symlink("/etc/hosts", exits.join("456")).unwrap();

        assert_eq!(prepare_exit_markers(&state_directory).unwrap(), exits);
        assert!(entries(&exits).is_empty());
        assert_eq!(fs::symlink_metadata(&exits).unwrap().mode() & 0o7777, 0o700);
        assert!(Path::new("/etc/hosts").exists());

        let exits_variable = |config: &Config| {
            worker_invocation(config, "default", "high", &config.supervisors["default"])
                .1
                .into_iter()
                .find(|(name, _)| name == "QUEEN_SUPERVISOR_EXITS_DIR")
                .expect("the variable is always set or removed")
                .1
        };
        let mut resolved = config(options("auto"));
        // None removes a value inherited from the master's environment.
        assert_eq!(exits_variable(&resolved), None);
        resolved.exit_markers = Some(exits.to_string_lossy().into_owned());
        assert_eq!(
            exits_variable(&resolved).as_deref(),
            Some(exits.to_str().unwrap())
        );
        fs::remove_dir_all(state_directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn exit_markers_refuse_a_linked_directory() {
        let state_directory = temporary_directory("exit-marker-link");
        let elsewhere = temporary_directory("exit-marker-elsewhere");
        std::os::unix::fs::symlink(&elsewhere, state_directory.join("exits")).unwrap();
        write_private_file(&elsewhere.join("123"), b"timeout");

        assert!(prepare_exit_markers(&state_directory).is_err());
        assert_eq!(take_exit_marker(&state_directory.join("exits"), 123), None);
        assert!(elsewhere.join("123").exists());
        fs::remove_dir_all(state_directory).unwrap();
        fs::remove_dir_all(elsewhere).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn only_stable_exact_probe_closes_an_active_probe() {
        let resolved = config(options("auto"));
        let key = ("default".to_owned(), "high".to_owned());
        let stable_at = Instant::now() - Duration::from_secs(61);
        let mut sibling = sleeping_worker(false);
        sibling.started_at = stable_at;
        let probe = sleeping_worker(true);
        let mut pools = Pools::from([(key.clone(), vec![sibling, probe])]);
        let mut restarts = RestartStates::from([(
            key.clone(),
            RestartGuard {
                consecutive_failures: CRASH_CIRCUIT_THRESHOLD,
                phase: RestartPhase::Probe,
            },
        )]);

        observe_stable_workers(&resolved, &mut pools, &mut restarts);
        assert!(pools[&key][0].stability_reported);
        assert!(!pools[&key][1].stability_reported);
        assert_eq!(restarts[&key].state_name(), "probe");
        assert_eq!(restarts[&key].consecutive_failures, CRASH_CIRCUIT_THRESHOLD);

        pools.get_mut(&key).unwrap()[1].started_at = stable_at;
        observe_stable_workers(&resolved, &mut pools, &mut restarts);
        assert!(pools[&key][1].stability_reported);
        assert!(!pools[&key][1].restart_probe);
        assert_eq!(restarts[&key].state_name(), "closed");
        assert_eq!(restarts[&key].consecutive_failures, 0);

        for mut worker in pools.remove(&key).unwrap() {
            let _ = worker.child.kill();
            let _ = worker.child.wait();
        }
    }

    #[test]
    fn lease_service_is_on_unless_the_document_turns_it_off() {
        let mut document = serde_json::json!({
            "version": 2,
            "cwd": "/app",
            "php_binary": "/usr/bin/php",
            "artisan": "/app/artisan",
            "state_directory": "/tmp/queen-supervisor-test",
            "poll_interval": 3,
            "http_timeout": 5,
            "shutdown_grace": 75,
            "telemetry_ttl": 300,
            "process_limit": 256,
            "queen": {
                "url": "http://127.0.0.1:6632",
                "urls": ["http://127.0.0.1:6632"],
                "bearer_token": null,
                "headers": {}
            },
            "supervisors": {}
        });

        // Laravel emits the key only when false; earlier releases never did.
        let absent: Config = serde_json::from_value(document.clone()).unwrap();
        assert!(absent.lease_service);
        document["lease_service"] = serde_json::json!(false);
        let off: Config = serde_json::from_value(document.clone()).unwrap();
        assert!(!off.lease_service);
        document["lease_service"] = serde_json::json!(true);
        let on: Config = serde_json::from_value(document.clone()).unwrap();
        assert!(on.lease_service);
        // A switch is a JSON boolean, as Laravel validates it.
        document["lease_service"] = serde_json::json!("false");
        assert!(serde_json::from_value::<Config>(document).is_err());
    }

    #[test]
    fn lease_service_false_leaves_renewal_to_one_helper_per_worker() {
        let mut renewing = options("auto");
        renewing.lease_renewal = true;
        let mut resolved = config(renewing);
        assert!(serves_lease_renewal(&resolved, true));
        assert!(!serves_lease_renewal(&resolved, false), "Linux only");

        resolved.lease_service = false;
        assert!(!serves_lease_renewal(&resolved, true));
        let directory = temporary_directory("lease-service-off");
        assert_eq!(start_lease_service(&resolved, &directory), None);
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 0, "no socket");
        fs::remove_dir_all(&directory).unwrap();

        // Without a pool that renews leases there is nothing to serve.
        resolved.lease_service = true;
        for options in resolved.supervisors.values_mut() {
            options.lease_renewal = false;
        }
        assert!(!serves_lease_renewal(&resolved, true));
    }

    #[test]
    fn legacy_v2_documents_receive_safe_policy_defaults() {
        let document = serde_json::json!({
            "version": 2,
            "cwd": "/app",
            "php_binary": "/usr/bin/php",
            "artisan": "/app/artisan",
            "state_directory": "/tmp/queen",
            "poll_interval": 3,
            "http_timeout": 5,
            "shutdown_grace": 75,
            "telemetry_ttl": 300,
            "process_limit": 256,
            "queen": {
                "url": "http://127.0.0.1:6632",
                "urls": ["http://127.0.0.1:6632"],
                "bearer_token": null,
                "headers": {}
            },
            "supervisors": {
                "default": {
                    "connection": "queen",
                    "consumer_group": "workers",
                    "queues": ["default"],
                    "balance": "auto",
                    "strategy": "size",
                    "processes": 2,
                    "min_processes": 1,
                    "max_processes": 2,
                    "target_jobs_per_process": 10,
                    "target_clear_seconds": 60.0,
                    "default_runtime_seconds": 1.0,
                    "balance_cooldown": 3,
                    "balance_max_shift": 1,
                    "lease_renewal": true,
                    "sleep": 1,
                    "timeout": 60,
                    "tries": 3,
                    "memory": 128,
                    "backoff": 0,
                    "max_jobs": 0,
                    "max_time": 0,
                    "rest": 0,
                    "force": false
                }
            }
        });

        let config: Config = serde_json::from_value(document).unwrap();
        let supervisor = &config.supervisors["default"];
        assert_eq!(config.control_ttl, 3_600);
        assert_eq!(config.heartbeat_timeout, 3_600);
        assert_eq!(supervisor.retry_after, 90);
        assert!(supervisor.lease_renewal);
        assert_eq!(supervisor.scale_down_delay, 10);
        assert_eq!(supervisor.restart_backoff, 1);
        assert_eq!(supervisor.restart_backoff_max, 30);
        assert_eq!(supervisor.stable_after, 60);
        assert!(supervisor.quiet);
        validate_config(&config).unwrap();
    }

    #[test]
    fn header_maps_accept_objects_and_only_empty_legacy_arrays() {
        let object: QueenConfig = serde_json::from_value(serde_json::json!({
            "headers": {"x-tenant": "orders"}
        }))
        .unwrap();
        assert_eq!(object.headers["x-tenant"], "orders");

        let legacy: QueenConfig =
            serde_json::from_value(serde_json::json!({"headers": []})).unwrap();
        assert!(legacy.headers.is_empty());

        let invalid = serde_json::from_value::<QueenConfig>(serde_json::json!({
            "headers": ["not-a-map"]
        }));
        assert!(invalid.is_err());
    }

    #[test]
    fn validation_rejects_unsafe_worker_and_queue_configuration() {
        let mut invalid_retry = config(options("auto"));
        invalid_retry
            .supervisors
            .get_mut("default")
            .unwrap()
            .retry_after = 60;
        assert!(validate_config(&invalid_retry).is_err());

        let mut duplicate_queue = config(options("auto"));
        duplicate_queue
            .supervisors
            .get_mut("default")
            .unwrap()
            .queues = vec!["high".into(), "high".into()];
        assert!(validate_config(&duplicate_queue).is_err());

        let mut excessive_limit = config(options("auto"));
        excessive_limit.process_limit = MAX_PROCESS_LIMIT + 1;
        assert!(validate_config(&excessive_limit).is_err());

        let mut renewal_budget = config(options("auto"));
        renewal_budget.process_limit = 10;
        renewal_budget
            .supervisors
            .get_mut("default")
            .unwrap()
            .lease_renewal = true;
        assert!(validate_config(&renewal_budget).is_err());

        let mut uncovered_queue = config(options("auto"));
        uncovered_queue
            .supervisors
            .get_mut("default")
            .unwrap()
            .max_processes = 1;
        assert!(validate_config(&uncovered_queue).is_err());

        let mut simple_uncovered = config(options("simple"));
        simple_uncovered
            .supervisors
            .get_mut("default")
            .unwrap()
            .processes = 1;
        assert!(validate_config(&simple_uncovered).is_err());

        let mut excessive_shift = config(options("auto"));
        let excessive_shift_options = excessive_shift.supervisors.get_mut("default").unwrap();
        excessive_shift_options.balance_max_shift = excessive_shift_options.max_processes + 1;
        assert!(validate_config(&excessive_shift).is_err());

        let mut root_state = config(options("auto"));
        root_state.state_directory = "/".into();
        assert!(validate_config(&root_state).is_err());

        let mut relative_state = config(options("auto"));
        relative_state.state_directory = "storage/queen-supervisor".into();
        assert!(validate_config(&relative_state).is_err());

        let mut parent_state = config(options("auto"));
        parent_state.state_directory = "/tmp/queen/../state".into();
        assert!(validate_config(&parent_state).is_err());

        let mut short_control_ttl = config(options("auto"));
        short_control_ttl.control_ttl = MIN_CONTROL_TTL_SECONDS - 1;
        assert!(validate_config(&short_control_ttl).is_err());

        let mut long_control_ttl = config(options("auto"));
        long_control_ttl.control_ttl = MAX_CONTROL_TTL_SECONDS + 1;
        assert!(validate_config(&long_control_ttl).is_err());

        let mut zero_heartbeat_timeout = config(options("auto"));
        zero_heartbeat_timeout.heartbeat_timeout = 0;
        assert!(validate_config(&zero_heartbeat_timeout).is_err());

        let mut long_heartbeat_timeout = config(options("auto"));
        long_heartbeat_timeout.heartbeat_timeout = MAX_CONTROL_TTL_SECONDS + 1;
        assert!(validate_config(&long_heartbeat_timeout).is_err());

        let mut tiny_scaling_window = config(options("auto"));
        tiny_scaling_window
            .supervisors
            .get_mut("default")
            .unwrap()
            .target_clear_seconds = MIN_SCALING_SECONDS / 2.0;
        assert!(validate_config(&tiny_scaling_window).is_err());

        let mut excessive_runtime = config(options("auto"));
        excessive_runtime
            .supervisors
            .get_mut("default")
            .unwrap()
            .default_runtime_seconds = MAX_DURATION_SECONDS as f64 + 1.0;
        assert!(validate_config(&excessive_runtime).is_err());
    }

    #[test]
    fn control_liveness_windows_strictly_exceed_the_conservative_loop_budget() {
        let mut supervisor = options("auto");
        supervisor.strategy = "time".into();
        supervisor.queues = (0..17).map(|index| format!("queue-{index}")).collect();
        supervisor.max_processes = 17;
        let mut resolved = config(supervisor);
        resolved.queen.urls = vec![
            "http://127.0.0.1:6632".into(),
            "http://127.0.0.1:6633".into(),
        ];

        // poll 3 + ceil(17/16) * 2 endpoints * timeout 5
        // + 17 process starts * 5 + time telemetry 60 + margin 5.
        let budget = control_loop_budget(&resolved).unwrap();
        assert_eq!(budget, 173);

        resolved.control_ttl = budget;
        let error = validate_config(&resolved).unwrap_err().to_string();
        assert!(error.contains("control_ttl"));

        resolved.control_ttl = budget + 1;
        resolved.heartbeat_timeout = budget;
        let error = validate_config(&resolved).unwrap_err().to_string();
        assert!(error.contains("heartbeat_timeout"));

        resolved.heartbeat_timeout = budget + 1;
        validate_config(&resolved).unwrap();
    }

    fn share(
        options: &SupervisorConfig,
        depths: &[(&str, usize)],
        replica: usize,
        replicas: usize,
    ) -> HashMap<String, usize> {
        let depths = depths
            .iter()
            .map(|(queue, depth)| ((*queue).to_owned(), *depth))
            .collect();
        desired_share(options, &depths, &HashMap::new(), replica, replicas)
    }

    fn total(allocation: &HashMap<String, usize>) -> usize {
        allocation.values().sum()
    }

    // The same cases as the Laravel package's ReplicaScalingTest: replicas of
    // both engines must split a fleet target identically.
    #[test]
    fn replicas_split_the_fleet_target_like_the_laravel_package() {
        let auto = options("auto");
        let busy = [("high", 95), ("default", 5)];
        assert_eq!(total(&share(&auto, &busy, 0, 2)), 5);
        assert_eq!(total(&share(&auto, &busy, 1, 2)), 5);
        assert_eq!(total(&share(&auto, &busy, 0, 1)), 10);

        let mut no_minimum = options("auto");
        no_minimum.min_processes = 0;
        let shares: Vec<usize> = (0..3)
            .map(|rank| {
                total(&share(
                    &no_minimum,
                    &[("high", 70), ("default", 0)],
                    rank,
                    3,
                ))
            })
            .collect();
        assert_eq!(shares, vec![3, 2, 2]);

        assert_eq!(
            total(&share(&auto, &[("high", 10), ("default", 0)], 1, 2)),
            2
        );
        assert_eq!(
            total(&share(&auto, &[("high", 400), ("default", 0)], 0, 2)),
            10
        );
        for rank in 0..3 {
            assert_eq!(
                total(&share(&auto, &[("high", 0), ("default", 0)], rank, 3)),
                2
            );
        }

        let simple = options("simple");
        assert_eq!(
            share(&simple, &[("high", 100), ("default", 0)], 1, 4),
            HashMap::from([("high".to_owned(), 3), ("default".to_owned(), 3)])
        );

        let mut off = options("off");
        off.min_processes = 0;
        assert_eq!(
            share(&off, &[("high", 30), ("default", 30)], 1, 2),
            HashMap::from([("high".to_owned(), 3), ("default".to_owned(), 0)])
        );
    }

    // The same cases as the Laravel package's FastScaleUpTest.
    #[test]
    fn a_fast_burst_closes_half_of_the_gap_every_cycle() {
        let mut fast = options("auto");
        fast.max_processes = 20;
        fast.min_processes = 1;
        assert_eq!(reconcile_budget(&fast, 1, 20), 1);
        fast.fast_scale_up = true;
        assert_eq!(reconcile_budget(&fast, 1, 20), 10);
        assert_eq!(reconcile_budget(&fast, 11, 20), 5);
        assert_eq!(reconcile_budget(&fast, 19, 20), 1);
        // Scaling down keeps the configured step.
        assert_eq!(reconcile_budget(&fast, 20, 4), 1);

        let mut cycles = 0;
        let mut active = 1;
        while active < 20 {
            active += reconcile_budget(&fast, active, 20).min(20 - active);
            cycles += 1;
        }
        assert_eq!(cycles, 5);

        let mut floor = options("auto");
        floor.min_processes = 0;
        floor.min_processes_per_queue = 2;
        assert_eq!(reconcile_budget(&floor, 0, 4), 4);
    }

    // The same cases as the Laravel package's PerQueueMinimumTest.
    #[test]
    fn every_queue_keeps_its_minimum_like_horizon() {
        let mut floor = options("auto");
        floor.min_processes = 0;
        floor.min_processes_per_queue = 2;
        let pair = |high: usize, default: usize| {
            HashMap::from([("high".to_owned(), high), ("default".to_owned(), default)])
        };

        assert_eq!(
            share(&floor, &[("high", 0), ("default", 0)], 0, 1),
            pair(2, 2)
        );
        assert_eq!(
            share(&floor, &[("high", 0), ("default", 0)], 1, 2),
            pair(2, 2)
        );
        let busy = share(&floor, &[("high", 95), ("default", 0)], 0, 1);
        assert_eq!(busy, pair(8, 2));
        assert_eq!(fail_open_desired(&floor), pair(2, 2));

        let mut simple = options("simple");
        simple.min_processes_per_queue = 1;
        let error = validate_config(&config(simple)).unwrap_err().to_string();
        assert!(error.contains("min_processes_per_queue"), "{error}");
        let mut too_many = options("auto");
        too_many.min_processes_per_queue = 6;
        let error = validate_config(&config(too_many)).unwrap_err().to_string();
        assert!(error.contains("min_processes_per_queue"), "{error}");
    }

    #[test]
    fn replicas_cover_different_queues_when_the_target_is_small() {
        let mut four = options("auto");
        four.queues = vec!["a".into(), "b".into(), "c".into(), "d".into()];
        four.min_processes = 0;
        let depths = [("a", 1), ("b", 1), ("c", 1), ("d", 1)];
        let expected = |counts: [usize; 4]| {
            ["a", "b", "c", "d"]
                .iter()
                .zip(counts)
                .map(|(queue, count)| ((*queue).to_owned(), count))
                .collect::<HashMap<_, _>>()
        };

        assert_eq!(share(&four, &depths, 0, 2), expected([1, 1, 0, 0]));
        assert_eq!(share(&four, &depths, 1, 2), expected([0, 0, 1, 1]));
    }

    #[test]
    fn every_queue_with_backlog_is_covered_when_the_maximum_cuts_the_share() {
        let mut five = options("auto");
        five.queues = (0..5).map(|index| format!("q{index}")).collect();
        five.min_processes = 0;
        five.max_processes = 3;
        let depths = |depth: usize| -> Vec<(String, usize)> {
            five.queues
                .iter()
                .map(|queue| (queue.clone(), depth))
                .collect()
        };
        let fleet = |depth: usize| -> HashMap<String, usize> {
            let depths = depths(depth);
            let depths: Vec<(&str, usize)> = depths
                .iter()
                .map(|(queue, depth)| (queue.as_str(), *depth))
                .collect();
            let mut fleet = HashMap::new();
            for rank in 0..3 {
                for (queue, workers) in share(&five, &depths, rank, 3) {
                    *fleet.entry(queue).or_insert(0) += workers;
                }
            }
            fleet
        };

        let busy = fleet(60);
        assert_eq!(busy.values().sum::<usize>(), 9);
        assert!(busy.values().all(|workers| *workers > 0), "{busy:?}");
        // Coverage does not move when the backlog grows.
        let covered = |fleet: &HashMap<String, usize>| {
            let mut queues: Vec<String> = fleet
                .iter()
                .filter(|(_, workers)| **workers > 0)
                .map(|(queue, _)| queue.clone())
                .collect();
            queues.sort();
            queues
        };
        assert_eq!(covered(&busy), covered(&fleet(61)));

        let mut small = five;
        small.max_processes = 10;
        let depths: Vec<(&str, usize)> = small
            .queues
            .iter()
            .map(|queue| (queue.as_str(), 1))
            .collect();
        let mut fleet = HashMap::new();
        for rank in 0..3 {
            for (queue, workers) in share(&small, &depths, rank, 3) {
                *fleet.entry(queue).or_insert(0) += workers;
            }
        }
        assert!(fleet.values().all(|workers| *workers > 0), "{fleet:?}");
    }

    #[test]
    fn the_laravel_coordination_contract_parses_and_joins_the_loop_budget() {
        let mut document: serde_json::Value = serde_json::from_str(
            r#"{
                "version": 2,
                "cwd": "/app",
                "php_binary": "/usr/bin/php",
                "artisan": "/app/artisan",
                "state_directory": "/tmp/queen-supervisor-test",
                "poll_interval": 3,
                "http_timeout": 5,
                "control_ttl": 3600,
                "heartbeat_timeout": 34,
                "shutdown_grace": 75,
                "process_limit": 256,
                "telemetry_ttl": 300,
                "queen": {"url": "http://127.0.0.1:6632", "urls": ["http://127.0.0.1:6632"], "bearer_token": null, "headers": {}},
                "connections": {
                    "queen": {"url": "http://127.0.0.1:6632", "urls": ["http://127.0.0.1:6632"], "bearer_token": null, "headers": {}}
                },
                "supervisors": {
                    "default": {
                        "connection": "queen", "consumer_group": "workers", "queues": ["default"],
                        "balance": "off", "strategy": "size", "processes": 1, "min_processes": 1,
                        "max_processes": 1, "target_jobs_per_process": 10, "target_clear_seconds": 60,
                        "default_runtime_seconds": 1, "balance_cooldown": 3, "balance_max_shift": 1,
                        "scale_down_delay": 10, "restart_backoff": 1, "restart_backoff_max": 30,
                        "stable_after": 60, "sleep": 3, "timeout": 60, "retry_after": 90,
                        "lease_renewal": false, "tries": 3, "memory": 128, "backoff": 0,
                        "max_jobs": 0, "max_time": 0, "rest": 0, "force": false, "quiet": true
                    }
                },
                "coordination": {
                    "connection": {
                        "url": "http://127.0.0.1:6632",
                        "urls": ["http://127.0.0.1:6632", "http://127.0.0.1:6633"],
                        "bearer_token": "write-secret",
                        "headers": {"X-Queen-Key": "header-secret"}
                    },
                    "namespace": "queen-supervisor",
                    "ttl": 34
                }
            }"#,
        )
        .unwrap();
        let mut resolved: Config = serde_json::from_value(document.clone()).unwrap();
        let coordination = resolved.coordination.as_ref().unwrap();
        assert_eq!(
            coordination.connection.bearer_token.as_deref(),
            Some("write-secret")
        );
        assert_eq!(
            coordinated_scopes(&resolved),
            HashMap::from([(
                "default".to_owned(),
                coordination::scope(
                    &["http://127.0.0.1:6632"],
                    "workers",
                    &["default".to_owned()]
                )
            )])
        );

        // poll 3 + one depth batch * 1 endpoint * timeout 5 + 1 process start * 5
        // + margin 5 + one coordination call per endpoint (2) * timeout 5.
        assert_eq!(control_loop_budget(&resolved).unwrap(), 28);
        validate_config(&resolved).unwrap();

        resolved.coordination.as_mut().unwrap().ttl = 28;
        let error = validate_config(&resolved).unwrap_err().to_string();
        assert!(error.contains("coordination ttl"), "{error}");

        // A fixed pool does not coordinate, so it costs no call.
        document["supervisors"]["default"]["balance"] = serde_json::json!("simple");
        let fixed: Config = serde_json::from_value(document.clone()).unwrap();
        assert!(coordinated_scopes(&fixed).is_empty());
        assert_eq!(control_loop_budget(&fixed).unwrap(), 18);

        document["coordination"]["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<Config>(document).is_err());
    }

    #[test]
    fn the_laravel_remote_status_contract_parses_and_joins_the_loop_budget() {
        let document: serde_json::Value = serde_json::from_str(
            r#"{
                "version": 2,
                "cwd": "/app",
                "php_binary": "/usr/bin/php",
                "artisan": "/app/artisan",
                "state_directory": "/tmp/queen-supervisor-test",
                "poll_interval": 3,
                "http_timeout": 5,
                "control_ttl": 3600,
                "heartbeat_timeout": 34,
                "shutdown_grace": 75,
                "process_limit": 256,
                "telemetry_ttl": 300,
                "queen": {"url": "http://127.0.0.1:6632", "urls": ["http://127.0.0.1:6632"], "bearer_token": null, "headers": {}},
                "connections": {
                    "queen": {"url": "http://127.0.0.1:6632", "urls": ["http://127.0.0.1:6632"], "bearer_token": null, "headers": {}}
                },
                "supervisors": {
                    "default": {
                        "connection": "queen", "consumer_group": "workers", "queues": ["default"],
                        "balance": "off", "strategy": "size", "processes": 1, "min_processes": 1,
                        "max_processes": 1, "target_jobs_per_process": 10, "target_clear_seconds": 60,
                        "default_runtime_seconds": 1, "balance_cooldown": 3, "balance_max_shift": 1,
                        "scale_down_delay": 10, "restart_backoff": 1, "restart_backoff_max": 30,
                        "stable_after": 60, "sleep": 3, "timeout": 60, "retry_after": 90,
                        "lease_renewal": false, "tries": 3, "memory": 128, "backoff": 0,
                        "max_jobs": 0, "max_time": 0, "rest": 0, "force": false, "quiet": true
                    }
                },
                "remote_status": {
                    "connection": {
                        "url": "http://127.0.0.1:6632",
                        "urls": ["http://127.0.0.1:6632", "http://127.0.0.1:6633"],
                        "bearer_token": "write-secret",
                        "headers": {"X-Queen-Key": "header-secret"}
                    },
                    "namespace": "queen-supervisor",
                    "key": "orders",
                    "interval": 3,
                    "ttl": 300
                }
            }"#,
        )
        .unwrap();
        let mut resolved: Config = serde_json::from_value(document.clone()).unwrap();
        let remote_status = resolved.remote_status.as_ref().unwrap();
        assert_eq!(
            remote_status.connection.bearer_token.as_deref(),
            Some("write-secret")
        );
        assert_eq!(remote_status.key, "orders");

        // poll 3 + one depth batch * 1 endpoint * timeout 5 + 1 process start * 5
        // + margin 5 + one publish per remote endpoint (2) * timeout 5.
        assert_eq!(control_loop_budget(&resolved).unwrap(), 28);
        validate_config(&resolved).unwrap();

        resolved.heartbeat_timeout = 28;
        let error = validate_config(&resolved).unwrap_err().to_string();
        assert!(error.contains("heartbeat_timeout"), "{error}");

        let mut unknown = document;
        unknown["remote_status"]["enabled"] = serde_json::json!(true);
        assert!(serde_json::from_value::<Config>(unknown).is_err());
    }

    #[test]
    fn validation_bounds_status_pool_and_identity_cardinality() {
        let queues = (0..MAX_STATUS_POOLS)
            .map(|index| format!("queue-{index}"))
            .collect::<Vec<_>>();
        let mut accepted = config(options("auto"));
        let supervisor = accepted.supervisors.get_mut("default").unwrap();
        supervisor.queues = queues;
        supervisor.max_processes = MAX_STATUS_POOLS;
        accepted.process_limit = MAX_STATUS_POOLS;
        validate_config(&accepted).unwrap();

        let supervisor = accepted.supervisors.get_mut("default").unwrap();
        supervisor.queues.push("one-pool-too-many".into());
        supervisor.max_processes = MAX_STATUS_POOLS + 1;
        accepted.process_limit = MAX_STATUS_POOLS + 1;
        let error = validate_config(&accepted).unwrap_err().to_string();
        assert!(error.contains("at most 256"));

        let mut identifier_boundary = config(options("auto"));
        let supervisor = identifier_boundary.supervisors.remove("default").unwrap();
        identifier_boundary
            .supervisors
            .insert("s".repeat(MAX_IDENTIFIER_BYTES), supervisor);
        validate_config(&identifier_boundary).unwrap();

        let mut identifier_too_long = config(options("auto"));
        let supervisor = identifier_too_long.supervisors.remove("default").unwrap();
        identifier_too_long
            .supervisors
            .insert("s".repeat(MAX_IDENTIFIER_BYTES + 1), supervisor);
        assert!(validate_config(&identifier_too_long).is_err());

        let mut queue_boundary = config(options("auto"));
        queue_boundary
            .supervisors
            .get_mut("default")
            .unwrap()
            .queues = vec!["q".repeat(MAX_QUEUE_BYTES)];
        validate_config(&queue_boundary).unwrap();

        let mut queue_too_long = config(options("auto"));
        queue_too_long
            .supervisors
            .get_mut("default")
            .unwrap()
            .queues = vec!["q".repeat(MAX_QUEUE_BYTES + 1)];
        assert!(validate_config(&queue_too_long).is_err());
    }

    #[test]
    fn supervisors_require_exported_v2_connections_with_only_queen_as_fallback() {
        let mut config = config(options("auto"));
        config.connections.insert(
            "secondary".into(),
            QueenConfig {
                url: "https://queen-secondary.internal/base".into(),
                urls: Vec::new(),
                bearer_token: Some("read-token".into()),
                headers: HashMap::from([("x-tenant".into(), "orders".into())]),
            },
        );
        config.supervisors.get_mut("default").unwrap().connection = "secondary".into();
        validate_config(&config).unwrap();
        assert_eq!(
            connection_config(&config, "secondary").unwrap().url,
            "https://queen-secondary.internal/base"
        );
        assert!(connection_config(&config, "not-exported").is_none());

        config.supervisors.get_mut("default").unwrap().connection = "not-exported".into();
        let error = validate_config(&config).unwrap_err().to_string();
        assert!(error.contains("missing from the resolved v2 contract"));

        config.supervisors.get_mut("default").unwrap().connection = "queen".into();
        assert!(connection_config(&config, "queen").is_some());
        validate_config(&config).unwrap();

        config.connections.get_mut("secondary").unwrap().url =
            "https://queen-secondary.internal?secret=in-url".into();
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn control_documents_reject_unknown_commands_and_fields() {
        assert!(serde_json::from_str::<Control>(r#"{"command":"restart","nonce":"1"}"#).is_err());
        assert!(serde_json::from_str::<Control>(r#"{"command":"pause","nonce":"1"}"#).is_err());
        assert!(
            serde_json::from_str::<Control>(r#"{"command":"pause","nonce":"1","extra":true}"#)
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn generation_publication_waits_for_the_control_lock() {
        let directory = temporary_directory("generation-control-lock");
        let held_control_lock = control_lock_for(&directory).unwrap();
        let path = directory.to_string_lossy().into_owned();
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (state_sender, state_receiver) = mpsc::sync_channel(1);
        let handle = thread::spawn(move || {
            started_sender.send(()).unwrap();
            let acquired = State::acquire(&path).map_err(|error| error.to_string());
            state_sender.send(acquired).unwrap();
        });

        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert!(matches!(
            state_receiver.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        drop(held_control_lock);
        let state = state_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        let owner: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(directory.join("supervisor.lock")).unwrap())
                .unwrap();
        assert_eq!(owner["instance_id"], state.instance_id);

        drop(state);
        handle.join().unwrap();
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn control_commands_are_scoped_to_the_running_instance() {
        let directory = temporary_directory("control");
        let state = State::acquire(directory.to_str().unwrap()).unwrap();
        let requested_at_epoch = now_epoch();
        atomic_json(
            &directory.join("control.json"),
            &serde_json::json!({
                "command": "terminate",
                "nonce": "request-1",
                "instance_id": "another-instance",
                "requested_at": iso8601_from_epoch(requested_at_epoch),
                "requested_at_epoch": requested_at_epoch,
                "expires_at_epoch": requested_at_epoch + 30
            }),
        )
        .unwrap();
        assert!(state.command(None).unwrap().is_none());
        assert!(!directory.join("control.json").exists());

        let requested_at_epoch = now_epoch();
        atomic_json(
            &directory.join("control.json"),
            &serde_json::json!({
                "command": "pause",
                "nonce": "request-2",
                "instance_id": &state.instance_id,
                "requested_at_epoch": requested_at_epoch,
                "expires_at_epoch": requested_at_epoch + 30
            }),
        )
        .unwrap();
        assert!(matches!(
            state.command(None).unwrap().unwrap().command,
            ControlCommand::Pause
        ));

        atomic_json(
            &directory.join("control.json"),
            &serde_json::json!({
                "command": "continue",
                "nonce": "request-expired",
                "instance_id": &state.instance_id,
                "requested_at_epoch": requested_at_epoch.saturating_sub(60),
                "expires_at_epoch": requested_at_epoch.saturating_sub(30)
            }),
        )
        .unwrap();
        assert!(state.command(None).is_err());
        assert!(!directory.join("control.json").exists());

        let resolved = config(options("auto"));
        let desired = HashMap::from([(
            "default".to_owned(),
            HashMap::from([("high".to_owned(), 3), ("default".to_owned(), 1)]),
        )]);
        let depths = HashMap::from([(
            "default".to_owned(),
            HashMap::from([("high".to_owned(), 9), ("default".to_owned(), 0)]),
        )]);
        let available = HashMap::from([("default".to_owned(), true)]);
        state
            .write_status(
                "rust",
                "running",
                StatusSnapshot {
                    config: &resolved,
                    pools: &Pools::new(),
                    restarts: &RestartStates::new(),
                    draining: &Draining::new(),
                    desired: &desired,
                    depths: &depths,
                    depths_available: &available,
                    replicas: &HashMap::from([("default".to_owned(), 2)]),
                },
            )
            .unwrap();
        let status: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(directory.join("status.json")).unwrap())
                .unwrap();
        assert_eq!(status["pool_status"][0]["replicas"], 2);
        assert_eq!(status["instance_id"], state.instance_id);
        // The dashboard tells published instances apart by host.
        let hostname = status["hostname"].as_str().unwrap();
        assert!(!hostname.is_empty() && !hostname.contains('\0'));
        assert_eq!(status["schema"], STATUS_SCHEMA);
        assert_eq!(status["paused"], false);
        assert_eq!(status["stopping"], false);
        assert_eq!(status["pool_status"][0]["supervisor"], "default");
        assert_eq!(status["pool_status"][0]["queue"], "high");
        assert_eq!(status["pool_status"][0]["desired"], 3);
        assert_eq!(status["pool_status"][0]["depth"], 9);
        assert_eq!(status["pool_status"][0]["depth_available"], true);
        assert_eq!(status["pool_status"][0]["ready"], false);
        assert_eq!(status["pool_status"][0]["capacity_satisfied"], false);
        assert_eq!(status["pool_status"][0]["process_cost_per_worker"], 1);
        assert_eq!(status["ready"], false);
        assert_eq!(status["capacity_satisfied"], false);
        assert_eq!(status["process_budget"]["used"], 0);
        assert_eq!(status["configuration"]["control_ttl"], 3_600);
        assert_eq!(status["configuration"]["heartbeat_timeout"], 3_600);
        assert_eq!(
            status["configuration"]["supervisors"][0]["connection"],
            "queen"
        );
        let updated_at_epoch = status["updated_at_epoch"].as_u64().unwrap();
        assert_eq!(
            status["updated_at"].as_str().unwrap(),
            iso8601_from_epoch(updated_at_epoch)
        );

        drop(state);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn status_configuration_is_sorted_allowlisted_and_secret_free() {
        let mut resolved = config(options("auto"));
        resolved.queen.bearer_token = Some("status-must-not-leak-this-token".into());
        resolved.queen.headers.insert(
            "authorization".into(),
            "status-must-not-leak-this-header".into(),
        );
        let zeta = resolved.supervisors.remove("default").unwrap();
        resolved.supervisors.insert("zeta".into(), zeta);
        resolved
            .supervisors
            .insert("alpha".into(), options("simple"));

        let configuration = status_configuration(&resolved);
        let mut keys = configuration
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "control_ttl",
                "heartbeat_timeout",
                "http_timeout",
                "poll_interval",
                "process_limit",
                "shutdown_grace",
                "supervisors",
                "telemetry_ttl",
            ]
        );
        assert_eq!(configuration["supervisors"][0]["name"], "alpha");
        assert_eq!(configuration["supervisors"][1]["name"], "zeta");
        let mut supervisor_keys = configuration["supervisors"][0]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        supervisor_keys.sort_unstable();
        assert_eq!(
            supervisor_keys,
            vec![
                "balance",
                "connection",
                "consumer_group",
                "lease_renewal",
                "max_processes",
                "memory",
                "min_processes",
                "name",
                "process_cost_per_worker",
                "processes",
                "queues",
                "retry_after",
                "strategy",
                "timeout",
                "tries",
            ]
        );
        let encoded = serde_json::to_string(&configuration).unwrap();
        assert!(!encoded.contains("status-must-not-leak-this-token"));
        assert!(!encoded.contains("status-must-not-leak-this-header"));
        assert!(!encoded.contains("127.0.0.1:6632"));
        assert!(!encoded.contains("php_binary"));
        assert!(!encoded.contains("state_directory"));
    }

    #[test]
    fn legacy_status_pools_preserve_colons_with_nested_identity() {
        let directory = temporary_directory("nested-pool-identity");
        let state = State::acquire(directory.to_str().unwrap()).unwrap();
        let mut resolved = config(options("auto"));
        resolved.supervisors.clear();
        let mut first = options("auto");
        first.queues = vec!["c".into()];
        first.processes = 1;
        first.min_processes = 1;
        first.max_processes = 1;
        let mut second = options("auto");
        second.queues = vec!["b:c".into()];
        second.processes = 1;
        second.min_processes = 1;
        second.max_processes = 1;
        resolved.supervisors.insert("a:b".into(), first);
        resolved.supervisors.insert("a".into(), second);
        validate_config(&resolved).unwrap();

        state
            .write_status(
                "rust",
                "running",
                StatusSnapshot {
                    config: &resolved,
                    pools: &Pools::new(),
                    restarts: &RestartStates::new(),
                    draining: &Draining::new(),
                    desired: &HashMap::new(),
                    depths: &HashMap::new(),
                    depths_available: &HashMap::new(),
                    replicas: &HashMap::new(),
                },
            )
            .unwrap();
        let status: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(directory.join("status.json")).unwrap())
                .unwrap();
        assert!(status["pools"]["a:b"]["c"].is_object());
        assert!(status["pools"]["a"]["b:c"].is_object());
        assert_eq!(status["pools"].as_object().unwrap().len(), 2);
        assert_eq!(status["pool_status"].as_array().unwrap().len(), 2);

        drop(state);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn maximum_identity_status_stays_below_the_one_mebibyte_contract() {
        let directory = temporary_directory("maximum-status");
        let state = State::acquire(directory.to_str().unwrap()).unwrap();
        let mut resolved = config(options("auto"));
        resolved.supervisors.clear();
        let connection = "c".repeat(MAX_IDENTIFIER_BYTES);
        resolved.connections.insert(
            connection.clone(),
            QueenConfig {
                url: "http://127.0.0.1:6632".into(),
                urls: Vec::new(),
                bearer_token: Some("maximum-status-secret".into()),
                headers: HashMap::new(),
            },
        );
        for index in 0..MAX_STATUS_POOLS {
            let mut supervisor = options("auto");
            supervisor.connection = connection.clone();
            supervisor.consumer_group = "g".repeat(MAX_IDENTIFIER_BYTES);
            supervisor.queues = vec![format!("{index:03}-{}", "q".repeat(MAX_QUEUE_BYTES - 4))];
            supervisor.processes = 1;
            supervisor.min_processes = 1;
            supervisor.max_processes = 1;
            resolved.supervisors.insert(
                format!("{index:03}-{}", "s".repeat(MAX_IDENTIFIER_BYTES - 4)),
                supervisor,
            );
        }
        resolved.process_limit = MAX_STATUS_POOLS;
        validate_config(&resolved).unwrap();

        state
            .write_status(
                "rust",
                "running",
                StatusSnapshot {
                    config: &resolved,
                    pools: &Pools::new(),
                    restarts: &RestartStates::new(),
                    draining: &Draining::new(),
                    desired: &HashMap::new(),
                    depths: &HashMap::new(),
                    depths_available: &HashMap::new(),
                    replicas: &HashMap::new(),
                },
            )
            .unwrap();
        let length = fs::metadata(directory.join("status.json")).unwrap().len();
        assert!(length < MAX_STATUS_BYTES, "status used {length} bytes");

        drop(state);
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn control_reader_rejects_symlinks_and_insecure_modes() {
        let directory = temporary_directory("unsafe-control");
        let state = State::acquire(directory.to_str().unwrap()).unwrap();
        let now = now_epoch();
        let document = serde_json::json!({
            "command": "pause",
            "nonce": "request-unsafe",
            "instance_id": &state.instance_id,
            "requested_at_epoch": now,
            "expires_at_epoch": now + 30
        });

        atomic_json(&directory.join("control.json"), &document).unwrap();
        fs::set_permissions(
            directory.join("control.json"),
            fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(state.command(None).is_err());
        assert!(!directory.join("control.json").exists());

        let target = directory.join("target.json");
        atomic_json(&target, &document).unwrap();
        std::os::unix::fs::symlink(&target, directory.join("control.json")).unwrap();
        assert!(state.command(None).is_err());
        assert!(!directory.join("control.json").exists());

        drop(state);
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn config_files_must_be_private_regular_and_not_symlinks() {
        let directory = temporary_directory("private-config");
        let path = directory.join("supervisor.json");
        fs::write(&path, "{}").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_private_config(&path).is_err());

        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(read_private_config(&path).is_err());

        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(read_private_config(&path).unwrap(), "{}");

        let link = directory.join("supervisor-link.json");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(read_private_config(&link).is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn existing_state_directories_must_already_be_private() {
        let parent = temporary_directory("state-mode");
        let existing = parent.join("existing");
        fs::create_dir(&existing).unwrap();
        fs::set_permissions(&existing, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(State::acquire(existing.to_str().unwrap()).is_err());
        assert_eq!(fs::metadata(&existing).unwrap().mode() & 0o7777, 0o755);

        let created = parent.join("created");
        let state = State::acquire(created.to_str().unwrap()).unwrap();
        assert_eq!(fs::metadata(&created).unwrap().mode() & 0o7777, 0o700);
        drop(state);

        fs::remove_dir_all(parent).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn state_acquisition_rejects_a_writable_non_sticky_ancestor_before_publishing_locks() {
        let root = temporary_directory("unsafe-state-ancestor");
        let unsafe_parent = root.join("shared");
        let state_directory = unsafe_parent.join("state");
        fs::create_dir(&unsafe_parent).unwrap();
        fs::set_permissions(&unsafe_parent, fs::Permissions::from_mode(0o777)).unwrap();

        let error = State::acquire(state_directory.to_str().unwrap())
            .err()
            .expect("writable non-sticky ancestor must be rejected")
            .to_string();
        assert!(error.contains("group/world-writable"));
        assert!(!state_directory.exists());
        assert!(!state_directory.join("control.lock").exists());
        assert!(!state_directory.join("supervisor.lock").exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn state_acquisition_accepts_a_private_child_below_a_trusted_sticky_ancestor() {
        let root = temporary_directory("sticky-state-ancestor");
        let sticky_parent = root.join("sticky");
        let state_directory = sticky_parent.join("state");
        fs::create_dir(&sticky_parent).unwrap();
        fs::set_permissions(&sticky_parent, fs::Permissions::from_mode(0o1777)).unwrap();

        let state = State::acquire(state_directory.to_str().unwrap()).unwrap();
        assert_eq!(
            fs::symlink_metadata(&state_directory).unwrap().mode() & 0o7777,
            0o700
        );
        assert!(state_directory.join("control.lock").exists());
        assert!(state_directory.join("supervisor.lock").exists());

        drop(state);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn acquired_generation_fails_closed_after_state_directory_replacement() {
        let state_directory = temporary_directory("replaced-state-generation");
        let previous_generation = state_directory.with_extension("previous");
        let state = State::acquire(state_directory.to_str().unwrap()).unwrap();
        fs::rename(&state_directory, &previous_generation).unwrap();
        fs::create_dir(&state_directory).unwrap();
        fs::set_permissions(&state_directory, fs::Permissions::from_mode(0o700)).unwrap();

        let error = state.command(None).unwrap_err().to_string();
        assert!(error.contains("changed after generation acquisition"));
        assert!(!state_directory.join("control.lock").exists());
        assert!(previous_generation.join("supervisor.lock").exists());

        drop(state);
        fs::remove_dir_all(state_directory).unwrap();
        fs::remove_dir_all(previous_generation).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn trusted_symlink_ancestors_are_canonicalized_once_for_the_generation() {
        let root = temporary_directory("canonical-state-ancestor");
        let actual_parent = root.join("actual");
        let alias = root.join("alias");
        fs::create_dir(&actual_parent).unwrap();
        fs::set_permissions(&actual_parent, fs::Permissions::from_mode(0o700)).unwrap();
        std::os::unix::fs::symlink(&actual_parent, &alias).unwrap();
        let requested = alias.join("state");

        let state = State::acquire(requested.to_str().unwrap()).unwrap();
        assert_eq!(
            state.directory,
            fs::canonicalize(&actual_parent).unwrap().join("state")
        );
        assert!(state.directory.join("supervisor.lock").exists());

        drop(state);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn symlink_target_chains_are_validated_before_canonicalization() {
        let root = temporary_directory("unsafe-symlink-target");
        let unsafe_parent = root.join("shared");
        let target = unsafe_parent.join("target");
        let alias = root.join("alias");
        fs::create_dir(&unsafe_parent).unwrap();
        fs::create_dir(&target).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&unsafe_parent, fs::Permissions::from_mode(0o777)).unwrap();
        std::os::unix::fs::symlink(&target, &alias).unwrap();

        let requested = alias.join("state");
        let error = State::acquire(requested.to_str().unwrap())
            .err()
            .expect("unsafe symlink target chain must be rejected")
            .to_string();
        assert!(error.contains("group/world-writable"));
        assert!(!target.join("state").exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn depth_payload_requires_numeric_pending_values() {
        assert!(serde_json::from_str::<DepthResponse>(r#"{"effectivePending":"12"}"#).is_err());
        let parsed: DepthResponse =
            serde_json::from_str(r#"{"effectivePending":12,"pending":20}"#).unwrap();
        assert_eq!(parsed.effective_pending, Some(12));
    }

    #[test]
    fn depth_polling_is_concurrent_bounded_and_returns_configured_order() {
        let queues = (0..(MAX_DEPTH_POLL_CONCURRENCY * 2))
            .map(|index| format!("queue-{index}"))
            .collect::<Vec<_>>();
        let running = AtomicBool::new(true);
        let active = AtomicUsize::new(0);
        let maximum = AtomicUsize::new(0);
        let calls = AtomicUsize::new(0);
        let first_wave = std::sync::Barrier::new(MAX_DEPTH_POLL_CONCURRENCY);

        let depths = poll_queue_depths(&queues, &running, MAX_DEPTH_POLL_CONCURRENCY, |queue| {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            let concurrent = active.fetch_add(1, Ordering::SeqCst) + 1;
            maximum.fetch_max(concurrent, Ordering::SeqCst);
            if call < MAX_DEPTH_POLL_CONCURRENCY {
                first_wave.wait();
            }
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(queue.trim_start_matches("queue-").parse::<usize>().unwrap())
        })
        .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), queues.len());
        assert_eq!(maximum.load(Ordering::SeqCst), MAX_DEPTH_POLL_CONCURRENCY);
        assert_eq!(
            depths,
            queues
                .iter()
                .enumerate()
                .map(|(index, queue)| (queue.clone(), index))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn depth_polling_does_not_start_requests_after_shutdown() {
        let queues = vec!["high".into(), "default".into()];
        let running = AtomicBool::new(false);
        let calls = AtomicUsize::new(0);

        let result = poll_queue_depths(&queues, &running, MAX_DEPTH_POLL_CONCURRENCY, |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        });

        assert_eq!(result, Err(DepthPollError::Interrupted));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn depth_http_request_is_scoped_authenticated_and_strict() {
        let (endpoint, request) =
            serve_http_once("200 OK", r#"{"effectivePending":12,"pending":20}"#);
        let queen = QueenConfig {
            url: endpoint,
            urls: Vec::new(),
            bearer_token: Some("read-secret".into()),
            headers: HashMap::from([("x-queen-test".into(), "yes".into())]),
        };
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        assert_eq!(
            queue_depth(&client, &queen, "high priority", "workers/eu").unwrap(),
            12
        );
        let request = request.join().unwrap().to_ascii_lowercase();
        assert!(request.starts_with(
            "get /api/v1/resources/queues/high%20priority/depth?group=workers%2feu http/1.1\r\n"
        ));
        assert!(request.contains("authorization: bearer read-secret\r\n"));
        assert!(request.contains("x-queen-test: yes\r\n"));
    }

    #[test]
    fn depth_http_distinguishes_missing_queues_from_missing_routes() {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let (missing_queue, request) =
            serve_http_once("404 Not Found", r#"{"code":"queue_not_found"}"#);
        let queen = QueenConfig {
            url: missing_queue,
            ..QueenConfig::default()
        };
        assert_eq!(
            queue_depth(&client, &queen, "default", "workers").unwrap(),
            0
        );
        request.join().unwrap();

        let (missing_route, request) =
            serve_http_once("404 Not Found", r#"{"code":"no_such_route"}"#);
        let queen = QueenConfig {
            url: missing_route,
            ..QueenConfig::default()
        };
        assert!(queue_depth(&client, &queen, "default", "workers").is_err());
        request.join().unwrap();
    }

    #[test]
    fn streamed_configuration_is_bounded_before_allocation() {
        assert_eq!(
            read_text_limited(std::io::Cursor::new(b"1234"), "test", 4).unwrap(),
            "1234"
        );
        assert!(read_text_limited(std::io::Cursor::new(b"12345"), "test", 4).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn artisan_configuration_export_has_a_hard_deadline() {
        let directory = temporary_directory("config-timeout");
        let script = directory.join("artisan");
        fs::write(&script, "exec sleep 5\n").unwrap();
        let options = CliOptions {
            config: None,
            php: "/bin/sh".into(),
            artisan: script.to_string_lossy().into_owned(),
        };

        let started = Instant::now();
        let error = export_artisan_config(&options, Duration::from_millis(50))
            .expect_err("a stuck Laravel bootstrap must time out");

        assert!(error.to_string().contains("exceeded"));
        assert!(started.elapsed() < Duration::from_secs(1));
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn graceful_drain_signals_only_the_worker_leader() {
        let directory = temporary_directory("leader-signal");
        let ready = directory.join("child-ready");
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg(
                r#"trap ':' TERM
(trap - TERM; : > "$1"; exec sleep 30) &
worker=$!
while kill -0 "$worker" 2>/dev/null; do wait "$worker"; done"#,
            )
            .arg("queen-worker-test")
            .arg(&ready);
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        let mut child = WorkerProcess::Spawned(command.spawn().unwrap());
        let deadline = Instant::now() + Duration::from_secs(2);
        while !ready.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists());

        signal_worker(&mut child, libc::SIGTERM);
        thread::sleep(Duration::from_millis(100));
        assert!(child.try_wait().unwrap().is_none());

        signal_process_group(&mut child, libc::SIGKILL);
        let _ = child.wait();
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "subprocess helper for linux_master_death_hard_fences_worker"]
    #[allow(
        clippy::zombie_processes,
        reason = "not waiting IS the fault under test: this helper writes the \
                  child's pid and exits, and the parent test asserts the child \
                  is fenced by PDEATHSIG when that happens"
    )]
    fn linux_pdeathsig_helper() {
        let Some(pid_file) = std::env::var_os("QUEEN_PDEATHSIG_PID_FILE") else {
            return;
        };
        let supervisor_pid = unsafe { libc::getpid() };
        let mut command = Command::new("/bin/sleep");
        command.arg("30");
        unsafe {
            command.pre_exec(move || {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::getppid() != supervisor_pid {
                    return Err(std::io::Error::other("test parent exited during spawn"));
                }
                Ok(())
            });
        }
        let child = command.spawn().unwrap();
        fs::write(pid_file, child.id().to_string()).unwrap();
        // Intentionally do not wait: exiting this test-process is the fault
        // being exercised by the parent test.
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_master_death_hard_fences_worker() {
        let directory = temporary_directory("pdeathsig");
        let pid_file = directory.join("pid");
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::linux_pdeathsig_helper",
                "--ignored",
                "--nocapture",
            ])
            .env("QUEEN_PDEATHSIG_PID_FILE", &pid_file)
            .status()
            .unwrap();
        assert!(status.success());

        let pid: u32 = fs::read_to_string(&pid_file).unwrap().parse().unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let state = fs::read_to_string(format!("/proc/{pid}/stat"))
                .ok()
                .and_then(|stat| stat.rsplit_once(") ").map(|(_, tail)| tail.to_owned()))
                .and_then(|tail| tail.chars().next());
            if state.is_none() || matches!(state, Some('Z' | 'X')) {
                break;
            }
            if Instant::now() >= deadline {
                unsafe {
                    libc::kill(-(pid as i32), libc::SIGKILL);
                    libc::kill(pid as i32, libc::SIGKILL);
                }
                panic!("worker {pid} survived its Rust master");
            }
            thread::sleep(Duration::from_millis(20));
        }

        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn pause_drains_all_prefetching_workers_instead_of_suspending_them() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "sleep 30"]);
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        let child = command.spawn().unwrap();
        let pool = ("default".to_owned(), "high".to_owned());
        let mut pools = Pools::from([(pool.clone(), vec![Worker::new(child, true)])]);
        let mut restart = RestartGuard::default();
        restart.mark_spawned(SpawnPermission::Probe);
        let mut restarts = RestartStates::from([(pool.clone(), restart)]);
        let mut draining = Draining::new();

        drain_all(
            &mut pools,
            &mut restarts,
            &mut draining,
            Duration::from_secs(30),
        );
        assert!(pools.values().all(Vec::is_empty));
        assert_eq!(draining.len(), 1);
        assert_eq!(
            restarts[&pool].spawn_permission(Instant::now()),
            SpawnPermission::Normal
        );

        signal_process_group(&mut draining[0].worker.child, libc::SIGKILL);
        let _ = draining[0].worker.child.wait();
    }

    #[cfg(unix)]
    #[test]
    fn forced_shutdown_observes_child_exit_before_clearing_tracking() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "trap '' TERM; exec sleep 30"]);
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        let child = command.spawn().unwrap();
        let mut pools = Pools::from([(
            ("default".into(), "high".into()),
            vec![Worker::new(child, false)],
        )]);
        let mut draining = Draining::new();
        let resolved = config(options("auto"));
        let mut pending = PendingTelemetryCleanup::new();

        shutdown(
            &resolved,
            &mut pools,
            &mut draining,
            Duration::ZERO,
            &mut pending,
            || {},
        );

        assert!(pools.values().all(Vec::is_empty));
        assert!(draining.is_empty());
    }

    /// A broker behind a private CA, as curl and so the PHP client accept it:
    /// from the platform store or SSL_CERT_FILE. Otherwise the lease renewal
    /// fails and fences every job that outlives its first lease.
    #[cfg(unix)]
    #[test]
    fn a_broker_certificate_from_a_private_ca_is_trusted() {
        let openssl = |args: &[&str], directory: &Path| {
            Command::new("openssl")
                .args(args)
                .current_dir(directory)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
        };
        let directory = temporary_directory("private-ca");
        if !openssl(&["version"], &directory) {
            // No openssl command line to stand up a TLS broker.
            let _ = fs::remove_dir_all(&directory);
            return;
        }
        fs::write(
            directory.join("leaf.ext"),
            "subjectAltName=DNS:localhost\nbasicConstraints=CA:FALSE\n\
             keyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n",
        )
        .unwrap();
        for step in [
            &[
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                "ca.key",
                "-out",
                "ca.pem",
                "-days",
                "2",
                "-subj",
                "/CN=queen-test-ca",
                "-addext",
                "basicConstraints=critical,CA:true",
                "-addext",
                "keyUsage=critical,keyCertSign",
            ][..],
            &[
                "req",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                "leaf.key",
                "-out",
                "leaf.csr",
                "-subj",
                "/CN=localhost",
            ],
            &[
                "x509",
                "-req",
                "-in",
                "leaf.csr",
                "-CA",
                "ca.pem",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-out",
                "leaf.pem",
                "-days",
                "2",
                "-extfile",
                "leaf.ext",
            ],
        ] {
            assert!(openssl(step, &directory), "openssl {step:?} failed");
        }
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let mut broker = Command::new("openssl")
            .args([
                "s_server", "-cert", "leaf.pem", "-key", "leaf.key", "-www", "-quiet",
            ])
            .args(["-accept", &format!("127.0.0.1:{port}")])
            .current_dir(&directory)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let started = Instant::now();
        while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "the TLS broker never listened"
            );
            thread::sleep(Duration::from_millis(20));
        }

        // The CA travels in SSL_CERT_FILE, for the helper process alone.
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::private_ca_helper",
                "--ignored",
                "--nocapture",
            ])
            .env("QUEEN_PRIVATE_CA_URL", format!("https://localhost:{port}/"))
            .env("SSL_CERT_FILE", directory.join("ca.pem"))
            .stdout(Stdio::null())
            .status()
            .unwrap();
        let _ = broker.kill();
        let _ = broker.wait();
        let _ = fs::remove_dir_all(&directory);
        assert!(
            status.success(),
            "the supervisor refused a broker certificate its CA bundle trusts"
        );
    }

    #[test]
    #[ignore = "subprocess helper for a_broker_certificate_from_a_private_ca_is_trusted"]
    fn private_ca_helper() {
        let Some(url) = std::env::var_os("QUEEN_PRIVATE_CA_URL") else {
            return;
        };
        // Built as every client of the supervisor is: its roots are the
        // crate's TLS features, not the call site.
        let client = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let response = client.get(url.to_str().unwrap()).send().unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
    }

    /// The broker calls of a stop (coordination leave, remote status) may
    /// take http_timeout per endpoint on a slow broker: they run only once
    /// every worker has its SIGTERM.
    #[cfg(unix)]
    #[test]
    fn shutdown_signals_every_worker_before_the_broker_calls() {
        let directory = temporary_directory("sigterm-first");
        let ready = directory.join("ready");
        let marker = directory.join("terminated");
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            "trap 'touch \"$MARKER\"; exit 0' TERM; touch \"$READY\"; while :; do sleep 0.05; done",
        ]);
        command.env("READY", &ready).env("MARKER", &marker);
        let child = command.spawn().unwrap();
        // A SIGTERM before the trap would end the shell without a trace.
        let started = Instant::now();
        while !ready.exists() {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "the worker never started"
            );
            thread::sleep(Duration::from_millis(10));
        }
        let mut pools = Pools::from([(
            ("default".into(), "high".into()),
            vec![Worker::new(child, false)],
        )]);
        let mut draining = Draining::new();
        let resolved = config(options("auto"));
        let mut pending = PendingTelemetryCleanup::new();
        let mut signalled_first = false;

        shutdown(
            &resolved,
            &mut pools,
            &mut draining,
            Duration::from_secs(5),
            &mut pending,
            || {
                let deadline = Instant::now() + Duration::from_secs(2);
                while !marker.exists() && Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(20));
                }
                signalled_first = marker.exists();
            },
        );

        assert!(
            signalled_first,
            "a broker call ran before the workers' SIGTERM"
        );
        assert!(pools.values().all(Vec::is_empty));
        let _ = fs::remove_dir_all(&directory);
    }

    #[cfg(unix)]
    #[test]
    fn scale_down_moves_workers_to_non_blocking_drain_tracking() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "sleep 30"]);
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        let child = command.spawn().unwrap();
        let mut draining = Draining::new();
        let resolved = config(options("auto"));
        let mut pending = PendingTelemetryCleanup::new();
        let started = Instant::now();
        begin_termination(
            Worker::new(child, false),
            Duration::from_secs(30),
            ("default".into(), "high".into()),
            &mut draining,
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(draining.len(), 1);

        draining[0].deadline = Instant::now();
        let kill_started = Instant::now();
        reap_draining(&resolved, &mut draining, &mut pending);
        assert!(kill_started.elapsed() < Duration::from_secs(1));
        assert_eq!(draining.len(), 1);

        let _ = draining[0].worker.child.wait();
        reap_draining(&resolved, &mut draining, &mut pending);
        assert!(draining.is_empty());
    }

    #[test]
    fn iso8601_timestamps_are_utc_and_deterministic() {
        assert_eq!(iso8601_from_epoch(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso8601_from_epoch(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(iso8601_from_epoch(1_776_422_096), "2026-04-17T10:34:56Z");
    }

    #[test]
    fn cli_help_version_and_configuration_are_explicit() {
        assert_eq!(parse_args(&["--help".into()]).unwrap(), CliAction::Help);
        assert_eq!(
            parse_args(&["--version".into()]).unwrap(),
            CliAction::Version
        );
        assert_eq!(
            parse_args(&[]).unwrap(),
            CliAction::Run(CliOptions {
                config: None,
                php: "php".into(),
                artisan: "artisan".into(),
            })
        );
        assert_eq!(
            parse_args(&[
                "--php".into(),
                "/usr/bin/php".into(),
                "--artisan".into(),
                "/srv/app/artisan".into(),
            ])
            .unwrap(),
            CliAction::Run(CliOptions {
                config: None,
                php: "/usr/bin/php".into(),
                artisan: "/srv/app/artisan".into(),
            })
        );
        assert!(parse_args(&["--unknown".into()]).is_err());
        assert!(parse_args(&["--config".into()]).is_err());
        assert!(parse_args(&[
            "--config".into(),
            "/run/queen.json".into(),
            "--php".into(),
            "php".into(),
        ])
        .is_err());
    }
}
