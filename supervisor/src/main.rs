use serde::Deserialize;
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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CONFIG_VERSION: u32 = 2;
const MAX_CONFIG_BYTES: u64 = 1_048_576;
const MAX_CONTROL_BYTES: u64 = 16_384;
const MAX_TELEMETRY_BYTES: u64 = 65_536;
const MAX_PROCESS_LIMIT: usize = 4_096;
const MAX_QUEUES_PER_SUPERVISOR: usize = 1_024;
const MAX_DURATION_SECONDS: u64 = 31_536_000;
const CONFIG_EXPORT_TIMEOUT_SECONDS: u64 = 60;
const CRASH_CIRCUIT_THRESHOLD: u32 = 5;
const MAX_DEPTH_POLL_CONCURRENCY: usize = 16;

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
    process_limit: usize,
    queen: QueenConfig,
    #[serde(default)]
    connections: HashMap<String, QueenConfig>,
    supervisors: HashMap<String, SupervisorConfig>,
}

#[derive(Debug, Deserialize, Default)]
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
    #[serde(default = "default_scale_down_delay")]
    scale_down_delay: u64,
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

type PoolKey = (String, String);
type Pools = HashMap<PoolKey, Vec<Worker>>;
type RestartStates = HashMap<PoolKey, RestartGuard>;
type Draining = Vec<DrainingWorker>;

struct Worker {
    child: Child,
    started_at: Instant,
    stability_reported: bool,
    restart_probe: bool,
}

struct DrainingWorker {
    worker: Worker,
    deadline: Instant,
    label: String,
}

impl Worker {
    fn new(child: Child, restart_probe: bool) -> Self {
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
    #[serde(default)]
    instance_id: Option<String>,
    #[serde(default, rename = "requested_at")]
    _requested_at: Option<String>,
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
    _lock: File,
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
    let config = load_config(options)?;
    validate_config(&config)?;
    let state = State::acquire(&config.state_directory)?;

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
    let mut scale_guards: HashMap<String, ScaleGuard> = HashMap::new();
    let mut last_reconcile: HashMap<String, Instant> = HashMap::new();
    let mut last_desired: HashMap<String, HashMap<String, usize>> = HashMap::new();
    let mut last_poll = Instant::now()
        .checked_sub(Duration::from_secs(config.poll_interval))
        .unwrap_or_else(Instant::now);
    let mut paused = false;
    let mut last_command_nonce: Option<String> = None;

    eprintln!(
        "queen-supervisor started ({} pool definitions)",
        config.supervisors.len()
    );
    if let Err(error) = state.write_status("rust", "running", &pools, &restarts, &draining) {
        eprintln!("state status write failed: {error}");
    }
    while running.load(Ordering::SeqCst) {
        reap(&config, &mut pools, &mut restarts);
        reap_draining(&mut draining);
        observe_stable_workers(&config, &mut pools, &mut restarts);
        match state.command(last_command_nonce.as_deref()) {
            Ok(Some(control)) => {
                last_command_nonce = Some(control.nonce);
                let control_state = match control.command {
                    ControlCommand::Pause => {
                        paused = true;
                        signal_all(&mut pools, libc::SIGUSR2);
                        "paused"
                    }
                    ControlCommand::Continue => {
                        paused = false;
                        signal_all(&mut pools, libc::SIGCONT);
                        "running"
                    }
                    ControlCommand::Terminate => {
                        running.store(false, Ordering::SeqCst);
                        "terminating"
                    }
                };
                if let Err(error) =
                    state.write_status("rust", control_state, &pools, &restarts, &draining)
                {
                    eprintln!("state status write failed: {error}");
                }
            }
            Ok(None) => {}
            Err(error) => {
                eprintln!("control command rejected: {error}");
            }
        }
        if !running.load(Ordering::SeqCst) {
            break;
        }
        if last_poll.elapsed() >= Duration::from_secs(config.poll_interval) {
            let mut names: Vec<_> = config.supervisors.keys().cloned().collect();
            names.sort_unstable();
            let mut unavailable_connections = HashSet::new();
            for name in names {
                if !running.load(Ordering::SeqCst) {
                    break;
                }
                let options = &config.supervisors[&name];
                let ready = last_reconcile
                    .get(&name)
                    .map(|at| at.elapsed() >= Duration::from_secs(options.balance_cooldown))
                    .unwrap_or(true);
                if !ready || paused {
                    continue;
                }

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
                if depth_failed {
                    scale_guards.entry(name.clone()).or_default().reset();
                    let fallback = last_desired
                        .get(&name)
                        .cloned()
                        .unwrap_or_else(|| fail_open_desired(options));
                    if let Err(error) = reconcile(
                        &config,
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
                let runtimes = read_runtimes(
                    &Path::new(&config.state_directory).join("telemetry"),
                    config.telemetry_ttl,
                    TelemetryScope {
                        supervisor: &name,
                        connection: &options.connection,
                        consumer_group: &options.consumer_group,
                    },
                );
                let raw = desired(options, &depths, &runtimes);
                let current = current_allocation(&pools, &name, options);
                let target = stabilize_desired(
                    options,
                    raw,
                    Some(&current),
                    scale_guards.entry(name.clone()).or_default(),
                    Instant::now(),
                );
                last_desired.insert(name.clone(), target.clone());
                if let Err(error) = reconcile(
                    &config,
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
                last_reconcile.insert(name, Instant::now());
            }
            if let Err(error) = state.write_status(
                "rust",
                if paused { "paused" } else { "running" },
                &pools,
                &restarts,
                &draining,
            ) {
                eprintln!("state status write failed: {error}");
            }
            last_poll = Instant::now();
        }
        thread::sleep(Duration::from_millis(200));
    }

    if let Err(error) = state.write_status("rust", "terminating", &pools, &restarts, &draining) {
        eprintln!("state status write failed: {error}");
    }
    shutdown(
        &mut pools,
        &mut draining,
        Duration::from_secs(config.shutdown_grace),
    );
    if let Err(error) = state.write_status("rust", "stopped", &pools, &restarts, &draining) {
        eprintln!("state status write failed: {error}");
    }
    Ok(())
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
    let file = File::open(path)?;
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
    if config.supervisors.is_empty() {
        return Err("configuration has no supervisors".into());
    }
    validate_connection("queen", &config.queen)?;
    for (name, connection) in &config.connections {
        validate_identifier(name, "connection name")?;
        validate_connection(name, connection)?;
    }
    let mut total_max = 0usize;
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
            return Err(format!("supervisor [{name}] has no queues").into());
        }
        let mut queues = HashSet::new();
        for queue in &options.queues {
            if queue.is_empty()
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
            || options.target_clear_seconds <= 0.0
            || !options.default_runtime_seconds.is_finite()
            || options.default_runtime_seconds <= 0.0
        {
            return Err(format!("supervisor [{name}] has invalid scaling targets").into());
        }
        if options.balance_cooldown == 0
            || options.balance_max_shift == 0
            || options.balance_max_shift > config.process_limit
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
    }
    if total_max > config.process_limit {
        return Err("sum of supervisor max_processes exceeds process_limit".into());
    }
    Ok(())
}

fn validate_identifier(value: &str, label: &str) -> Result<(), Box<dyn std::error::Error>> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(format!("{label} must be non-empty and contain no control characters").into());
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

impl State {
    fn acquire(directory: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let directory = PathBuf::from(directory);
        let created = match fs::symlink_metadata(&directory) {
            Ok(_) => false,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(&directory)?;
                true
            }
            Err(error) => return Err(error.into()),
        };
        let metadata = fs::symlink_metadata(&directory)?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err("state_directory must be a real directory".into());
        }
        #[cfg(unix)]
        {
            if metadata.uid() != unsafe { libc::geteuid() } {
                return Err("state_directory must be owned by the supervisor user".into());
            }
            if created {
                fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
            } else if metadata.mode() & 0o7777 != 0o700 {
                return Err("an existing state_directory must use mode 0700".into());
            }
        }

        let path = directory.join("supervisor.lock");
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        let mut lock = options.open(path)?;
        #[cfg(unix)]
        {
            fs::set_permissions(
                directory.join("supervisor.lock"),
                fs::Permissions::from_mode(0o600),
            )?;
        }
        #[cfg(unix)]
        unsafe {
            if libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) != 0 {
                return Err("another Queen supervisor owns the state directory".into());
            }
        }
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
        Ok(Self {
            directory,
            instance_id,
            _lock: lock,
        })
    }

    fn command(
        &self,
        last_nonce: Option<&str>,
    ) -> Result<Option<Control>, Box<dyn std::error::Error>> {
        let path = self.directory.join("control.json");
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if !metadata.file_type().is_file() || metadata.len() > MAX_CONTROL_BYTES {
            let _ = fs::remove_file(&path);
            return Err("control command must be a small regular file".into());
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
            || control.instance_id.as_deref().is_some_and(|value| {
                value.is_empty() || value.len() > 128 || value.chars().any(char::is_control)
            })
        {
            let _ = fs::remove_file(&path);
            return Err("control nonce is invalid".into());
        }
        if Some(control.nonce.as_str()) == last_nonce
            || control
                .instance_id
                .as_deref()
                .is_some_and(|target| target != self.instance_id)
        {
            let _ = fs::remove_file(&path);
            return Ok(None);
        }
        fs::remove_file(path)?;
        Ok(Some(control))
    }

    fn write_status(
        &self,
        engine: &str,
        state: &str,
        pools: &Pools,
        restarts: &RestartStates,
        draining: &Draining,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut entries = serde_json::Map::new();
        for ((supervisor, queue), children) in pools {
            let key = (supervisor.clone(), queue.clone());
            let restart = restarts.get(&key);
            entries.insert(
                format!("{supervisor}:{queue}"),
                serde_json::json!({
                    "processes": children.len(),
                    "pids": children.iter().map(|worker| worker.child.id()).collect::<Vec<_>>(),
                    "restart_failures": restart.map(|guard| guard.consecutive_failures).unwrap_or(0),
                    "restart_state": restart.map(RestartGuard::state_name).unwrap_or("closed"),
                    "restart_in_seconds": restart.and_then(|guard| guard.retry_in_seconds(Instant::now())),
                }),
            );
        }
        let updated_at_epoch = now_epoch();
        atomic_json(
            &self.directory.join("status.json"),
            &serde_json::json!({
                "engine": engine,
                "state": state,
                "pid": std::process::id(),
                "instance_id": &self.instance_id,
                "updated_at": iso8601_from_epoch(updated_at_epoch),
                "updated_at_epoch": updated_at_epoch,
                "draining": draining.len(),
                "pools": entries,
            }),
        )
    }
}

fn atomic_json(path: &Path, value: &serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    fs::write(&temporary, serde_json::to_vec(value)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))?;
    }
    fs::rename(temporary, path)?;
    Ok(())
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
    )
}

fn read_runtimes(directory: &Path, ttl: u64, scope: TelemetryScope<'_>) -> HashMap<String, f64> {
    let mut totals: HashMap<String, f64> = HashMap::new();
    let mut samples: HashMap<String, u64> = HashMap::new();
    let Ok(files) = fs::read_dir(directory) else {
        return HashMap::new();
    };
    let mut paths: Vec<_> = files.flatten().map(|entry| entry.path()).collect();
    paths.sort_unstable();
    for path in paths {
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }
        let Ok(metadata) = fs::symlink_metadata(&path) else {
            continue;
        };
        if !metadata.file_type().is_file() || metadata.len() > MAX_TELEMETRY_BYTES {
            continue;
        }
        let stale = metadata
            .modified()
            .ok()
            .and_then(|at| SystemTime::now().duration_since(at).ok())
            .map(|age| age.as_secs() > ttl)
            .unwrap_or(false);
        if stale {
            let _ = fs::remove_file(&path);
            continue;
        }
        let document: TelemetryDocument = match read_limited(&path, MAX_TELEMETRY_BYTES)
            .ok()
            .and_then(|body| serde_json::from_str(&body).ok())
        {
            Some(document) => document,
            None => continue,
        };
        if document.supervisor != scope.supervisor
            || document.connection != scope.connection
            || document.consumer_group != scope.consumer_group
        {
            continue;
        }
        for (queue, stats) in document.queues {
            let count = stats.samples.min(100);
            if count == 0
                || !stats.runtime_ewma_seconds.is_finite()
                || stats.runtime_ewma_seconds <= 0.0
            {
                continue;
            }
            *totals.entry(queue.clone()).or_default() += stats.runtime_ewma_seconds * count as f64;
            *samples.entry(queue).or_default() += count;
        }
    }
    totals
        .into_iter()
        .filter_map(|(queue, total)| {
            let count = samples.get(&queue).copied().unwrap_or(0);
            (count > 0).then_some((queue, total / count as f64))
        })
        .collect()
}

fn desired(
    options: &SupervisorConfig,
    depths: &HashMap<String, usize>,
    runtimes: &HashMap<String, f64>,
) -> HashMap<String, usize> {
    if options.balance == "simple" {
        return spread(
            empty_allocation(options),
            options.processes,
            &options.queues.iter().cloned().map(|q| (q, 1.0)).collect(),
            &options.queues,
        );
    }

    let weights = scaling_weights(options, depths, runtimes);
    let total_pressure: f64 = options
        .queues
        .iter()
        .map(|queue| weights.get(queue).copied().unwrap_or(0.0))
        .sum();
    let mut target = if total_pressure <= 0.0 {
        options.min_processes
    } else {
        (total_pressure / scaling_divisor(options)).ceil() as usize
    };
    if total_pressure > 0.0 && options.balance == "auto" {
        let active_queues = options
            .queues
            .iter()
            .filter(|queue| weights.get(*queue).copied().unwrap_or(0.0) > 0.0)
            .count();
        target = target.max(active_queues);
    }
    target = target.clamp(options.min_processes, options.max_processes);
    allocation_for_target(options, &weights, target)
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
) -> HashMap<String, usize> {
    let mut allocation = empty_allocation(options);
    if options.balance == "off" {
        allocation.insert(options.queues[0].clone(), target);
        return allocation;
    }
    spread(allocation, target, weights, &options.queues)
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

fn spread(
    mut allocation: HashMap<String, usize>,
    target: usize,
    weights: &HashMap<String, f64>,
    queues: &[String],
) -> HashMap<String, usize> {
    let mut remaining = target;
    for queue in queues {
        if remaining > 0 && weights.get(queue).copied().unwrap_or(0.0) > 0.0 {
            *allocation.get_mut(queue).unwrap() += 1;
            remaining -= 1;
        }
    }
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

fn reconcile(
    config: &Config,
    name: &str,
    options: &SupervisorConfig,
    desired: HashMap<String, usize>,
    pools: &mut Pools,
    restarts: &mut RestartStates,
    draining: &mut Draining,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut budget = options.balance_max_shift;
    let active_processes = pools
        .values()
        .fold(0usize, |total, pool| total.saturating_add(pool.len()));
    let mut process_slots =
        remaining_process_slots(config.process_limit, active_processes, draining.len());
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
                    format!("{name}:{queue}"),
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
        while budget > 0 && process_slots > 0 && target > pool.len() {
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
            ) {
                Ok(worker) => {
                    restart.mark_spawned(permission);
                    pool.push(worker);
                    budget -= 1;
                    process_slots -= 1;
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

fn remaining_process_slots(limit: usize, active: usize, draining: usize) -> usize {
    limit.saturating_sub(active.saturating_add(draining))
}

fn spawn_worker(
    config: &Config,
    name: &str,
    queue: &str,
    o: &SupervisorConfig,
    restart_probe: bool,
) -> Result<Worker, std::io::Error> {
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
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) != 0 {
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
    Ok(Worker::new(child, restart_probe))
}

fn worker_command(config: &Config, name: &str, queue: &str, o: &SupervisorConfig) -> Command {
    let mut command = Command::new(&config.php_binary);
    let worker_queues = if o.balance == "off" {
        o.queues.join(",")
    } else {
        queue.to_owned()
    };
    command
        .current_dir(&config.cwd)
        .arg(&config.artisan)
        .arg("queue:work")
        .arg(&o.connection)
        .arg(format!("--queue={worker_queues}"))
        .arg(format!("--sleep={}", o.sleep))
        .arg(format!("--timeout={}", o.timeout))
        .arg(format!("--tries={}", o.tries))
        .arg(format!("--memory={}", o.memory))
        .arg(format!("--backoff={}", o.backoff))
        .arg(format!("--max-jobs={}", o.max_jobs))
        .arg(format!("--max-time={}", o.max_time))
        .arg(format!("--rest={}", o.rest))
        .env("QUEEN_LARAVEL_CONSUMER_GROUP", &o.consumer_group)
        .env("QUEEN_LARAVEL_CONNECTION", &o.connection)
        .env("QUEEN_LARAVEL_SUPERVISOR", name)
        .env("QUEEN_LARAVEL_RETRY_AFTER", o.retry_after.to_string())
        .env(
            "QUEEN_SUPERVISOR_TELEMETRY_DIR",
            Path::new(&config.state_directory).join("telemetry"),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    if o.force {
        command.arg("--force");
    }
    if o.quiet {
        command.arg("--quiet");
    }
    if o.balance == "off" {
        // A blocking reserve on the first queue in Laravel's ordered list
        // would prevent lower-priority queues from ever being checked.
        command.env("QUEEN_LARAVEL_BLOCK_FOR", "0");
    }
    command
}

fn reap(config: &Config, pools: &mut Pools, restarts: &mut RestartStates) {
    for (key, pool) in pools.iter_mut() {
        let Some(options) = config.supervisors.get(&key.0) else {
            continue;
        };
        let restart = restarts.entry(key.clone()).or_default();
        pool.retain_mut(|worker| match worker.child.try_wait() {
            Ok(None) => true,
            Ok(Some(status)) => {
                record_worker_exit(key, worker, status, options, restart);
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
}

fn record_worker_exit(
    key: &PoolKey,
    worker: &Worker,
    status: ExitStatus,
    options: &SupervisorConfig,
    restart: &mut RestartGuard,
) {
    let uptime = worker.started_at.elapsed();
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
    if uptime >= Duration::from_secs(options.stable_after) {
        restart.record_healthy();
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

fn observe_stable_workers(config: &Config, pools: &mut Pools, restarts: &mut RestartStates) {
    for (key, pool) in pools {
        let Some(options) = config.supervisors.get(&key.0) else {
            continue;
        };
        for worker in pool {
            if !worker.stability_reported
                && worker.started_at.elapsed() >= Duration::from_secs(options.stable_after)
            {
                worker.stability_reported = true;
                worker.restart_probe = false;
                restarts.entry(key.clone()).or_default().record_healthy();
            }
        }
    }
}

fn shutdown(pools: &mut Pools, draining: &mut Draining, grace: Duration) {
    for pool in pools.values_mut() {
        for worker in pool.iter_mut() {
            signal_process_group(&mut worker.child, libc::SIGTERM);
        }
    }
    for entry in draining.iter_mut() {
        signal_process_group(&mut entry.worker.child, libc::SIGTERM);
    }
    let deadline = Instant::now() + grace;
    loop {
        reap_for_shutdown(pools);
        reap_draining(draining);
        if (pools.values().all(Vec::is_empty) && draining.is_empty()) || Instant::now() >= deadline
        {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    for pool in pools.values_mut() {
        for worker in pool {
            signal_process_group(&mut worker.child, libc::SIGKILL);
            let _ = worker.child.wait();
        }
    }
    for entry in draining.iter_mut() {
        signal_process_group(&mut entry.worker.child, libc::SIGKILL);
        let _ = entry.worker.child.wait();
    }
    pools.clear();
    draining.clear();
}

fn reap_for_shutdown(pools: &mut Pools) {
    for pool in pools.values_mut() {
        pool.retain_mut(|worker| !matches!(worker.child.try_wait(), Ok(Some(_))));
    }
}

fn signal_all(pools: &mut Pools, signal: i32) {
    for pool in pools.values_mut() {
        for worker in pool {
            signal_worker(&mut worker.child, signal);
        }
    }
}

fn begin_termination(worker: Worker, grace: Duration, label: String, draining: &mut Draining) {
    let mut entry = DrainingWorker {
        worker,
        deadline: Instant::now() + grace,
        label,
    };
    signal_process_group(&mut entry.worker.child, libc::SIGTERM);
    draining.push(entry);
}

fn reap_draining(draining: &mut Draining) {
    let now = Instant::now();
    draining.retain_mut(|entry| match entry.worker.child.try_wait() {
        Ok(Some(_)) => false,
        Ok(None) if now >= entry.deadline => {
            eprintln!(
                "[{}] pid={} exceeded shutdown grace; sending SIGKILL",
                entry.label,
                entry.worker.child.id()
            );
            signal_process_group(&mut entry.worker.child, libc::SIGKILL);
            let _ = entry.worker.child.wait();
            false
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
                let _ = entry.worker.child.wait();
                false
            } else {
                true
            }
        }
    });
}

fn signal_worker(child: &mut Child, signal: i32) {
    #[cfg(unix)]
    unsafe {
        libc::kill(child.id() as i32, signal);
    }
    #[cfg(not(unix))]
    let _ = child.kill();
}

fn signal_process_group(child: &mut Child, signal: i32) {
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
        }
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
            process_limit: 256,
            queen: QueenConfig {
                url: "http://127.0.0.1:6632".into(),
                urls: vec!["http://127.0.0.1:6632".into()],
                bearer_token: None,
                headers: HashMap::new(),
            },
            connections: HashMap::new(),
            supervisors: HashMap::from([("default".into(), options)]),
        }
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
        fs::write(
            directory.join(file),
            serde_json::to_vec(&serde_json::json!({
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
        )
        .unwrap();
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
        assert_eq!(remaining_process_slots(10, 7, 2), 1);
        assert_eq!(remaining_process_slots(10, 8, 2), 0);
        assert_eq!(remaining_process_slots(10, 8, 4), 0);
    }

    #[test]
    fn simple_is_even() {
        let got = desired(&options("simple"), &HashMap::new(), &HashMap::new());
        assert_eq!(got["high"], 3);
        assert_eq!(got["default"], 3);
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
        assert_eq!(supervisor.retry_after, 90);
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

        let mut root_state = config(options("auto"));
        root_state.state_directory = "/".into();
        assert!(validate_config(&root_state).is_err());

        let mut relative_state = config(options("auto"));
        relative_state.state_directory = "storage/queen-supervisor".into();
        assert!(validate_config(&relative_state).is_err());

        let mut parent_state = config(options("auto"));
        parent_state.state_directory = "/tmp/queen/../state".into();
        assert!(validate_config(&parent_state).is_err());
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
        assert!(
            serde_json::from_str::<Control>(r#"{"command":"pause","nonce":"1","extra":true}"#)
                .is_err()
        );
    }

    #[test]
    fn control_commands_are_scoped_to_the_running_instance() {
        let directory = temporary_directory("control");
        let state = State::acquire(directory.to_str().unwrap()).unwrap();
        atomic_json(
            &directory.join("control.json"),
            &serde_json::json!({
                "command": "terminate",
                "nonce": "request-1",
                "instance_id": "another-instance",
                "requested_at": "2026-08-28T12:00:00Z"
            }),
        )
        .unwrap();
        assert!(state.command(None).unwrap().is_none());
        assert!(!directory.join("control.json").exists());

        state
            .write_status(
                "rust",
                "running",
                &Pools::new(),
                &RestartStates::new(),
                &Draining::new(),
            )
            .unwrap();
        let status: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(directory.join("status.json")).unwrap())
                .unwrap();
        assert_eq!(status["instance_id"], state.instance_id);
        let updated_at_epoch = status["updated_at_epoch"].as_u64().unwrap();
        assert_eq!(
            status["updated_at"].as_str().unwrap(),
            iso8601_from_epoch(updated_at_epoch)
        );

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
    fn pause_signals_only_the_worker_leader() {
        let directory = temporary_directory("leader-signal");
        let ready = directory.join("child-ready");
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg(
                r#"trap ':' USR2
(trap - USR2; : > "$1"; exec sleep 30) &
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
        let mut child = command.spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !ready.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists());

        signal_worker(&mut child, libc::SIGUSR2);
        thread::sleep(Duration::from_millis(100));
        assert!(child.try_wait().unwrap().is_none());

        signal_process_group(&mut child, libc::SIGKILL);
        let _ = child.wait();
        fs::remove_dir_all(directory).unwrap();
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
        let started = Instant::now();
        begin_termination(
            Worker::new(child, false),
            Duration::from_secs(30),
            "default:high".into(),
            &mut draining,
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(draining.len(), 1);

        signal_process_group(&mut draining[0].worker.child, libc::SIGKILL);
        let _ = draining[0].worker.child.wait();
        reap_draining(&mut draining);
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
