//! Event-driven scaling: the broker wakes the master when a watched queue
//! receives jobs, instead of the master waiting for its next poll.
//!
//! Queen has no push channel, but `POST /api/v1/fetch` is a long poll that
//! parks until one of the listed partitions grows. It takes no lease, reads or
//! moves no consumer cursor and claims nothing, so watching a queue never
//! steals a job from a worker. One thread per connection holds that fetch over
//! the Laravel partition stripes (`<prefix>-0000` to `<prefix>-<count - 1>`)
//! of every autoscaling pool and reports which queues grew; the master then
//! reads the depth of those pools at once. A job pushed to a partition outside
//! the stripes (`QueenPartitionable`) is found by the regular poll, as before.
//!
//! Every grown partition answers with the segment that holds its first new
//! record, payloads included, so an answer larger than `MAX_RESPONSE_BYTES`
//! is not read: it counts as growth on every lane. A watcher re-arms at most
//! once per `WAKE_INTERVAL`. A queue that does not exist yet would release the long
//! poll at once, so its lanes are left out and probed again every
//! `MISSING_REPROBE`. A broker without the endpoint, or a token that may not
//! consume, turns the watcher off with one log line; polling continues.

use crate::{connection_config, connection_endpoints, Config, QueenConfig};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// The shortest time between two wakes of one watcher, and between two
/// event-driven reconciles of one pool.
pub(crate) const WAKE_INTERVAL: Duration = Duration::from_secs(1);
/// Parking per fetch, under the broker's 30 s ceiling.
const LONG_POLL_MS: u64 = 20_000;
/// An offset past every log: the broker answers it at once with the bounds.
const PROBE_OFFSET: i64 = i64::MAX / 2;
/// The broker's limit of entries per fetch.
const MAX_ENTRIES: usize = 1024;
/// A grown partition answers with a whole segment; a larger answer is
/// treated as "everything grew" rather than read.
const MAX_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;
const RETRY_DELAY: Duration = Duration::from_secs(5);
/// How often lanes of queues that did not exist are probed again.
const MISSING_REPROBE: Duration = Duration::from_secs(30);
const OUT_OF_RANGE: &str = "OFFSET_OUT_OF_RANGE";

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct EventDrivenConfig {
    /// The partition stripes each connection pushes to, by connection name.
    pub(crate) stripes: HashMap<String, Stripes>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Stripes {
    pub(crate) prefix: String,
    pub(crate) count: usize,
}

impl EventDrivenConfig {
    pub(crate) fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        for (connection, stripes) in &self.stripes {
            if stripes.prefix.is_empty() || stripes.prefix.chars().any(char::is_control) {
                return Err(format!(
                    "event_driven stripes of [{connection}] need a prefix without control characters"
                )
                .into());
            }
            if !(1..=MAX_ENTRIES).contains(&stripes.count) {
                return Err(format!(
                    "event_driven stripes of [{connection}] must count 1 to {MAX_ENTRIES}"
                )
                .into());
            }
        }
        Ok(())
    }
}

/// One watched lane.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Lane {
    pub(crate) queue: String,
    pub(crate) partition: String,
}

/// The lanes to watch per connection: every stripe of every queue of the pools
/// whose worker count follows the backlog (not `simple`, and room to grow).
pub(crate) fn lanes(config: &Config, settings: &EventDrivenConfig) -> HashMap<String, Vec<Lane>> {
    let mut queues: HashMap<String, Vec<String>> = HashMap::new();
    let mut names: Vec<_> = config.supervisors.keys().collect();
    names.sort_unstable();
    for name in names {
        let options = &config.supervisors[name];
        if !follows_backlog(options) {
            continue;
        }
        let watched = queues.entry(options.connection.clone()).or_default();
        for queue in &options.queues {
            if !watched.contains(queue) {
                watched.push(queue.clone());
            }
        }
    }
    let mut lanes = HashMap::new();
    for (connection, queues) in queues {
        let Some(stripes) = settings.stripes.get(&connection) else {
            continue;
        };
        let connection_lanes: Vec<Lane> = queues
            .iter()
            .flat_map(|queue| {
                (0..stripes.count).map(move |slot| Lane {
                    queue: queue.clone(),
                    partition: format!("{}-{slot:04}", stripes.prefix),
                })
            })
            .collect();
        let connection_lanes = if connection_lanes.len() > MAX_ENTRIES {
            eprintln!(
                "event-driven: connection [{connection}] has {} partition(s) to watch, over the broker's {MAX_ENTRIES} per fetch; the rest are found by the regular poll",
                connection_lanes.len()
            );
            connection_lanes.into_iter().take(MAX_ENTRIES).collect()
        } else {
            connection_lanes
        };
        if !connection_lanes.is_empty() {
            lanes.insert(connection, connection_lanes);
        }
    }
    lanes
}

pub(crate) fn follows_backlog(options: &crate::SupervisorConfig) -> bool {
    options.balance != "simple" && options.min_processes < options.max_processes
}

/// The queues that grew since the master last asked, keyed as
/// `connection\0queue`.
#[derive(Clone, Default)]
pub(crate) struct Wakes(Arc<Mutex<HashSet<String>>>);

impl Wakes {
    pub(crate) fn take(&self) -> HashSet<String> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(|error| error.into_inner()))
    }

    fn add(&self, connection: &str, queues: impl IntoIterator<Item = String>) {
        let mut woken = self.0.lock().unwrap_or_else(|error| error.into_inner());
        woken.extend(queues.into_iter().map(|queue| key(connection, &queue)));
    }
}

pub(crate) fn key(connection: &str, queue: &str) -> String {
    format!("{connection}\0{queue}")
}

/// Start one watcher thread per connection with lanes to watch. The threads
/// are detached: a parked fetch must not delay the master's exit.
pub(crate) fn start(
    config: &Config,
    settings: &EventDrivenConfig,
    running: Arc<AtomicBool>,
) -> Result<Wakes, Box<dyn std::error::Error>> {
    let wakes = Wakes::default();
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_millis(LONG_POLL_MS) + Duration::from_secs(config.http_timeout))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut connections: Vec<_> = lanes(config, settings).into_iter().collect();
    connections.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    for (connection, lanes) in connections {
        let queen = connection_config(config, &connection)
            .ok_or_else(|| format!("event-driven connection [{connection}] is not configured"))?
            .clone();
        let watcher = Watcher::new(connection, queen, lanes, client.clone());
        let (wakes, running) = (wakes.clone(), Arc::clone(&running));
        thread::Builder::new()
            .name(format!("queen-watch-{}", watcher.connection))
            .spawn(move || watcher.run(&wakes, &running))?;
    }
    Ok(wakes)
}

struct Watcher {
    connection: String,
    queen: QueenConfig,
    /// Every lane configured for this connection.
    lanes: Vec<Lane>,
    /// The lanes of queues that exist, with their high watermarks.
    armed: Vec<Lane>,
    highs: Vec<i64>,
    probed: bool,
    /// Set while some lanes' queues did not exist: when to probe them again.
    reprobe_at: Option<Instant>,
    client: reqwest::blocking::Client,
}

enum Failure {
    /// The broker cannot serve this watcher: stop it, polling continues.
    Fatal(String),
    Transient(String),
}

enum Step {
    /// The bounds are known again; lanes that grew since the last answer woke.
    Probed(HashSet<String>),
    /// Nothing to watch until the next probe.
    Idle,
    Parked(HashSet<String>),
}

impl Watcher {
    fn new(
        connection: String,
        queen: QueenConfig,
        lanes: Vec<Lane>,
        client: reqwest::blocking::Client,
    ) -> Self {
        Self {
            connection,
            queen,
            lanes,
            armed: Vec::new(),
            highs: Vec::new(),
            probed: false,
            reprobe_at: None,
            client,
        }
    }

    fn run(mut self, wakes: &Wakes, running: &AtomicBool) {
        eprintln!(
            "event-driven: watching {} partition(s) on connection [{}]",
            self.lanes.len(),
            self.connection
        );
        while running.load(Ordering::SeqCst) {
            let started = Instant::now();
            match self.step() {
                Ok(Step::Probed(grown)) if grown.is_empty() => {}
                Ok(Step::Probed(grown)) => wakes.add(&self.connection, grown),
                Ok(Step::Idle) => pause(MISSING_REPROBE, running),
                Ok(Step::Parked(grown)) if grown.is_empty() => {
                    // A long poll released early without growth (retention,
                    // a queue deleted meanwhile) must not become a busy loop.
                    let spent = started.elapsed();
                    if spent < WAKE_INTERVAL {
                        pause(WAKE_INTERVAL - spent, running);
                    }
                }
                Ok(Step::Parked(grown)) => {
                    wakes.add(&self.connection, grown);
                    pause(WAKE_INTERVAL, running);
                }
                Err(Failure::Fatal(error)) => {
                    eprintln!(
                        "event-driven: watching connection [{}] is off, polling continues: {error}",
                        self.connection
                    );
                    return;
                }
                Err(Failure::Transient(error)) => {
                    eprintln!(
                        "event-driven: watching connection [{}] failed, retrying: {error}",
                        self.connection
                    );
                    self.probed = false;
                    pause(RETRY_DELAY, running);
                }
            }
        }
    }

    /// Probe the bounds when they are unknown or due again, otherwise park
    /// until a lane grows or the long poll times out.
    fn step(&mut self) -> Result<Step, Failure> {
        let now = Instant::now();
        if !self.probed || self.reprobe_at.is_some_and(|at| now >= at) {
            let body = fetch_body(&self.lanes, &vec![PROBE_OFFSET; self.lanes.len()], 0);
            let answer = self.send(&body)?;
            let (armed, highs, missing) = arm(&self.lanes, &answer).map_err(Failure::Transient)?;
            // Growth between the last answer and this probe still wakes.
            let before: HashMap<&Lane, i64> =
                self.armed.iter().zip(self.highs.iter().copied()).collect();
            let grown = armed
                .iter()
                .zip(&highs)
                .filter(|(lane, high)| before.get(lane).is_some_and(|old| *high > old))
                .map(|(lane, _)| lane.queue.clone())
                .collect();
            self.armed = armed;
            self.highs = highs;
            self.probed = true;
            self.reprobe_at = missing.then(|| now + MISSING_REPROBE);
            return Ok(Step::Probed(grown));
        }
        if self.armed.is_empty() {
            return Ok(Step::Idle);
        }
        let wait = self.reprobe_at.map_or(LONG_POLL_MS, |at| {
            u64::try_from(at.saturating_duration_since(now).as_millis())
                .unwrap_or(LONG_POLL_MS)
                .min(LONG_POLL_MS)
        });
        let body = fetch_body(&self.armed, &self.highs, wait);
        match self.send(&body) {
            Ok(answer) => {
                let (grown, highs, broken) =
                    grown(&self.armed, &self.highs, &answer).map_err(Failure::Transient)?;
                self.highs = highs;
                self.probed = !broken;
                Ok(Step::Parked(grown))
            }
            Err(Failure::Transient(error)) if error == OVERSIZED => {
                // Too much arrived to read: every lane may have grown.
                self.probed = false;
                Ok(Step::Parked(
                    self.armed.iter().map(|lane| lane.queue.clone()).collect(),
                ))
            }
            Err(failure) => Err(failure),
        }
    }

    /// Every endpoint in turn. The watcher turns off only when none can
    /// serve it; an oversized answer is an answer, not an endpoint failure.
    fn send(&self, body: &serde_json::Value) -> Result<Vec<u8>, Failure> {
        let mut transient = None;
        let mut fatal = None;
        for endpoint in connection_endpoints(&self.queen) {
            match self.send_to(endpoint, body) {
                Ok(answer) => return Ok(answer),
                Err(Failure::Transient(error)) if error == OVERSIZED => {
                    return Err(Failure::Transient(error));
                }
                Err(Failure::Fatal(error)) => fatal = Some(error),
                Err(Failure::Transient(error)) => transient = Some(error),
            }
        }
        match (transient, fatal) {
            (Some(error), _) => Err(Failure::Transient(error)),
            (None, Some(error)) => Err(Failure::Fatal(error)),
            (None, None) => Err(Failure::Fatal("the connection has no URL".into())),
        }
    }

    fn send_to(&self, endpoint: &str, body: &serde_json::Value) -> Result<Vec<u8>, Failure> {
        let mut url =
            reqwest::Url::parse(endpoint).map_err(|error| Failure::Fatal(error.to_string()))?;
        url.path_segments_mut()
            .map_err(|_| Failure::Fatal("Queen URL cannot be a base URL".into()))?
            .extend(["api", "v1", "fetch"]);
        let mut request = self
            .client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_string());
        for (name, value) in &self.queen.headers {
            request = request.header(name, value);
        }
        if let Some(token) = &self.queen.bearer_token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .map_err(|error| Failure::Transient(error.to_string()))?;
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND
            || status == reqwest::StatusCode::UNAUTHORIZED
            || status == reqwest::StatusCode::FORBIDDEN
        {
            return Err(Failure::Fatal(format!(
                "POST /api/v1/fetch answered {status}; the broker needs the fetch endpoint and a token that may consume"
            )));
        }
        if !status.is_success() {
            return Err(Failure::Transient(format!(
                "POST /api/v1/fetch answered {status}"
            )));
        }
        if response.content_length().unwrap_or(0) > MAX_RESPONSE_BYTES {
            return Err(Failure::Transient(OVERSIZED.into()));
        }
        let mut answer = Vec::new();
        response
            .take(MAX_RESPONSE_BYTES + 1)
            .read_to_end(&mut answer)
            .map_err(|error| Failure::Transient(error.to_string()))?;
        if answer.len() as u64 > MAX_RESPONSE_BYTES {
            return Err(Failure::Transient(OVERSIZED.into()));
        }
        Ok(answer)
    }
}

const OVERSIZED: &str = "the fetch answer is larger than the watcher reads";

fn pause(duration: Duration, running: &AtomicBool) {
    let deadline = Instant::now() + duration;
    while running.load(Ordering::SeqCst) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(100).min(deadline - Instant::now()));
    }
}

pub(crate) fn fetch_body(lanes: &[Lane], offsets: &[i64], max_wait_ms: u64) -> serde_json::Value {
    serde_json::json!({
        "entries": lanes
            .iter()
            .zip(offsets)
            .map(|(lane, offset)| serde_json::json!({
                "queue": lane.queue,
                "partition": lane.partition,
                "offset": offset,
                "maxBytes": 1,
            }))
            .collect::<Vec<_>>(),
        "maxWaitMs": max_wait_ms,
    })
}

#[derive(Deserialize)]
struct FetchAnswer {
    entries: Vec<FetchedLane>,
}

#[derive(Deserialize)]
struct FetchedLane {
    #[serde(rename = "highWatermark")]
    high_watermark: i64,
    #[serde(default)]
    error: Option<String>,
}

fn parse(answer: &[u8], expected: usize) -> Result<Vec<FetchedLane>, String> {
    let parsed: FetchAnswer = serde_json::from_slice(answer).map_err(|error| error.to_string())?;
    if parsed.entries.len() != expected {
        return Err(format!(
            "the fetch answered {} entries for {expected} lanes",
            parsed.entries.len()
        ));
    }
    Ok(parsed.entries)
}

/// The lanes a probe found, with their high watermarks, and whether some
/// were left out: an unknown queue would release every long poll at once.
pub(crate) fn arm(lanes: &[Lane], answer: &[u8]) -> Result<(Vec<Lane>, Vec<i64>, bool), String> {
    let mut armed = Vec::with_capacity(lanes.len());
    let mut highs = Vec::with_capacity(lanes.len());
    let mut missing = false;
    for (lane, fetched) in lanes.iter().zip(parse(answer, lanes.len())?) {
        match fetched.error.as_deref() {
            None | Some(OUT_OF_RANGE) => {
                armed.push(lane.clone());
                highs.push(fetched.high_watermark.max(0));
            }
            Some(_) => missing = true,
        }
    }
    Ok((armed, highs, missing))
}

/// The queues whose lanes grew past the watermarks the fetch was armed with,
/// the watermarks to arm the next one with, and whether a lane answered an
/// error that calls for a new probe. A lane retention moved past is re-armed
/// at its new end without counting as growth.
pub(crate) fn grown(
    lanes: &[Lane],
    armed: &[i64],
    answer: &[u8],
) -> Result<(HashSet<String>, Vec<i64>, bool), String> {
    let fetched = parse(answer, lanes.len())?;
    let mut grown = HashSet::new();
    let mut highs = Vec::with_capacity(lanes.len());
    let mut broken = false;
    for ((lane, armed), fetched) in lanes.iter().zip(armed).zip(fetched) {
        let high = fetched.high_watermark.max(0);
        match fetched.error.as_deref() {
            None if high > *armed => {
                grown.insert(lane.queue.clone());
            }
            None | Some(OUT_OF_RANGE) => {}
            Some(_) => broken = true,
        }
        highs.push(high);
    }
    Ok((grown, highs, broken))
}

/// When a pool may be evaluated outside the regular poll: a watched queue
/// grew, or it is still climbing towards a higher target. Both wait for
/// `WAKE_INTERVAL` since the pool's last evaluation.
pub(crate) fn due(woken: bool, climbing: bool, last: Option<Instant>, now: Instant) -> bool {
    (woken || climbing)
        && last.is_none_or(|last| now.saturating_duration_since(last) >= WAKE_INTERVAL)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn lane(queue: &str, partition: &str) -> Lane {
        Lane {
            queue: queue.into(),
            partition: partition.into(),
        }
    }

    fn config(pools: serde_json::Value) -> Config {
        serde_json::from_value(serde_json::json!({
            "version": 2,
            "cwd": "/app",
            "php_binary": "php",
            "artisan": "artisan",
            "state_directory": "/tmp/queen-watch-test",
            "poll_interval": 3,
            "http_timeout": 5,
            "shutdown_grace": 5,
            "telemetry_ttl": 300,
            "process_limit": 32,
            "queen": {"url": "http://127.0.0.1:1"},
            "connections": {"other": {"url": "http://127.0.0.1:2"}},
            "supervisors": pools,
        }))
        .unwrap()
    }

    fn pool(
        connection: &str,
        queues: &[&str],
        balance: &str,
        min: usize,
        max: usize,
    ) -> serde_json::Value {
        serde_json::json!({
            "connection": connection,
            "consumer_group": "laravel",
            "queues": queues,
            "balance": balance,
            "strategy": "size",
            "processes": max,
            "min_processes": min,
            "max_processes": max,
            "target_jobs_per_process": 10,
            "target_clear_seconds": 60.0,
            "default_runtime_seconds": 1.0,
            "balance_cooldown": 3,
            "balance_max_shift": 1,
            "sleep": 1,
            "timeout": 60,
            "tries": 1,
            "memory": 128,
            "backoff": 0,
            "max_jobs": 0,
            "max_time": 0,
            "rest": 0,
            "force": false,
        })
    }

    fn stripes(count: usize) -> EventDrivenConfig {
        EventDrivenConfig {
            stripes: [
                (
                    "queen".to_owned(),
                    Stripes {
                        prefix: "laravel".into(),
                        count,
                    },
                ),
                (
                    "other".to_owned(),
                    Stripes {
                        prefix: "jobs".into(),
                        count,
                    },
                ),
            ]
            .into(),
        }
    }

    #[test]
    fn only_pools_that_follow_the_backlog_are_watched_on_every_stripe() {
        let config = config(serde_json::json!({
            "auto": pool("queen", &["high", "default"], "auto", 1, 10),
            "shared": pool("queen", &["default", "emails"], "off", 1, 4),
            "fixed": pool("queen", &["payments"], "simple", 2, 2),
            "pinned": pool("other", &["reports"], "auto", 3, 3),
        }));

        let lanes = lanes(&config, &stripes(2));

        assert_eq!(
            lanes["queen"],
            vec![
                lane("high", "laravel-0000"),
                lane("high", "laravel-0001"),
                lane("default", "laravel-0000"),
                lane("default", "laravel-0001"),
                lane("emails", "laravel-0000"),
                lane("emails", "laravel-0001"),
            ]
        );
        assert!(!lanes.contains_key("other"));
    }

    #[test]
    fn a_fetch_carries_one_lane_per_entry_and_never_more_than_the_broker_takes() {
        let names: Vec<String> = (0..20).map(|n| format!("q{n}")).collect();
        let queues: Vec<&str> = names.iter().map(String::as_str).collect();
        let config = config(serde_json::json!({"auto": pool("queen", &queues, "auto", 1, 10)}));
        assert_eq!(lanes(&config, &stripes(64))["queen"].len(), MAX_ENTRIES);

        let body = fetch_body(&[lane("high", "laravel-0003")], &[42], 20_000);
        assert_eq!(
            body,
            serde_json::json!({
                "entries": [{"queue": "high", "partition": "laravel-0003", "offset": 42, "maxBytes": 1}],
                "maxWaitMs": 20_000,
            })
        );
    }

    #[test]
    fn a_grown_lane_wakes_its_queue_and_retention_does_not() {
        let lanes = [
            lane("high", "laravel-0000"),
            lane("high", "laravel-0001"),
            lane("default", "laravel-0000"),
        ];
        let answer = br#"{"entries":[
            {"queue":"high","partition":"laravel-0000","records":[],"highWatermark":7,"logStartOffset":0},
            {"queue":"high","partition":"laravel-0001","records":[{"offset":3,"payload":{"job":"x"}}],"highWatermark":4,"logStartOffset":0},
            {"queue":"default","partition":"laravel-0000","records":[],"highWatermark":12,"logStartOffset":12,"error":"OFFSET_OUT_OF_RANGE"}
        ]}"#;

        let (grown, highs, broken) = grown(&lanes, &[7, 3, 9], answer).unwrap();

        assert_eq!(grown, HashSet::from(["high".to_owned()]));
        assert_eq!(highs, vec![7, 4, 12]);
        assert!(!broken);
        assert!(super::grown(&lanes, &[7, 3, 9], br#"{"entries":[]}"#).is_err());
        let deleted = br#"{"entries":[{"highWatermark":7},{"highWatermark":4},{"highWatermark":0,"error":"UNKNOWN_TOPIC_OR_PARTITION"}]}"#;
        assert!(super::grown(&lanes, &[7, 3, 9], deleted).unwrap().2);
    }

    #[test]
    fn a_probe_leaves_out_the_lanes_of_queues_that_do_not_exist_yet() {
        let lanes = [lane("high", "laravel-0000"), lane("new", "laravel-0000")];
        let answer = br#"{"entries":[
            {"highWatermark":3,"error":"OFFSET_OUT_OF_RANGE"},
            {"highWatermark":0,"error":"UNKNOWN_TOPIC_OR_PARTITION"}
        ]}"#;

        let (armed, highs, missing) = arm(&lanes, answer).unwrap();

        assert_eq!(armed, vec![lane("high", "laravel-0000")]);
        assert_eq!(highs, vec![3]);
        assert!(missing);
    }

    #[test]
    fn a_pool_is_due_between_polls_once_a_second_while_woken_or_climbing() {
        let now = Instant::now();
        assert!(due(true, false, None, now));
        assert!(due(false, true, Some(now - WAKE_INTERVAL), now));
        assert!(!due(
            true,
            true,
            Some(now - Duration::from_millis(400)),
            now
        ));
        assert!(!due(false, false, None, now));
    }

    /// Serves the given answers in order, one connection each, and returns
    /// the request bodies it received.
    fn broker(answers: Vec<(&'static str, String)>) -> (String, thread::JoinHandle<Vec<String>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let mut bodies = Vec::new();
            for (status, body) in answers {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 4096];
                let mut expected = None;
                loop {
                    let read = stream.read(&mut buffer).unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if let Some(end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&request[..end]).to_lowercase();
                        let length = head
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length:"))
                            .map(|value| value.trim().parse::<usize>().unwrap())
                            .unwrap_or(0);
                        expected = Some(end + 4 + length);
                    }
                    if expected.is_some_and(|expected| request.len() >= expected) {
                        break;
                    }
                }
                let text = String::from_utf8(request).unwrap();
                bodies.push(text.split("\r\n\r\n").nth(1).unwrap_or("").to_owned());
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            bodies
        });
        (format!("http://{address}"), handle)
    }

    fn watcher(url: String) -> Watcher {
        Watcher::new(
            "queen".into(),
            QueenConfig {
                url,
                bearer_token: Some("token".into()),
                ..QueenConfig::default()
            },
            vec![lane("high", "laravel-0000"), lane("high", "laravel-0001")],
            reqwest::blocking::Client::new(),
        )
    }

    #[test]
    fn the_watcher_probes_then_parks_from_the_bounds_and_reports_growth() {
        let (url, server) = broker(vec![
            ("200 OK", r#"{"entries":[{"highWatermark":5,"error":"OFFSET_OUT_OF_RANGE"},{"highWatermark":0,"error":"OFFSET_OUT_OF_RANGE"}]}"#.into()),
            ("200 OK", r#"{"entries":[{"records":[],"highWatermark":5},{"records":[{"offset":0}],"highWatermark":1}]}"#.into()),
        ]);
        let mut watcher = watcher(url);

        assert!(matches!(watcher.step(), Ok(Step::Probed(grown)) if grown.is_empty()));
        assert!(
            matches!(watcher.step(), Ok(Step::Parked(grown)) if grown == HashSet::from(["high".to_owned()]))
        );
        assert_eq!(watcher.highs, vec![5, 1]);

        let bodies: Vec<serde_json::Value> = server
            .join()
            .unwrap()
            .iter()
            .map(|body| serde_json::from_str(body).unwrap())
            .collect();
        assert_eq!(bodies[0]["maxWaitMs"], 0);
        assert_eq!(bodies[0]["entries"][0]["offset"], PROBE_OFFSET);
        assert_eq!(bodies[1]["maxWaitMs"], LONG_POLL_MS);
        assert_eq!(bodies[1]["entries"][0]["offset"], 5);
        assert_eq!(bodies[1]["entries"][1]["offset"], 0);
    }

    #[test]
    fn growth_before_a_new_probe_still_wakes_its_queue() {
        let (url, server) = broker(vec![
            ("200 OK", r#"{"entries":[{"highWatermark":5,"error":"OFFSET_OUT_OF_RANGE"},{"highWatermark":0,"error":"OFFSET_OUT_OF_RANGE"}]}"#.into()),
            ("200 OK", r#"{"entries":[{"highWatermark":5,"error":"OFFSET_OUT_OF_RANGE"},{"highWatermark":2,"error":"OFFSET_OUT_OF_RANGE"}]}"#.into()),
        ]);
        let mut watcher = watcher(url);

        assert!(matches!(watcher.step(), Ok(Step::Probed(grown)) if grown.is_empty()));
        watcher.probed = false;
        assert!(
            matches!(watcher.step(), Ok(Step::Probed(grown)) if grown == HashSet::from(["high".to_owned()]))
        );
        server.join().unwrap();
    }

    #[test]
    fn one_endpoint_without_the_endpoint_does_not_turn_the_watcher_off() {
        let (missing, first) = broker(vec![(
            "404 Not Found",
            r#"{"code":"no_such_route"}"#.into(),
        )]);
        let (serving, second) = broker(vec![(
            "200 OK",
            r#"{"entries":[{"highWatermark":1,"error":"OFFSET_OUT_OF_RANGE"},{"highWatermark":0,"error":"OFFSET_OUT_OF_RANGE"}]}"#.into(),
        )]);
        let mut watcher = watcher(missing);
        watcher.queen.urls = vec![watcher.queen.url.clone(), serving];

        assert!(matches!(watcher.step(), Ok(Step::Probed(_))));
        assert_eq!(watcher.highs, vec![1, 0]);
        first.join().unwrap();
        second.join().unwrap();
    }

    #[test]
    fn a_broker_without_the_endpoint_turns_the_watcher_off() {
        let (url, server) = broker(vec![(
            "404 Not Found",
            r#"{"code":"no_such_route"}"#.into(),
        )]);
        let mut watcher = watcher(url);

        assert!(matches!(watcher.step(), Err(Failure::Fatal(_))));
        server.join().unwrap();
    }
}
