use super::broker::test_server;
use super::*;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Instant;

const RENEWED: &str = r#"{"success":true,"renewed":1,"newExpiresAt":"2026-09-30T10:00:00Z"}"#;
const REFUSED: &str = r#"{"success":false,"renewed":0,"error":"lease not found"}"#;

type Signals = Arc<Mutex<Vec<(u32, i32)>>>;

/// A running service on a fresh socket that records signals instead of
/// sending them; the directory goes with it.
struct Service {
    directory: PathBuf,
    socket: PathBuf,
    signals: Signals,
}

impl Service {
    fn start(name: &str) -> Self {
        let directory = std::env::temp_dir().join(format!("qls-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        let signals: Signals = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&signals);
        let fence: Fence = Arc::new(move |peer, signal| {
            // Signal 0 is the acceptance probe.
            if signal != 0 {
                recorded.lock().unwrap().push((peer.pid, signal));
            }
            Ok(())
        });
        let socket = PathBuf::from(start(&directory, fence).unwrap());
        Self {
            directory,
            socket,
            signals,
        }
    }

    fn signals(&self) -> Vec<(u32, i32)> {
        self.signals.lock().unwrap().clone()
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

struct Worker {
    stream: UnixStream,
    reader: BufReader<UnixStream>,
}

impl Worker {
    fn connect(service: &Service, url: &str, interval_millis: i64, kill_grace_millis: i64) -> Self {
        Self::connect_with(service, url, interval_millis, kill_grace_millis, 0)
    }

    fn connect_with(
        service: &Service,
        url: &str,
        interval_millis: i64,
        kill_grace_millis: i64,
        clock_offset: i64,
    ) -> Self {
        let stream = UnixStream::connect(&service.socket).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        let mut worker = Self { stream, reader };
        worker.send(&serde_json::json!({
            "command": "init",
            "client": {"urls": [url], "bearerToken": "secret", "headers": {}, "timeoutMillis": 500},
            "lease_seconds": 30,
            "interval_millis": interval_millis,
            "request_budget_millis": 500,
            "kill_grace_millis": kill_grace_millis,
            "safety_margin_millis": 300,
            "monotonic_millis": monotonic_millis() + clock_offset,
        }));
        worker
    }

    fn send(&mut self, value: &serde_json::Value) {
        writeln!(self.stream, "{value}").unwrap();
    }

    fn event(&mut self) -> serde_json::Value {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        serde_json::from_str(&line).unwrap()
    }

    fn ready(mut self) -> Self {
        assert_eq!(self.event()["event"], "ready");
        self
    }

    fn track(&mut self, id: &str, millis_left: i64) -> serde_json::Value {
        self.send(&serde_json::json!({
            "command": "track",
            "lease_id": id,
            "deadline_monotonic_millis": monotonic_millis() + millis_left,
        }));
        self.event()
    }
}

fn tracked(id: &str) -> serde_json::Value {
    serde_json::json!({"event": "tracked", "lease_id": id})
}

fn wait_until(what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done() {
        assert!(Instant::now() < deadline, "timed out: {what}");
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn a_renewal_is_due_after_the_interval_or_at_the_reserve_whichever_is_first() {
    let timing = Timing {
        lease_seconds: 60,
        interval: 1000,
        budget: 500,
        kill_grace: 200,
        margin: 300,
        initial_reserve: 2 * 500 + RETRY_DELAY_MILLIS + 200 + 300,
    };
    assert_eq!(timing.next_renewal(1_000, 1_000 + 60_000), 2_000);
    // Close to the deadline the reserve wins, but never in the past.
    assert_eq!(timing.next_renewal(1_000, 1_000 + 2_500), 1_000);
    assert_eq!(timing.next_renewal(1_000, 1_000 + 2_800), 1_300);
}

#[test]
fn a_tracked_lease_is_renewed_on_schedule_until_it_is_forgotten() {
    let (url, requests) = test_server::start(vec![(200, RENEWED)]);
    let service = Service::start("renew");
    let mut worker = Worker::connect(&service, &url, 100, 200).ready();

    assert_eq!(worker.track("lease/1", 30_000), tracked("lease/1"));
    wait_until("two renewals", || requests.lock().unwrap().len() >= 2);

    worker.send(&serde_json::json!({"command": "forget", "lease_id": "lease/1"}));
    thread::sleep(Duration::from_millis(300));
    let settled = requests.lock().unwrap().len();
    thread::sleep(Duration::from_millis(300));
    assert_eq!(
        requests.lock().unwrap().len(),
        settled,
        "a forgotten lease was renewed"
    );
    worker.send(&serde_json::json!({"command": "shutdown"}));
    thread::sleep(Duration::from_millis(100));
    assert!(service.signals().is_empty());
}

#[test]
fn a_lease_that_cannot_be_renewed_in_time_terminates_then_kills_its_worker() {
    let (url, _requests) = test_server::start(vec![(200, REFUSED)]);
    let service = Service::start("unsafe");
    let mut worker = Worker::connect(&service, &url, 100, 200).ready();

    // 2.2 s left: one refused renewal, then no time for another.
    assert_eq!(worker.track("doomed", 2_200), tracked("doomed"));
    let unsafe_event = worker.event();
    assert_eq!(unsafe_event["event"], "unsafe");
    assert_eq!(unsafe_event["error"], "lease not found");

    let pid = std::process::id();
    wait_until("SIGKILL", || {
        service.signals().contains(&(pid, libc::SIGKILL))
    });
    assert_eq!(service.signals()[0], (pid, libc::SIGTERM));
}

#[test]
fn a_job_that_finishes_in_the_kill_grace_is_not_killed() {
    let (url, _requests) = test_server::start(vec![(200, REFUSED)]);
    let service = Service::start("grace");
    let mut worker = Worker::connect(&service, &url, 100, 1000).ready();

    // Unsafe after the second refusal at about 1 s, SIGKILL due about 0.9 s later.
    assert_eq!(worker.track("finishing", 2_200), tracked("finishing"));
    assert_eq!(worker.event()["event"], "unsafe");
    worker.send(&serde_json::json!({"command": "forget", "lease_id": "finishing"}));
    thread::sleep(Duration::from_millis(1500));

    assert_eq!(service.signals(), vec![(std::process::id(), libc::SIGTERM)]);
}

#[test]
fn a_connection_lost_while_holding_a_lease_kills_the_worker_and_a_shutdown_does_not() {
    let (url, _requests) = test_server::start(vec![(200, RENEWED)]);
    let service = Service::start("lost");
    let pid = std::process::id();

    let mut leaving = Worker::connect(&service, &url, 1000, 200).ready();
    assert_eq!(leaving.track("held", 30_000), tracked("held"));
    leaving.send(&serde_json::json!({"command": "shutdown"}));
    thread::sleep(Duration::from_millis(300));
    assert!(service.signals().is_empty());

    let mut lost = Worker::connect(&service, &url, 1000, 200).ready();
    assert_eq!(lost.track("held", 30_000), tracked("held"));
    drop(lost);
    wait_until("SIGKILL", || {
        service.signals() == vec![(pid, libc::SIGKILL)]
    });

    // No lease held: closing is a normal exit.
    let idle = Worker::connect(&service, &url, 1000, 200).ready();
    drop(idle);
    thread::sleep(Duration::from_millis(300));
    assert_eq!(service.signals().len(), 1);
}

#[test]
fn a_worker_holds_one_lease_at_a_time() {
    let (url, _requests) = test_server::start(vec![(200, RENEWED)]);
    let service = Service::start("one");
    let mut worker = Worker::connect(&service, &url, 1000, 200).ready();

    assert_eq!(worker.track("first", 30_000), tracked("first"));
    assert_eq!(worker.track("first", 30_000), tracked("first"));
    let second = worker.track("second", 30_000);
    assert_eq!(second["event"], "unsafe");
    assert_eq!(second["lease_id"], "second");
    worker.send(&serde_json::json!({"command": "shutdown"}));
}

#[test]
fn a_worker_on_another_clock_or_with_an_impossible_budget_is_refused() {
    let (url, _requests) = test_server::start(vec![(200, RENEWED)]);
    let service = Service::start("refused");

    let mut skewed = Worker::connect_with(&service, &url, 1000, 200, 60_000);
    let event = skewed.event();
    assert_eq!(event["event"], "startup_failed");
    assert_eq!(
        event["error"],
        "the worker and the supervisor read different monotonic clocks"
    );

    let mut invalid = Worker::connect(&service, &url, 0, 200);
    assert_eq!(invalid.event()["error"], "invalid renewal timing");

    // Two endpoints of 500 ms each cannot fit a 500 ms request budget.
    let stream = UnixStream::connect(&service.socket).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut over = Worker {
        reader: BufReader::new(stream.try_clone().unwrap()),
        stream,
    };
    over.send(&serde_json::json!({
        "command": "init",
        "client": {"urls": [url.clone(), url], "timeoutMillis": 500},
        "lease_seconds": 30, "interval_millis": 1000, "request_budget_millis": 500,
        "kill_grace_millis": 200, "safety_margin_millis": 300,
        "monotonic_millis": monotonic_millis(),
    }));
    assert_eq!(over.event()["error"], "invalid renewal timing");
}

#[test]
fn the_socket_is_private_to_the_supervisor_user() {
    let service = Service::start("mode");
    let mode = std::fs::metadata(&service.socket)
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
}
