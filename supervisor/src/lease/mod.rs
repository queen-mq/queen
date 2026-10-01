//! Lease renewal served by the master, instead of one PHP helper process per
//! worker.
//!
//! A Laravel worker with lease renewal holds a broker lease while it runs
//! synchronous job code, and something else must keep that lease alive. The
//! Laravel package's `ProcessLeaseRenewer` starts one PHP helper per worker
//! for that. With this service the worker connects to a private Unix socket
//! in the state directory instead, and one thread of the master runs the
//! helper's algorithm (`LeaseRenewalWorker`) for its one lease:
//!
//! - renew every interval, anchoring the new deadline at the request start;
//! - retry after a second on failure;
//! - when no renewal can finish before the deadline less the safety margin,
//!   SIGTERM the worker, then SIGKILL it at the earlier of the kill grace and
//!   that margin, so a job never outlives its lease.
//!
//! The worker is the connecting process as the kernel reports it, never a PID
//! from a message, and it must run as this user. On Linux a pidfd pins it, so
//! a signal cannot reach a later process that reused the PID; a worker the
//! pidfd cannot signal is refused and falls back to its helper. The worker
//! and this service read the same monotonic clock as PHP's `hrtime`, checked
//! when the worker connects. A connection that ends while it holds a lease,
//! for any reason but a `shutdown` or the worker's exit, kills the worker:
//! fail closed, as the helper's watchdog does.
//!
//! Protocol, one JSON object per line (the helper's, over the socket):
//!
//! ```text
//! worker -> master  {"command":"init","client":{"urls":[..],"bearerToken":..,"headers":{..},"timeoutMillis":N},
//!                    "lease_seconds":N,"interval_millis":N,"request_budget_millis":N,
//!                    "kill_grace_millis":N,"safety_margin_millis":N,"monotonic_millis":N}
//! master -> worker  {"event":"ready"} | {"event":"startup_failed","error":".."}
//! worker -> master  {"command":"track","lease_id":"..","deadline_monotonic_millis":N}
//! master -> worker  {"event":"tracked","lease_id":".."} | {"event":"unsafe","lease_id":"..","error":".."}
//! worker -> master  {"command":"forget","lease_id":".."} | {"command":"shutdown"}
//! ```

mod broker;
mod connection;

use broker::{Broker, ClientConfig};
use connection::Connection;
use serde::Deserialize;
use std::io::ErrorKind;
use std::os::fd::RawFd;
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub(crate) const SOCKET_NAME: &str = "lease.sock";
const RETRY_DELAY_MILLIS: i64 = 1000;
const MAX_ERROR_CHARS: usize = 128;
const MAX_LEASE_ID_BYTES: usize = 255;
/// A year: bounds every duration so the deadline arithmetic cannot overflow.
const MAX_LEASE_SECONDS: u64 = 31_536_000;
const MAX_MILLIS: i64 = 31_536_000_000;
/// How far the worker's clock may be from this one when it connects.
const CLOCK_TOLERANCE_MILLIS: i64 = 1000;
const INIT_TIMEOUT_MILLIS: i64 = 5000;
/// A worker thread only waits on its socket and its deadlines.
const THREAD_STACK_BYTES: usize = 256 * 1024;

/// The process behind a connection.
pub(crate) struct Peer {
    pub(crate) pid: u32,
    #[cfg(target_os = "linux")]
    pidfd: OwnedFd,
}

impl Peer {
    /// Readable once the worker exits (Linux).
    fn exit_fd(&self) -> Option<RawFd> {
        #[cfg(target_os = "linux")]
        {
            Some(self.pidfd.as_raw_fd())
        }
        #[cfg(not(target_os = "linux"))]
        {
            None
        }
    }

    /// Whether the worker has exited; a child that inherited the socket must
    /// not keep a dead worker's lease alive.
    fn exited(&self) -> bool {
        self.exit_fd().is_some_and(|fd| {
            let mut poll = libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: one valid pollfd for the duration of the call.
            unsafe { libc::poll(&mut poll, 1, 0) > 0 }
        })
    }
}

/// Delivers a signal to a worker; injected so tests never signal.
pub(crate) type Fence = Arc<dyn Fn(&Peer, i32) -> std::io::Result<()> + Send + Sync>;

pub(crate) fn signal_fence() -> Fence {
    Arc::new(send_signal)
}

fn send_signal(peer: &Peer, signal: i32) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    // SAFETY: the pidfd stays open for the connection's lifetime; a process
    // that already exited answers ESRCH instead of a recycled PID.
    let result = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            peer.pidfd.as_raw_fd(),
            signal,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    };
    // Other systems only run the service in tests, with a recording fence.
    #[cfg(not(target_os = "linux"))]
    // SAFETY: plain signal delivery to the peer the kernel reported.
    let result = i64::from(unsafe { libc::kill(peer.pid as libc::pid_t, signal) });
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn fence_worker(fence: &Fence, peer: &Peer, signal: i32) {
    if let Err(error) = fence(peer, signal) {
        if error.raw_os_error() != Some(libc::ESRCH) {
            eprintln!(
                "lease service: signal {signal} to worker {} failed: {error}",
                peer.pid
            );
        }
    }
}

/// The clock PHP's `hrtime` reads: CLOCK_MONOTONIC, or on macOS the
/// uptime clock behind `mach_absolute_time`.
pub(crate) fn monotonic_millis() -> i64 {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    #[cfg(target_os = "macos")]
    let clock = libc::CLOCK_UPTIME_RAW;
    #[cfg(not(target_os = "macos"))]
    let clock = libc::CLOCK_MONOTONIC;
    // SAFETY: `now` is a valid, writable timespec.
    unsafe {
        libc::clock_gettime(clock, &mut now);
    }
    // The fields are narrower than i64 on 32-bit targets.
    #[allow(clippy::unnecessary_cast)]
    let millis = now.tv_sec as i64 * 1000 + now.tv_nsec as i64 / 1_000_000;
    millis
}

/// Bind the socket in the private state directory and serve every worker
/// connection on its own thread.
pub(crate) fn start(state_directory: &Path, fence: Fence) -> std::io::Result<String> {
    let path = state_directory.join(SOCKET_NAME);
    // A socket left by an earlier master of this state directory; the
    // directory is private and this master holds its lock.
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let listener = UnixListener::bind(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    let client = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(std::io::Error::other)?;
    thread::Builder::new()
        .name("queen-lease-accept".into())
        .spawn(move || accept(&listener, &client, &fence))?;
    Ok(path.to_string_lossy().into_owned())
}

fn accept(listener: &UnixListener, client: &reqwest::blocking::Client, fence: &Fence) {
    for stream in listener.incoming() {
        let stream = match stream {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("lease service: accept failed: {error}");
                thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        let peer = match peer(&stream, fence) {
            Ok(peer) => peer,
            Err(error) => {
                eprintln!("lease service: refused a worker: {error}");
                continue;
            }
        };
        let pid = peer.pid;
        let (client, fence) = (client.clone(), Arc::clone(fence));
        let spawned = thread::Builder::new()
            .name(format!("queen-lease-{pid}"))
            .stack_size(THREAD_STACK_BYTES)
            .spawn(move || serve(stream, &peer, client, &fence));
        // The worker sees the connection close and falls back to a helper.
        if let Err(error) = spawned {
            eprintln!("lease service: no thread for worker {pid}: {error}");
        }
    }
}

/// The connecting process from the kernel; it must run as this user, and the
/// fence must be able to signal it.
fn peer(stream: &UnixStream, fence: &Fence) -> std::io::Result<Peer> {
    let (pid, uid) = peer_credentials(stream)?;
    // SAFETY: geteuid has no preconditions.
    let euid = unsafe { libc::geteuid() };
    if uid != euid || pid == 0 {
        return Err(std::io::Error::other(format!(
            "peer {pid} runs as uid {uid}"
        )));
    }
    #[cfg(target_os = "linux")]
    let peer = Peer {
        pid,
        pidfd: peer_pidfd(stream, pid)?,
    };
    #[cfg(not(target_os = "linux"))]
    let peer = Peer { pid };
    // Signal 0 only checks: a sandbox that denies the signal must not accept
    // a worker it could never fence.
    fence(&peer, 0).map_err(|error| {
        std::io::Error::other(format!("worker {pid} cannot be signalled: {error}"))
    })?;
    Ok(peer)
}

/// The kernel's pidfd of the peer (Linux 6.5), or else one opened from its
/// PID. The PID was the peer's at connect time, and the worker cannot exit
/// and be reaped while its master is still accepting it, unless it crashed
/// at once: a narrow race that only older kernels keep.
#[cfg(target_os = "linux")]
fn peer_pidfd(stream: &UnixStream, pid: u32) -> std::io::Result<OwnedFd> {
    let mut fd: libc::c_int = -1;
    let mut length = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `fd` and `length` are valid for getsockopt to fill.
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERPIDFD,
            (&mut fd as *mut libc::c_int).cast(),
            &mut length,
        )
    };
    let fd = if result == 0 && fd >= 0 {
        fd
    } else {
        // SAFETY: pidfd_open takes a PID and flags and returns a new fd.
        let opened = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) };
        if opened < 0 {
            return Err(std::io::Error::last_os_error());
        }
        opened as libc::c_int
    };
    // SAFETY: the kernel just returned this fd, and nothing else owns it.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(target_os = "linux")]
fn peer_credentials(stream: &UnixStream) -> std::io::Result<(u32, u32)> {
    let mut credentials = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: `credentials` and `length` are valid for getsockopt to fill.
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut credentials as *mut libc::ucred).cast(),
            &mut length,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok((credentials.pid as u32, credentials.uid))
}

#[cfg(target_os = "macos")]
fn peer_credentials(stream: &UnixStream) -> std::io::Result<(u32, u32)> {
    use std::os::fd::AsRawFd;
    let mut pid: libc::pid_t = 0;
    let mut length = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
    // SAFETY: `pid` and `length` are valid for getsockopt to fill.
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_LOCAL,
            libc::LOCAL_PEERPID,
            (&mut pid as *mut libc::pid_t).cast(),
            &mut length,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let (mut uid, mut gid) = (0, 0);
    // SAFETY: `uid` and `gid` are valid out-pointers.
    if unsafe { libc::getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok((pid as u32, uid))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn peer_credentials(_stream: &UnixStream) -> std::io::Result<(u32, u32)> {
    Err(std::io::Error::other(
        "peer credentials are not supported here",
    ))
}

/// The first message; no Debug, since the client settings hold a token.
#[derive(Deserialize)]
struct Init {
    command: String,
    client: ClientConfig,
    lease_seconds: u64,
    interval_millis: i64,
    request_budget_millis: i64,
    kill_grace_millis: i64,
    safety_margin_millis: i64,
    monotonic_millis: i64,
}

struct Timing {
    lease_seconds: u64,
    interval: i64,
    budget: i64,
    kill_grace: i64,
    margin: i64,
    /// What one renewal needs before the deadline: two request budgets, the
    /// retry delay, the kill grace and the safety margin.
    initial_reserve: i64,
}

impl Timing {
    fn from(init: &Init) -> Result<Self, String> {
        let millis = [
            init.interval_millis,
            init.request_budget_millis,
            init.safety_margin_millis,
        ];
        let endpoints = i64::try_from(init.client.urls.len()).unwrap_or(i64::MAX);
        let attempt = i64::try_from(init.client.timeout_millis).unwrap_or(i64::MAX);
        if !(1..=MAX_LEASE_SECONDS).contains(&init.lease_seconds)
            || millis.iter().any(|value| !(1..=MAX_MILLIS).contains(value))
            || !(0..=MAX_MILLIS).contains(&init.kill_grace_millis)
            || endpoints == 0
            || attempt < 1
            // One renewal tries every endpoint; together they must fit the budget.
            || endpoints.saturating_mul(attempt) > init.request_budget_millis
        {
            return Err("invalid renewal timing".into());
        }
        Ok(Self {
            lease_seconds: init.lease_seconds,
            interval: init.interval_millis,
            budget: init.request_budget_millis,
            kill_grace: init.kill_grace_millis,
            margin: init.safety_margin_millis,
            initial_reserve: 2 * init.request_budget_millis
                + RETRY_DELAY_MILLIS
                + init.kill_grace_millis
                + init.safety_margin_millis,
        })
    }

    fn next_renewal(&self, now: i64, expires: i64) -> i64 {
        (now + self.interval).min(now.max(expires - self.initial_reserve))
    }
}

/// The worker's one lease; `ProcessLeaseRenewer` tracks one at a time too.
struct Held {
    id: String,
    expires: i64,
    next: i64,
    kill_at: Option<i64>,
}

/// How a session ended.
enum End {
    /// The worker asked to stop, or it exited: nothing to fence.
    Left,
    /// The worker may still run a leased job that nobody renews.
    Fence(String),
}

fn serve(stream: UnixStream, peer: &Peer, client: reqwest::blocking::Client, fence: &Fence) {
    let mut held: Option<Held> = None;
    let end = catch_unwind(AssertUnwindSafe(|| match Connection::new(stream) {
        Ok(mut connection) => session(&mut connection, client, &mut held, peer, fence),
        Err(error) => End::Fence(error.to_string()),
    }))
    .unwrap_or_else(|_| End::Fence("the renewal thread panicked".into()));
    if let (End::Fence(reason), Some(lease)) = (end, held) {
        // Nobody renews this lease any more: the job must not outlive it.
        eprintln!(
            "lease service: killing worker {} holding lease {}: {reason}",
            peer.pid,
            bounded(&lease.id)
        );
        fence_worker(fence, peer, libc::SIGKILL);
    }
}

fn session(
    connection: &mut Connection,
    client: reqwest::blocking::Client,
    held: &mut Option<Held>,
    peer: &Peer,
    fence: &Fence,
) -> End {
    let (broker, timing) = match init(connection, client) {
        Ok(started) => started,
        Err(error) => {
            // The worker falls back to its helper whether or not it reads this.
            let _ = connection.emit(&serde_json::json!({
                "event": "startup_failed",
                "error": bounded(&error),
            }));
            return End::Left;
        }
    };
    if connection
        .emit(&serde_json::json!({"event": "ready"}))
        .is_err()
    {
        return End::Left;
    }
    loop {
        // Deadlines first: nothing the worker sends may delay a kill.
        let kill_at = held.as_ref().and_then(|lease| lease.kill_at);
        if kill_at.is_some_and(|kill_at| monotonic_millis() >= kill_at) {
            return End::Fence("its lease reached the safety margin after SIGTERM".into());
        }
        if peer.exited() {
            return End::Left;
        }
        if let Some(end) = apply_commands(connection, held, &timing) {
            return end;
        }
        if let Some(end) = renew_if_due(connection, &broker, held, peer, fence, &timing) {
            return end;
        }
        let wait = held
            .as_ref()
            .map(|lease| (lease.kill_at.unwrap_or(lease.next) - monotonic_millis()).max(0));
        connection.wait(wait, peer.exit_fd());
    }
}

fn init(
    connection: &mut Connection,
    client: reqwest::blocking::Client,
) -> Result<(Broker, Timing), String> {
    let deadline = monotonic_millis() + INIT_TIMEOUT_MILLIS;
    let line = loop {
        if let Some(line) = connection.next_line().map_err(|error| error.to_string())? {
            break line;
        }
        let left = deadline - monotonic_millis();
        if left <= 0 {
            return Err("no initialization message".into());
        }
        connection.wait(Some(left), None);
    };
    let init: Init =
        serde_json::from_slice(&line).map_err(|_| "invalid initialization message".to_owned())?;
    if init.command != "init" {
        return Err("invalid initialization message".into());
    }
    let timing = Timing::from(&init)?;
    let broker = Broker::new(client, &init.client)?;
    if (init.monotonic_millis - monotonic_millis()).abs() > CLOCK_TOLERANCE_MILLIS {
        return Err("the worker and the supervisor read different monotonic clocks".into());
    }
    Ok((broker, timing))
}

/// Apply every complete command available now; Some ends the session.
fn apply_commands(
    connection: &mut Connection,
    held: &mut Option<Held>,
    timing: &Timing,
) -> Option<End> {
    loop {
        let line = match connection.next_line() {
            Ok(Some(line)) => line,
            Ok(None) => return None,
            Err(error) => return Some(End::Fence(error.to_string())),
        };
        let Ok(command) = serde_json::from_slice::<serde_json::Value>(&line) else {
            continue;
        };
        let kind = command.get("command").and_then(serde_json::Value::as_str);
        if kind == Some("shutdown") {
            return Some(End::Left);
        }
        let Some(id) = command
            .get("lease_id")
            .and_then(serde_json::Value::as_str)
            .filter(|id| !id.is_empty() && id.len() <= MAX_LEASE_ID_BYTES)
        else {
            continue;
        };
        let answer = match kind {
            Some("forget") => {
                if held.as_ref().is_some_and(|lease| lease.id == id) {
                    *held = None;
                }
                continue;
            }
            Some("track") => track(held, timing, id, &command),
            _ => continue,
        };
        if let Err(error) = connection.emit(&answer) {
            return Some(End::Fence(format!(
                "its events cannot be delivered: {error}"
            )));
        }
    }
}

/// Track a lease and return the answer for the worker.
fn track(
    held: &mut Option<Held>,
    timing: &Timing,
    id: &str,
    command: &serde_json::Value,
) -> serde_json::Value {
    let refuse =
        |error: &str| serde_json::json!({"event": "unsafe", "lease_id": id, "error": error});
    let Some(deadline) = command
        .get("deadline_monotonic_millis")
        .and_then(serde_json::Value::as_i64)
        .filter(|deadline| (1..=i64::MAX / 2).contains(deadline))
    else {
        return refuse("invalid monotonic lease deadline");
    };
    match held {
        Some(lease) if lease.id != id => return refuse("a worker holds one lease at a time"),
        Some(_) => {}
        None => {
            *held = Some(Held {
                id: id.to_owned(),
                expires: deadline,
                next: timing.next_renewal(monotonic_millis(), deadline),
                kill_at: None,
            });
        }
    }
    serde_json::json!({"event": "tracked", "lease_id": id})
}

fn renew_if_due(
    connection: &mut Connection,
    broker: &Broker,
    held: &mut Option<Held>,
    peer: &Peer,
    fence: &Fence,
    timing: &Timing,
) -> Option<End> {
    let now = monotonic_millis();
    let lease = held
        .as_mut()
        .filter(|lease| lease.kill_at.is_none() && now >= lease.next)?;
    // Do not begin an attempt that cannot finish before the lease's safety
    // boundary: TERM gives Laravel a moment to finish and ACK, KILL fences a
    // job still running before another consumer can receive the lease.
    if now + timing.budget + timing.margin >= lease.expires {
        mark_unsafe(
            connection,
            lease,
            peer,
            fence,
            timing,
            "renewal deadline exhausted",
        );
        return None;
    }
    let id = lease.id.clone();
    let outcome = broker.renew(&id, timing.lease_seconds);
    let after = monotonic_millis();
    if let Err(error) = outcome {
        // An ACK racing this request may have closed the lease and sent
        // `forget`: read it before blaming the job.
        if let Some(end) = apply_commands(connection, held, timing) {
            return Some(end);
        }
        let lease = held.as_mut().filter(|lease| lease.id == id)?;
        lease.next = after + RETRY_DELAY_MILLIS;
        if lease.next + timing.budget + timing.margin >= lease.expires {
            mark_unsafe(connection, lease, peer, fence, timing, &error);
        }
        return None;
    }
    // The broker renewed during the request, never after the answer:
    // anchoring at the start is conservative.
    lease.expires = now + timing.lease_seconds as i64 * 1000;
    lease.next = timing.next_renewal(after, lease.expires);
    None
}

fn mark_unsafe(
    connection: &mut Connection,
    lease: &mut Held,
    peer: &Peer,
    fence: &Fence,
    timing: &Timing,
    error: &str,
) {
    // Arm the fence before the diagnostic: the worker is running job code and
    // may not read the socket until the job returns.
    fence_worker(fence, peer, libc::SIGTERM);
    let now = monotonic_millis();
    lease.kill_at = Some((now + timing.kill_grace).min(lease.expires - timing.margin));
    connection.emit_best_effort(&serde_json::json!({
        "event": "unsafe",
        "lease_id": lease.id,
        "error": bounded(error),
    }));
}

fn bounded(error: &str) -> String {
    error
        .chars()
        .map(|c| if (' '..='~').contains(&c) { c } else { '?' })
        .take(MAX_ERROR_CHARS)
        .collect()
}

#[cfg(test)]
mod tests;
