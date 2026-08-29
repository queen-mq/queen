//! EMBEDDED MODE for the Kafka wire facade (PLAN_QUEEN_KAFKA.md, packaging).
//!
//! `QUEEN_KAFKA_EMBEDDED=true` makes this broker process SPAWN and SUPERVISE the
//! `queen-kafka` binary as a child, wired to this broker's own HTTP listener over
//! loopback. One deployment, two processes.
//!
//! ## Why a child process and not a library
//! The facade is a Kafka protocol server with its own accept loop, its own
//! connection budget, its own decompression arena and its own crash modes. Linked
//! in-process, a malformed Produce that walks the decompressor into an allocation
//! failure would take the BROKER down with it — and `panic = "abort"` (see the
//! header of `obs.rs`) means "take down" is literal, not "lose one task". A child
//! keeps the blast radius at the facade: it dies, the broker keeps serving, and
//! this supervisor brings it back. The isolation is the point; the packaging is
//! the convenience.
//!
//! ## What is guaranteed on shutdown, and what is best-effort
//! The child is put in its OWN process group (`setpgid`), so a stop signals the
//! whole group and any grandchild dies with it rather than being re-parented.
//!
//! * broker **SIGTERM / Ctrl-C** — guaranteed: `main` awaits [`Supervisor::shutdown`]
//!   after the serve loop drains, which sends SIGTERM to the group and escalates to
//!   SIGKILL after `QUEEN_KAFKA_SHUTDOWN_GRACE_MS`.
//! * broker **panic / unexpected drop** — `kill_on_drop(true)`: tokio reaps the
//!   child when the `Child` handle drops.
//! * broker **SIGKILL** — platform-dependent, and the honest answer differs:
//!   on **Linux** the child sets `PR_SET_PDEATHSIG(SIGKILL)`, so the kernel kills it
//!   when the parent dies, whatever the parent died of. On **macOS/BSD** there is no
//!   equivalent: a SIGKILLed broker leaves the facade running, re-parented to init,
//!   still holding its Kafka port. That is a dev-machine caveat, not a production
//!   one (production is Linux), and it is stated rather than papered over.
//!
//! ## Anti-flood
//! Two independent guards, because there are two independent floods. A crash-loop
//! is bounded by the exponential backoff itself: the exit line can print at most
//! once per backoff, i.e. 1s, 2s, 4s … capped at 30s, so a permanently broken
//! child costs two lines a minute forever. The child's own stdout/stderr is bounded
//! by [`LineBudget`], which passes the first `LOG_LINES_PER_WINDOW` lines of each
//! window and then reports `suppressed=N` once — the same shape, and the same field
//! name, as `obs::Sampler`.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Notify;

use crate::config::KafkaFacadeConfig;

/// The binary this supervises, and the file name looked for next to the broker's
/// own executable when `QUEEN_KAFKA_BIN` is unset.
pub const FACADE_BIN: &str = "queen-kafka";

/// Backoff ladder: the wait before re-spawning a child that exited. Doubling from
/// one second to a thirty second ceiling — long enough that a permanently broken
/// facade costs nothing, short enough that a one-off crash is invisible to clients
/// (a Kafka client reconnects and resumes from offsets that live in Queen).
const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// A child that ran this long before exiting was HEALTHY, and its successor starts
/// the ladder again from one second. Without the reset, a facade that crashes once
/// a week would eventually be restarted at the 30s ceiling for a fault that has
/// nothing to do with the previous one, a week earlier.
const HEALTHY_RUN: Duration = Duration::from_secs(3600);

/// Child output budget: lines per window, and the window.
const LOG_LINES_PER_WINDOW: u64 = 200;
const LOG_WINDOW: Duration = Duration::from_secs(10);
/// A forwarded line is truncated here. The broker's log is a shared resource and
/// `lines()` has no bound of its own; a child that prints a megabyte on one line
/// must not be able to write a megabyte into the broker's log shipper.
const LOG_LINE_MAX: usize = 4096;

/// Variables REMOVED from the inherited environment before exec.
///
/// Everything else forwards verbatim — which is the whole of the child's
/// configuration surface, `QUEEN_KAFKA_*` included, plus `QUEEN_TOKEN`,
/// `LOG_LEVEL`/`RUST_LOG` and `QUEEN_LOG_JSON`. These four are the broker's own
/// secrets (the set `config::log_effective` masks) and the facade reads none of
/// them: a database password has no business being readable in a second process's
/// environment just because that process happens to be colocated.
///
/// `QUEEN_TOKEN` is deliberately NOT here. It is a secret, but it is the CHILD's
/// credential — the one it presents to this broker — so removing it would be
/// removing the feature.
const STRIPPED_ENV: &[&str] = &[
    "PG_PASSWORD",
    "JWT_SECRET",
    "QUEEN_ENCRYPTION_KEY",
    "QUEEN_SYNC_SECRET",
];

// ---------------------------------------------------------------------------
// Boot-time resolution. All pure, all unit-tested: an operator's mistake must
// name itself at boot, and the code that decides that must be testable without
// spawning anything.
// ---------------------------------------------------------------------------

/// Where the facade binary is. `QUEEN_KAFKA_BIN` when set, otherwise the file
/// named [`FACADE_BIN`] NEXT TO the running broker executable — which is what
/// makes the Docker image work with no configuration at all (both binaries land
/// in `/app/bin`).
pub fn resolve_bin(configured: &str, current_exe: Option<&Path>) -> PathBuf {
    if !configured.trim().is_empty() {
        return PathBuf::from(configured.trim());
    }
    match current_exe.and_then(Path::parent) {
        Some(dir) => dir.join(FACADE_BIN),
        // No executable path (a platform that cannot answer `current_exe`): fall
        // back to the bare name so PATH resolution still has a chance, rather
        // than building a path relative to an unknown directory.
        None => PathBuf::from(FACADE_BIN),
    }
}

/// The misconfiguration that must be caught at BOOT rather than discovered as a
/// crash-loop. Both cases are conditions no amount of restarting can resolve, and
/// the broker's own convention for those is `obs::fatal` (see
/// `AuthConfig::validate`, `checked_bind_addr`, the KV trusted-proxy interlock).
///
/// `advertised` is `QUEEN_KAFKA_ADVERTISED_ADDR`, which the facade REQUIRES and
/// deliberately gives no default (its `resolve` explains why: there is no default
/// that is right more often than it is wrong). Left unset, the child would exit 1
/// on every spawn forever, and the operator would read a backoff ladder instead of
/// the one sentence that fixes it.
pub fn preflight(bin: &Path, advertised: Option<&str>) -> Result<(), String> {
    if !bin.is_file() {
        return Err(format!(
            "QUEEN_KAFKA_EMBEDDED=true but no {FACADE_BIN} binary at {}. \
             Embedded mode spawns the Kafka facade as a child process; it cannot spawn a file \
             that is not there. Point QUEEN_KAFKA_BIN at the binary, or put {FACADE_BIN} next \
             to the broker executable (the queen Docker image ships both in /app/bin), or unset \
             QUEEN_KAFKA_EMBEDDED to run the broker alone.",
            bin.display()
        ));
    }
    if advertised
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .is_none()
    {
        return Err(
            "QUEEN_KAFKA_EMBEDDED=true without QUEEN_KAFKA_ADVERTISED_ADDR. It is the host:port \
             OTHER MACHINES use to reach the Kafka listener, and the facade has no default for \
             it on purpose: Kafka clients bootstrap once and then talk only to the address they \
             were advertised, so a wrong one fails AFTER a successful connection. Set it (e.g. \
             QUEEN_KAFKA_ADVERTISED_ADDR=kafka.example.com:9092), or unset QUEEN_KAFKA_EMBEDDED. \
             It is refused here rather than in the child because a child that can never boot is \
             a crash-loop, not a configuration error an operator can read."
                .to_string(),
        );
    }
    Ok(())
}

/// The loopback URL the child is given for `QUEEN_URL`, derived from the address
/// the broker's listener ACTUALLY bound (so `PORT=0` and a wildcard bind both come
/// out right). A wildcard is rewritten to the matching loopback — the child is in
/// this process's own network namespace by construction, so the shortest path to
/// this broker is the local one, and it never leaves the host.
pub fn loopback_url(local: &std::net::SocketAddr) -> String {
    let port = local.port();
    match local.ip() {
        std::net::IpAddr::V4(v4) if v4.is_unspecified() => format!("http://127.0.0.1:{port}"),
        std::net::IpAddr::V4(v4) => format!("http://{v4}:{port}"),
        std::net::IpAddr::V6(v6) if v6.is_unspecified() => format!("http://[::1]:{port}"),
        std::net::IpAddr::V6(v6) => format!("http://[{v6}]:{port}"),
    }
}

/// The one auth posture embedded mode cannot fix by itself, worded once.
///
/// With `JWT_ENABLED=true` the broker requires a token on every data path the
/// facade uses (push, fetch, kv — none of them are in the default
/// `JWT_SKIP_PATHS`), and the loopback hop is not exempt: embedded mode does not
/// get a private door into the broker, because a private door is exactly the kind
/// of thing that is later found open from somewhere else. So the child needs a
/// credential like any other client, and it has two legitimate ways to hold one:
/// `QUEEN_TOKEN` (one identity for the whole listener) or `QUEEN_KAFKA_SASL=plain`
/// (each client presents its own, the Cloud shape). With neither, every produce
/// and fetch answers 401 — a WARN and not a fatal, because the operator may be
/// mid-rollout and the facade is not load-bearing for the broker.
pub fn auth_advisory(
    broker_auth_on: bool,
    token: Option<&str>,
    sasl: Option<&str>,
) -> Option<String> {
    let has = |v: Option<&str>| v.map(str::trim).is_some_and(|v| !v.is_empty());
    if !broker_auth_on || has(token) || has(sasl) {
        return None;
    }
    Some(
        "JWT_ENABLED=true with QUEEN_KAFKA_EMBEDDED=true, but the facade has no credential: \
         every produce and fetch it makes will be answered 401. Give the child a QUEEN_TOKEN, \
         or set QUEEN_KAFKA_SASL=plain so each Kafka client presents its own Queen token. \
         The loopback hop is authenticated like any other."
            .to_string(),
    )
}

/// The wait before the next spawn. A HEALTHY run resets the ladder.
fn backoff_after(current: Duration, uptime: Duration) -> Duration {
    if uptime >= HEALTHY_RUN {
        BACKOFF_INITIAL
    } else {
        current
    }
}

/// The next rung: double, capped.
fn next_backoff(current: Duration) -> Duration {
    (current * 2).min(BACKOFF_MAX)
}

// ---------------------------------------------------------------------------
// The child output budget.
// ---------------------------------------------------------------------------

/// Windowed line budget for forwarded child output. `allow` answers `Some(n)` when
/// the line may be printed, where `n` is how many lines were dropped since the last
/// one that was (`0` in the normal case) — the same contract as `obs::Sampler`,
/// down to the `suppressed` field the caller prints.
struct LineBudget {
    window_start: Instant,
    used: u64,
    suppressed: u64,
}

impl LineBudget {
    fn new(now: Instant) -> LineBudget {
        LineBudget {
            window_start: now,
            used: 0,
            suppressed: 0,
        }
    }

    fn allow(&mut self, now: Instant) -> Option<u64> {
        if now.duration_since(self.window_start) >= LOG_WINDOW {
            self.window_start = now;
            self.used = 0;
        }
        if self.used >= LOG_LINES_PER_WINDOW {
            self.suppressed += 1;
            return None;
        }
        self.used += 1;
        Some(std::mem::take(&mut self.suppressed))
    }
}

/// Strip escape sequences and control characters from a forwarded line, then cap
/// its length, marking a truncation so it is never mistaken for the child having
/// stopped mid-sentence.
///
/// Not cosmetics. Every byte here was written by ANOTHER process into THIS
/// process's log stream, which is the log-injection surface: an escape sequence
/// reprograms the terminal of whoever is tailing, and a bare `\r` hides the rest
/// of the line from them. It is also load-bearing for readability today, because
/// the facade's `init_tracing` colourises unconditionally (it never tests for a
/// tty), so without this every forwarded line arrives wrapped in `\x1b[2m…\x1b[0m`
/// and the broker's own deliberate `with_ansi(false)` is undone by its child.
fn sanitize(line: &str) -> String {
    let mut out = String::with_capacity(line.len().min(LOG_LINE_MAX + 16));
    let mut chars = line.chars();
    let mut truncated = false;
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            match chars.next() {
                // CSI (`ESC [ … final`): the colour codes, the cursor moves.
                Some('[') => {
                    for n in chars.by_ref() {
                        if ('\u{40}'..='\u{7e}').contains(&n) {
                            break;
                        }
                    }
                }
                // OSC (`ESC ] … BEL|ESC`): window titles, hyperlinks.
                Some(']') => {
                    for n in chars.by_ref() {
                        if n == '\u{7}' || n == '\u{1b}' {
                            break;
                        }
                    }
                }
                // Any other two-character escape: the second char is consumed.
                _ => {}
            }
            continue;
        }
        // One space, so words do not run together where a tab or a stray control
        // byte used to be — and never the byte itself.
        let c = if c.is_control() { ' ' } else { c };
        if out.len() + c.len_utf8() > LOG_LINE_MAX {
            truncated = true;
            break;
        }
        out.push(c);
    }
    if truncated {
        out.push_str("… [truncated]");
    }
    out
}

// ---------------------------------------------------------------------------
// The supervisor.
// ---------------------------------------------------------------------------

/// What an operator can read about the child, without a new endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// `running` | `restarting` | `stopped`.
    pub phase: &'static str,
    pub pid: Option<u32>,
    pub restarts: u64,
    /// How the last child ended (`exit code 1`, `signal 9`), `None` before the
    /// first exit.
    pub last_exit: Option<String>,
    /// Milliseconds the CURRENT child has been up, `None` while restarting.
    pub uptime_ms: Option<u64>,
    /// The backoff in force, `None` while running.
    pub backoff_ms: Option<u64>,
}

struct Inner {
    phase: &'static str,
    pid: Option<u32>,
    restarts: u64,
    last_exit: Option<String>,
    started: Option<Instant>,
    backoff_ms: Option<u64>,
}

pub struct Supervisor {
    bin: PathBuf,
    queen_url: String,
    grace: Duration,
    /// Raised once by [`Supervisor::shutdown`]. `notify_one` and not
    /// `notify_waiters`: it stores a permit, so a stop that arrives while the loop
    /// is between awaits is not lost.
    stop: Notify,
    /// Raised by the loop as it returns, so `shutdown` can WAIT for the child to
    /// actually be gone rather than for the signal to have been sent.
    done: Notify,
    inner: Mutex<Inner>,
}

/// The process-global handle, so `/status` can read the child's state without
/// threading a field through `AppState` (the precedent is
/// `admission::set_global`: one arbiter per process, reached from the long tail).
/// Never set in the library target — the embedded `queen::Broker` has no HTTP
/// listener to point a facade at — so `status_value` there is always `None` and
/// `/status` renders exactly as it always did.
static GLOBAL: std::sync::OnceLock<Arc<Supervisor>> = std::sync::OnceLock::new();

/// The `kafka` block of `GET /status`, or `None` when embedded mode is off.
pub fn status_value() -> Option<serde_json::Value> {
    let s = GLOBAL.get()?.snapshot();
    Some(serde_json::json!({
        "mode": "embedded",
        "phase": s.phase,
        "pid": s.pid,
        "restarts": s.restarts,
        "lastExit": s.last_exit,
        "uptimeMs": s.uptime_ms,
        "backoffMs": s.backoff_ms,
    }))
}

impl Supervisor {
    pub fn snapshot(&self) -> Snapshot {
        let g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        Snapshot {
            phase: g.phase,
            pid: g.pid,
            restarts: g.restarts,
            last_exit: g.last_exit.clone(),
            uptime_ms: g.started.map(|t| t.elapsed().as_millis() as u64),
            backoff_ms: g.backoff_ms,
        }
    }

    fn set(&self, f: impl FnOnce(&mut Inner)) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        f(&mut g);
    }

    /// Stop the child and wait for it to be gone. Called from `main` once the
    /// serve loop has drained; bounded by the grace window plus a second, so a
    /// child that ignores both signals cannot hang the broker's exit.
    pub async fn shutdown(&self) {
        // The waiter is created BEFORE the signal: `notify_one` stores a permit so
        // it would be safe either way, but the order is the one that stays correct
        // if this ever becomes `notify_waiters`.
        let done = self.done.notified();
        self.stop.notify_one();
        if tokio::time::timeout(self.grace + Duration::from_secs(1), done)
            .await
            .is_err()
        {
            tracing::warn!(
                target: "kafka",
                grace_ms = self.grace.as_millis() as u64,
                "the queen-kafka child did not report gone within the grace window; \
                 the broker is exiting anyway (kill_on_drop reaps it)"
            );
        }
    }

    /// The command line, rebuilt per spawn so a replaced binary (an upgrade under
    /// a running broker) is picked up by the next restart.
    fn command(&self) -> Command {
        let mut cmd = Command::new(&self.bin);
        // The child inherits this process's environment — which is what makes
        // "every QUEEN_KAFKA_* knob forwards verbatim" true without listing one of
        // them here — with the broker's own secrets removed and QUEEN_URL pointed
        // at the listener we just bound.
        for k in STRIPPED_ENV {
            cmd.env_remove(k);
        }
        cmd.env("QUEEN_URL", &self.queen_url);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // The drop path (a panic, an abort that still unwinds the runtime):
            // tokio sends SIGKILL when the handle drops.
            .kill_on_drop(true);
        #[cfg(unix)]
        unsafe {
            cmd.pre_exec(|| {
                // Own process group, so a stop can signal the whole tree with one
                // kill(-pgid) and nothing the facade spawned outlives it.
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                // Linux only, and the only HARD guarantee against a SIGKILLed
                // broker (see the module header). Racy in one direction by
                // construction: if the parent died between fork and here, the
                // signal was already missed — which is why it is a backstop and
                // not the mechanism.
                #[cfg(target_os = "linux")]
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        cmd
    }

    /// Sleep, or return `false` because a stop arrived first.
    async fn sleep_or_stop(&self, d: Duration) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(d) => true,
            _ = self.stop.notified() => false,
        }
    }
}

/// SIGTERM the child's process group, escalate to SIGKILL after the grace window.
/// Returns how it ended, for the log line.
async fn terminate(child: &mut Child, grace: Duration) -> &'static str {
    let Some(pid) = child.id() else {
        return "already-exited";
    };
    #[cfg(unix)]
    {
        signal_group(pid, libc::SIGTERM);
        if tokio::time::timeout(grace, child.wait()).await.is_ok() {
            return "sigterm";
        }
        signal_group(pid, libc::SIGKILL);
        let _ = child.wait().await;
        "sigkill (grace expired)"
    }
    #[cfg(not(unix))]
    {
        let _ = grace;
        let _ = child.kill().await;
        "killed"
    }
}

/// Signal the whole process group. Negative pid IS the group, and the group is
/// the child's own because `pre_exec` made it a leader.
#[cfg(unix)]
fn signal_group(pid: u32, sig: i32) {
    // Safe: `kill` on a pid we own, with a constant signal. A failure means the
    // group is already gone, which is the outcome we wanted.
    unsafe {
        libc::kill(-(pid as i32), sig);
    }
}

/// How a child ended, in the words an operator needs to tell a crash from a stop.
fn describe(status: &std::io::Result<std::process::ExitStatus>) -> String {
    match status {
        Err(e) => format!("wait failed: {e}"),
        Ok(s) => {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                if let Some(sig) = s.signal() {
                    return format!("signal {sig}");
                }
            }
            match s.code() {
                Some(c) => format!("exit code {c}"),
                None => "ended without a code".to_string(),
            }
        }
    }
}

/// Forward one of the child's streams into the broker's log, tagged and budgeted.
fn pump<R>(reader: Option<R>, stream: &'static str, pid: u32)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let Some(reader) = reader else { return };
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        let mut budget = LineBudget::new(Instant::now());
        while let Ok(Some(line)) = lines.next_line().await {
            match budget.allow(Instant::now()) {
                None => continue,
                Some(0) => {}
                Some(n) => tracing::warn!(
                    target: "kafka",
                    pid,
                    stream,
                    suppressed = n,
                    "queen-kafka output rate-limited"
                ),
            }
            // The child stamps its own line with its own timestamp and level; this
            // is a forward, not a re-format, so the line goes through as it was
            // written and the broker's fields say only where it came from.
            tracing::info!(target: "kafka", pid, stream, "{}", sanitize(&line));
        }
    });
}

/// Start supervising. Call AFTER the HTTP listener is bound: `queen_url` names it,
/// and a child that dialled before the socket existed would burn a restart on
/// nothing. (The facade itself never calls the broker at boot in single-node mode
/// — the first call is a client's Metadata — so this is belt and braces.)
pub fn spawn(cfg: &KafkaFacadeConfig, bin: PathBuf, queen_url: String) -> Arc<Supervisor> {
    let sup = Arc::new(Supervisor {
        bin,
        queen_url,
        grace: Duration::from_millis(cfg.shutdown_grace_ms),
        stop: Notify::new(),
        done: Notify::new(),
        inner: Mutex::new(Inner {
            phase: "restarting",
            pid: None,
            restarts: 0,
            last_exit: None,
            started: None,
            backoff_ms: None,
        }),
    });
    // Best-effort: a second call would mean two supervisors, which cannot happen
    // (one `spawn` in `main`), and the first one is the one `/status` should read.
    let _ = GLOBAL.set(sup.clone());
    tokio::spawn(run(sup.clone()));
    sup
}

async fn run(sup: Arc<Supervisor>) {
    let mut delay = BACKOFF_INITIAL;
    loop {
        let started = Instant::now();
        let mut child = match sup.command().spawn() {
            Ok(c) => c,
            Err(e) => {
                // The binary was there at boot (preflight) and is not now, or the
                // exec failed. Restartable — an upgrade that replaces the file in
                // place is exactly this — so it takes the same ladder as a crash.
                let wait = backoff_after(delay, Duration::ZERO);
                tracing::error!(
                    target: "kafka",
                    bin = %sup.bin.display(),
                    error = %e,
                    backoff_ms = wait.as_millis() as u64,
                    "cannot spawn the queen-kafka facade; retrying after backoff"
                );
                sup.set(|g| {
                    g.phase = "restarting";
                    g.pid = None;
                    g.started = None;
                    g.last_exit = Some(format!("spawn failed: {e}"));
                    g.backoff_ms = Some(wait.as_millis() as u64);
                });
                if !sup.sleep_or_stop(wait).await {
                    break;
                }
                delay = next_backoff(wait);
                continue;
            }
        };
        let pid = child.id().unwrap_or(0);
        pump(child.stdout.take(), "stdout", pid);
        pump(child.stderr.take(), "stderr", pid);
        sup.set(|g| {
            g.phase = "running";
            g.pid = Some(pid);
            g.started = Some(started);
            g.backoff_ms = None;
        });
        tracing::info!(
            target: "kafka",
            pid,
            bin = %sup.bin.display(),
            queen_url = %sup.queen_url,
            "queen-kafka facade started (embedded)"
        );

        let exit = tokio::select! {
            r = child.wait() => Some(r),
            _ = sup.stop.notified() => None,
        };

        let Some(status) = exit else {
            // Ordered shutdown: the broker is going away and takes the child with
            // it. Not a restart, and not an error.
            let how = terminate(&mut child, sup.grace).await;
            tracing::info!(target: "kafka", pid, how, "queen-kafka facade stopped with the broker");
            sup.set(|g| {
                g.phase = "stopped";
                g.pid = None;
                g.started = None;
                g.backoff_ms = None;
            });
            break;
        };

        let uptime = started.elapsed();
        let reason = describe(&status);
        let wait = backoff_after(delay, uptime);
        let restarts = {
            let mut n = 0;
            sup.set(|g| {
                g.restarts += 1;
                n = g.restarts;
                g.phase = "restarting";
                g.pid = None;
                g.started = None;
                g.last_exit = Some(reason.clone());
                g.backoff_ms = Some(wait.as_millis() as u64);
            });
            n
        };
        // ERROR, always, and unsampled: the backoff below is what bounds this to
        // two lines a minute in a permanent crash-loop, so a gate here would only
        // hide the first crash — the one worth reading.
        tracing::error!(
            target: "kafka",
            pid,
            reason = %reason,
            uptime_ms = uptime.as_millis() as u64,
            restarts,
            backoff_ms = wait.as_millis() as u64,
            "queen-kafka facade EXITED; restarting after backoff"
        );
        if !sup.sleep_or_stop(wait).await {
            sup.set(|g| {
                g.phase = "stopped";
                g.backoff_ms = None;
            });
            break;
        }
        delay = next_backoff(wait);
    }
    sup.done.notify_one();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_binary_defaults_to_the_one_beside_the_broker() {
        let exe = PathBuf::from("/app/bin/queen");
        assert_eq!(
            resolve_bin("", Some(&exe)),
            PathBuf::from("/app/bin/queen-kafka")
        );
        // Present-but-empty is unset, the same rule every other knob follows.
        assert_eq!(
            resolve_bin("   ", Some(&exe)),
            PathBuf::from("/app/bin/queen-kafka")
        );
        // ...and an explicit path wins, trimmed.
        assert_eq!(
            resolve_bin(" /usr/local/bin/qk ", Some(&exe)),
            PathBuf::from("/usr/local/bin/qk")
        );
        // No executable path at all: the bare name, so PATH still has a chance.
        assert_eq!(resolve_bin("", None), PathBuf::from(FACADE_BIN));
    }

    #[test]
    fn a_missing_binary_names_the_fix() {
        let err = preflight(
            Path::new("/nonexistent/queen-kafka"),
            Some("kafka.example.com:9092"),
        )
        .unwrap_err();
        assert!(err.contains("QUEEN_KAFKA_BIN"), "{err}");
        assert!(err.contains("/nonexistent/queen-kafka"), "{err}");
        assert!(err.contains("QUEEN_KAFKA_EMBEDDED"), "{err}");
    }

    #[test]
    fn a_missing_advertised_address_is_refused_at_boot_not_in_a_crash_loop() {
        // The broker's own executable exists, so the binary check passes and the
        // second gate is the one under test.
        let exe = std::env::current_exe().unwrap();
        for absent in [None, Some(""), Some("   ")] {
            let err = preflight(&exe, absent).unwrap_err();
            assert!(err.contains("QUEEN_KAFKA_ADVERTISED_ADDR"), "{err}");
            assert!(err.contains("crash-loop"), "{err}");
        }
        assert!(preflight(&exe, Some("kafka.example.com:9092")).is_ok());
    }

    #[test]
    fn the_child_is_pointed_at_the_address_the_listener_actually_bound() {
        let url = |s: &str| loopback_url(&s.parse().unwrap());
        // A wildcard bind is not an address anything can dial.
        assert_eq!(url("0.0.0.0:6632"), "http://127.0.0.1:6632");
        assert_eq!(url("[::]:6632"), "http://[::1]:6632");
        // A pinned bind is used as it is: the child is in this host's namespace,
        // so whatever the broker answers on, it can reach.
        assert_eq!(url("127.0.0.1:32601"), "http://127.0.0.1:32601");
        assert_eq!(url("10.0.0.7:6632"), "http://10.0.0.7:6632");
        assert_eq!(url("[::1]:6632"), "http://[::1]:6632");
    }

    #[test]
    fn the_auth_advisory_fires_only_when_the_child_would_get_401s() {
        // Auth off: nothing to say.
        assert!(auth_advisory(false, None, None).is_none());
        // Auth on with a credential, in either of its two legitimate shapes.
        assert!(auth_advisory(true, Some("eyJ.a.b"), None).is_none());
        assert!(auth_advisory(true, None, Some("plain")).is_none());
        // Auth on with neither — and empty is not a credential.
        for token in [None, Some(""), Some("  ")] {
            let msg = auth_advisory(true, token, None).unwrap();
            assert!(msg.contains("QUEEN_TOKEN"), "{msg}");
            assert!(msg.contains("QUEEN_KAFKA_SASL"), "{msg}");
            assert!(msg.contains("401"), "{msg}");
        }
    }

    #[test]
    fn the_backoff_doubles_to_a_ceiling_and_a_healthy_run_resets_it() {
        let mut d = BACKOFF_INITIAL;
        let mut waits = Vec::new();
        for _ in 0..8 {
            let w = backoff_after(d, Duration::from_secs(1)); // always a fast crash
            waits.push(w.as_secs());
            d = next_backoff(w);
        }
        assert_eq!(waits, vec![1, 2, 4, 8, 16, 30, 30, 30]);
        // An hour of health puts the ladder back on its first rung.
        assert_eq!(backoff_after(d, HEALTHY_RUN), BACKOFF_INITIAL);
        assert_eq!(
            backoff_after(d, HEALTHY_RUN - Duration::from_secs(1)),
            d,
            "one second short of healthy is not healthy"
        );
    }

    #[test]
    fn the_output_budget_passes_a_window_then_reports_what_it_dropped() {
        let t0 = Instant::now();
        let mut b = LineBudget::new(t0);
        for _ in 0..LOG_LINES_PER_WINDOW {
            assert_eq!(b.allow(t0), Some(0));
        }
        // Over budget inside the window: dropped, silently, and counted.
        for _ in 0..50 {
            assert_eq!(b.allow(t0), None);
        }
        // The next window's first line carries the count, once.
        let t1 = t0 + LOG_WINDOW;
        assert_eq!(b.allow(t1), Some(50));
        assert_eq!(
            b.allow(t1),
            Some(0),
            "the count is reported once, not per line"
        );
    }

    #[test]
    fn a_forwarded_line_is_sanitized_and_clipped_on_a_char_boundary() {
        assert_eq!(sanitize("short"), "short");
        // Multi-byte, so a naive byte slice at the cap would panic.
        let long = "à".repeat(LOG_LINE_MAX);
        let out = sanitize(&long);
        assert!(out.ends_with("… [truncated]"), "{out}");
        assert!(out.len() < long.len());
    }

    /// The child writes into the broker's log stream, so what it writes is
    /// untrusted input to whoever is tailing it — and today it really does arrive
    /// colourised, because the facade's subscriber never tests for a tty.
    #[test]
    fn escape_sequences_and_control_bytes_never_reach_the_brokers_log() {
        // The exact shape the facade emits (dim timestamp, red level, dim target).
        let real = "\u{1b}[2m2026-08-29T18:14:50Z\u{1b}[0m \u{1b}[31mERROR\u{1b}[0m \
                    \u{1b}[2mboot\u{1b}[0m: FATAL: bad partition count";
        let out = sanitize(real);
        assert!(!out.contains('\u{1b}'), "{out}");
        assert!(
            !out.contains('['),
            "the CSI body must go with the escape: {out}"
        );
        assert!(out.contains("FATAL: bad partition count"), "{out}");
        assert!(
            out.contains("ERROR"),
            "the words survive, only the colour goes: {out}"
        );
        // An OSC hyperlink, and the classic line-hiding carriage return.
        assert_eq!(sanitize("a\u{1b}]8;;http://x\u{7}b"), "ab");
        assert_eq!(sanitize("visible\rhidden"), "visible hidden");
        assert_eq!(sanitize("tab\there"), "tab here");
    }

    /// The child's credential must survive the strip; the broker's must not.
    #[test]
    fn the_brokers_secrets_do_not_reach_the_child_but_its_own_token_does() {
        assert!(STRIPPED_ENV.contains(&"PG_PASSWORD"));
        assert!(STRIPPED_ENV.contains(&"JWT_SECRET"));
        assert!(!STRIPPED_ENV.contains(&"QUEEN_TOKEN"));
        // Nothing QUEEN_KAFKA_* is stripped: the passthrough is the contract.
        assert!(!STRIPPED_ENV.iter().any(|k| k.starts_with("QUEEN_KAFKA_")));
    }
}
