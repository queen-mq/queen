//! EMBEDDED MODE for the SQS/SNS wire facade (PLAN_QUEEN_SQS.md, Architecture).
//!
//! `QUEEN_SQS_EMBEDDED=true` makes this broker process SPAWN and SUPERVISE the
//! `queen-sqs` binary as a child, wired to this broker's own HTTP listener over
//! loopback. One deployment, two processes.
//!
//! This is the TWIN of `kafka_facade.rs`, deliberately written as a twin and not
//! as a shared generic: the two supervise different binaries with different
//! boot-time preconditions, and a change to one must be a decision about that
//! one. Everything below that is identical to the Kafka supervisor is identical
//! ON PURPOSE — the discipline is the feature, and an operator who has read one
//! module has read both.
//!
//! ## Why a child process and not a library
//! The facade is an HTTP server with its own accept loop, its own connection
//! budget, its own request decoders (SigV4 over bytes the CLIENT chose, two wire
//! codecs, base64 and XML) and its own crash modes. Linked in-process, a
//! malformed request that walks a decoder into an allocation failure would take
//! the BROKER down with it — and `panic = "abort"` (see the header of `obs.rs`)
//! means "take down" is literal, not "lose one task". A child keeps the blast
//! radius at the facade: it dies, the broker keeps serving, and this supervisor
//! brings it back. The isolation is the point; the packaging is the convenience.
//!
//! ## What is guaranteed on shutdown, and what is best-effort
//! The child is put in its OWN process group (`setpgid`), so a stop signals the
//! whole group and any grandchild dies with it rather than being re-parented.
//!
//! * broker **SIGTERM / Ctrl-C** — guaranteed: `main` awaits [`Supervisor::shutdown`]
//!   after the serve loop drains, which sends SIGTERM to the group and escalates to
//!   SIGKILL after `QUEEN_SQS_SHUTDOWN_GRACE_MS`.
//! * broker **panic / unexpected drop** — `kill_on_drop(true)`: tokio reaps the
//!   child when the `Child` handle drops.
//! * broker **SIGKILL** — platform-dependent, and the honest answer differs:
//!   on **Linux** the child sets `PR_SET_PDEATHSIG(SIGKILL)`, so the kernel kills it
//!   when the parent dies, whatever the parent died of. On **macOS/BSD** there is no
//!   equivalent: a SIGKILLed broker leaves the facade running, re-parented to init,
//!   still holding its SQS port. That is a dev-machine caveat, not a production
//!   one (production is Linux), and it is stated rather than papered over.
//!
//! ## One variable, two readers
//! `QUEEN_SQS_SHUTDOWN_GRACE_MS` is read HERE (how long a stopping child has
//! between SIGTERM and SIGKILL) and, because the child inherits this
//! environment, by the child itself (how long it drains in-flight requests after
//! it stops accepting). Set, the two agree by construction. Left unset they do
//! not: this supervisor's default is 5s and the facade's own is 25s, because the
//! facade sizes its default to outlive one 20s long poll. A deployment that
//! wants a `ReceiveMessage(WaitTimeSeconds=20)` answered rather than cut at a
//! rolling restart sets the variable explicitly — both sides then read the one
//! number the operator wrote. It is not defaulted to 25s here because the
//! broker's own exit would then wait half a minute on a child that, in the
//! common case, has nothing to drain.
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

use crate::config::SqsFacadeConfig;

/// The binary this supervises, and the file name looked for next to the broker's
/// own executable when `QUEEN_SQS_BIN` is unset.
pub const FACADE_BIN: &str = "queen-sqs";

/// Backoff ladder: the wait before re-spawning a child that exited. Doubling from
/// one second to a thirty second ceiling — long enough that a permanently broken
/// facade costs nothing, short enough that a one-off crash is invisible to clients
/// (an AWS SDK retries with its own backoff, and every piece of state the facade
/// holds — the queue registry, the offsets, the delete-sets — lives in Queen).
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
/// configuration surface, `QUEEN_SQS_*` included (`_LISTEN`, `_AUTH`,
/// `_CREDENTIALS`, `_REGION`, `_ACCOUNT`, `_RECEIVE_MODE`, `_DEFAULT_PARTITIONS`,
/// `_HANDLE_SECRET`, `_TLS_*`), plus `QUEEN_TOKEN`, `LOG_LEVEL`/`RUST_LOG` and
/// `QUEEN_LOG_JSON`. These four are the broker's own secrets (the set
/// `config::log_effective` masks) and the facade reads none of them: a database
/// password has no business being readable in a second process's environment just
/// because that process happens to be colocated.
///
/// `QUEEN_TOKEN` is deliberately NOT here. It is a secret, but it is the CHILD's
/// credential — the one it presents to this broker for a principal whose
/// `QUEEN_SQS_CREDENTIALS` entry carries no token of its own — so removing it
/// would be removing the feature.
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

/// Where the facade binary is. `QUEEN_SQS_BIN` when set, otherwise the file
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

/// Does `QUEEN_SQS_AUTH` select SigV4? Unset is SigV4 — the facade's default is
/// the safe one, so the common misconfiguration is the DEFAULT mode with no keys.
///
/// A value this function does not recognize answers `false`, which is not a
/// judgement about the value: it is the deliberate refusal to guess. The child
/// owns that parse and refuses with the line that names the legal spellings
/// (`protocols/queen-sqs/src/config.rs`), and a broker that shouted about credentials
/// instead would be answering a question the operator did not ask.
fn sigv4_selected(auth: Option<&str>) -> bool {
    match auth.map(str::trim).filter(|v| !v.is_empty()) {
        None => true,
        Some(v) => matches!(
            v.to_ascii_lowercase().as_str(),
            "sigv4" | "sig-v4" | "v4" | "on"
        ),
    }
}

/// The misconfiguration that must be caught at BOOT rather than discovered as a
/// crash-loop. Both cases are conditions no amount of restarting can resolve, and
/// the broker's own convention for those is `obs::fatal` (see
/// `AuthConfig::validate`, `checked_bind_addr`, the KV trusted-proxy interlock).
///
/// The second gate is the SQS twin of the Kafka supervisor's advertised-address
/// gate: the one setting the facade cannot default. `QUEEN_SQS_AUTH` defaults to
/// `sigv4`, and SigV4 verification needs the secret half of a keypair to verify
/// AGAINST — there is no default keypair, and inventing one would be a listener
/// with a published password. Started that way the child exits 1 on every spawn
/// forever, and the operator reads a backoff ladder instead of the one sentence
/// that fixes it.
pub fn preflight(bin: &Path, auth: Option<&str>, credentials: Option<&str>) -> Result<(), String> {
    if !bin.is_file() {
        return Err(format!(
            "QUEEN_SQS_EMBEDDED=true but no {FACADE_BIN} binary at {}. \
             Embedded mode spawns the SQS facade as a child process; it cannot spawn a file \
             that is not there. Point QUEEN_SQS_BIN at the binary, or put {FACADE_BIN} next \
             to the broker executable (the queen Docker image ships both in /app/bin), or unset \
             QUEEN_SQS_EMBEDDED to run the broker alone.",
            bin.display()
        ));
    }
    let has_credentials = credentials.map(str::trim).is_some_and(|v| !v.is_empty());
    if sigv4_selected(auth) && !has_credentials {
        return Err(
            "QUEEN_SQS_EMBEDDED=true with QUEEN_SQS_AUTH=sigv4 (the default) and no \
             QUEEN_SQS_CREDENTIALS. SigV4 is verified against the secret half of a keypair the \
             facade must hold, and there is no default keypair: started this way the listener \
             answers every request InvalidClientTokenId, which reads to a client like a wrong \
             access key rather than like a server with no keys. Set \
             QUEEN_SQS_CREDENTIALS=akid:secret:token[,…], or QUEEN_SQS_AUTH=off for a \
             development listener that accepts anything, or unset QUEEN_SQS_EMBEDDED. It is \
             refused here rather than in the child because a child that can never boot is a \
             crash-loop, not a configuration error an operator can read."
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

/// The URL the child is given for `QUEEN_URL`: the operator's if they set one,
/// the loopback we bound otherwise.
///
/// Loopback is right for the single-deployment case this mode was built for. It
/// is WRONG for a Queen Cloud cell, where the facade must reach the broker
/// THROUGH the cell proxy so that every SQS request crosses the proxy's auth,
/// tenant scoping, metering and quotas. There is no way to express that with an
/// injected loopback, and an operator who sets `QUEEN_URL` has said something
/// specific that this supervisor has no better information than.
///
/// Empty is not "set": `QUEEN_URL=` is an unset variable spelled by a Helm
/// template that resolved to nothing, and inheriting it would send the child to
/// its own boot-time refusal (protocols/queen-sqs/src/queen.rs, `normalize_base_url`).
/// So the same trim-and-reject-empty rule every other knob here follows.
pub fn child_queen_url(inherited: Option<&str>, loopback: &str) -> String {
    match inherited.map(str::trim).filter(|v| !v.is_empty()) {
        Some(explicit) => explicit.to_string(),
        None => loopback.to_string(),
    }
}

/// Which branch [`child_queen_url`] took, for the one line an operator greps
/// when they need to know whether the facade is hairpinning through a proxy or
/// talking to the listener in its own process.
fn queen_url_source(inherited: Option<&str>) -> &'static str {
    match inherited.map(str::trim).filter(|v| !v.is_empty()) {
        Some(_) => "QUEEN_URL (explicit)",
        None => "loopback (bound listener)",
    }
}

/// Does any `QUEEN_SQS_CREDENTIALS` entry carry a Queen token of its own?
///
/// The spec is `akid:secret:token[,akid:secret:token…]` and the token is
/// optional, so this reads the third colon-field of each comma-separated entry.
/// It is a heuristic for a WARN and nothing else — the authoritative parse is
/// the child's (`protocols/queen-sqs/src/credentials.rs`), which is also the only place
/// that may reject a malformed spec.
fn credentials_carry_tokens(spec: Option<&str>) -> bool {
    let Some(spec) = spec else { return false };
    spec.split(',').any(|entry| {
        entry
            .splitn(3, ':')
            .nth(2)
            .is_some_and(|token| !token.trim().is_empty())
    })
}

/// The one auth posture embedded mode cannot fix by itself, worded once.
///
/// With `JWT_ENABLED=true` the broker requires a token on every data path the
/// facade uses (push, pop, ack, lease, kv, timers, transaction — none of them are
/// in the default `JWT_SKIP_PATHS`), and the loopback hop is not exempt: embedded
/// mode does not get a private door into the broker, because a private door is
/// exactly the kind of thing that is later found open from somewhere else. So the
/// child needs a credential like any other client, and it has two legitimate ways
/// to hold one: `QUEEN_TOKEN` (one identity for the whole listener) or a Queen
/// token on each `QUEEN_SQS_CREDENTIALS` entry (each SigV4 principal presents its
/// own, the Cloud shape). With neither, every send and receive answers 401 — a
/// WARN and not a fatal, because the operator may be mid-rollout and the facade is
/// not load-bearing for the broker.
pub fn auth_advisory(
    broker_auth_on: bool,
    token: Option<&str>,
    credentials: Option<&str>,
) -> Option<String> {
    let has = |v: Option<&str>| v.map(str::trim).is_some_and(|v| !v.is_empty());
    if !broker_auth_on || has(token) || credentials_carry_tokens(credentials) {
        return None;
    }
    Some(
        "JWT_ENABLED=true with QUEEN_SQS_EMBEDDED=true, but the facade has no credential: \
         every send and receive it makes will be answered 401. Give the child a QUEEN_TOKEN, \
         or put a Queen token on each QUEEN_SQS_CREDENTIALS entry \
         (akid:secret:token) so every SigV4 principal presents its own. \
         The hop the child makes is authenticated like any other client's, whether it is the \
         loopback this supervisor injects or the QUEEN_URL an operator set (see \
         child_queen_url) — there is no exempt door."
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
/// of the line from them. It is also load-bearing for readability, because the
/// facade's own subscriber colourises without testing for a tty, so without this
/// every forwarded line arrives wrapped in `\x1b[2m…\x1b[0m` and the broker's
/// deliberate `with_ansi(false)` is undone by its child.
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
    /// Which branch [`child_queen_url`] took. Logged at every spawn, never
    /// acted on — an operator debugging a hairpin must be able to read "the
    /// facade is calling the proxy" out of the boot log.
    queen_url_from: &'static str,
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
/// Its own cell, separate from the Kafka supervisor's: the two facades are
/// independent and a deployment may run either, both or neither. Never set in the
/// library target — the embedded `queen::Broker` has no HTTP listener to point a
/// facade at — so `status_value` there is always `None` and `/status` renders
/// exactly as it always did.
static GLOBAL: std::sync::OnceLock<Arc<Supervisor>> = std::sync::OnceLock::new();

/// The `sqs` block of `GET /status`, or `None` when embedded mode is off.
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
                target: "sqs",
                grace_ms = self.grace.as_millis() as u64,
                "the queen-sqs child did not report gone within the grace window; \
                 the broker is exiting anyway (kill_on_drop reaps it)"
            );
        }
    }

    /// The command line, rebuilt per spawn so a replaced binary (an upgrade under
    /// a running broker) is picked up by the next restart.
    fn command(&self) -> Command {
        let mut cmd = Command::new(&self.bin);
        // The child inherits this process's environment — which is what makes
        // "every QUEEN_SQS_* knob forwards verbatim" true without listing one of
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
                    target: "sqs",
                    pid,
                    stream,
                    suppressed = n,
                    "queen-sqs output rate-limited"
                ),
            }
            // The child stamps its own line with its own timestamp and level; this
            // is a forward, not a re-format, so the line goes through as it was
            // written and the broker's fields say only where it came from.
            tracing::info!(target: "sqs", pid, stream, "{}", sanitize(&line));
        }
    });
}

/// Start supervising. Call AFTER the HTTP listener is bound: `loopback` names it,
/// and a child that dialled before the socket existed would burn a restart on
/// nothing.
///
/// `loopback` is the DEFAULT and not the answer: an explicitly set `QUEEN_URL` in
/// this process's own environment wins ([`child_queen_url`]), which is what makes
/// a Cloud cell possible — there the facade must reach the broker through the
/// proxy, and no address derived from the local listener can express that.
pub fn spawn(cfg: &SqsFacadeConfig, bin: PathBuf, loopback: String) -> Arc<Supervisor> {
    let inherited = std::env::var("QUEEN_URL").ok();
    let sup = Arc::new(Supervisor {
        bin,
        queen_url: child_queen_url(inherited.as_deref(), &loopback),
        queen_url_from: queen_url_source(inherited.as_deref()),
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
                    target: "sqs",
                    bin = %sup.bin.display(),
                    error = %e,
                    backoff_ms = wait.as_millis() as u64,
                    "cannot spawn the queen-sqs facade; retrying after backoff"
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
            target: "sqs",
            pid,
            bin = %sup.bin.display(),
            queen_url = %sup.queen_url,
            queen_url_from = sup.queen_url_from,
            "queen-sqs facade started (embedded)"
        );

        let exit = tokio::select! {
            r = child.wait() => Some(r),
            _ = sup.stop.notified() => None,
        };

        let Some(status) = exit else {
            // Ordered shutdown: the broker is going away and takes the child with
            // it. Not a restart, and not an error.
            let how = terminate(&mut child, sup.grace).await;
            tracing::info!(target: "sqs", pid, how, "queen-sqs facade stopped with the broker");
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
            target: "sqs",
            pid,
            reason = %reason,
            uptime_ms = uptime.as_millis() as u64,
            restarts,
            backoff_ms = wait.as_millis() as u64,
            "queen-sqs facade EXITED; restarting after backoff"
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
            PathBuf::from("/app/bin/queen-sqs")
        );
        // Present-but-empty is unset, the same rule every other knob follows.
        assert_eq!(
            resolve_bin("   ", Some(&exe)),
            PathBuf::from("/app/bin/queen-sqs")
        );
        // ...and an explicit path wins, trimmed.
        assert_eq!(
            resolve_bin(" /usr/local/bin/qs ", Some(&exe)),
            PathBuf::from("/usr/local/bin/qs")
        );
        // No executable path at all: the bare name, so PATH still has a chance.
        assert_eq!(resolve_bin("", None), PathBuf::from(FACADE_BIN));
    }

    #[test]
    fn a_missing_binary_names_the_fix() {
        let err = preflight(Path::new("/nonexistent/queen-sqs"), Some("off"), None).unwrap_err();
        assert!(err.contains("QUEEN_SQS_BIN"), "{err}");
        assert!(err.contains("/nonexistent/queen-sqs"), "{err}");
        assert!(err.contains("QUEEN_SQS_EMBEDDED"), "{err}");
    }

    /// The SQS twin of the advertised-address gate: SigV4 is the DEFAULT, and a
    /// verifier with no keypair refuses every request forever.
    #[test]
    fn sigv4_without_credentials_is_refused_at_boot_not_in_a_crash_loop() {
        // The broker's own executable exists, so the binary check passes and the
        // second gate is the one under test.
        let exe = std::env::current_exe().unwrap();
        for auth in [None, Some(""), Some("  "), Some("sigv4"), Some("ON")] {
            for creds in [None, Some(""), Some("   ")] {
                let err = preflight(&exe, auth, creds).unwrap_err();
                assert!(err.contains("QUEEN_SQS_CREDENTIALS"), "{err}");
                assert!(err.contains("crash-loop"), "{err}");
            }
        }
        // Either half fixes it: keys, or the development posture.
        assert!(preflight(&exe, None, Some("AKID:secret:tok")).is_ok());
        assert!(preflight(&exe, Some("off"), None).is_ok());
        assert!(preflight(&exe, Some("none"), None).is_ok());
        assert!(preflight(&exe, Some("DISABLED"), None).is_ok());
        // A value neither parser recognizes belongs to the CHILD's refusal, which
        // names the legal spellings; the broker does not guess a mode.
        assert!(preflight(&exe, Some("iam"), None).is_ok());
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

    /// The Cloud case. A cell's facade must call the broker THROUGH the proxy,
    /// and the only way to say so is an explicit `QUEEN_URL`.
    #[test]
    fn an_explicit_queen_url_wins_over_the_loopback() {
        assert_eq!(
            child_queen_url(Some("http://proxy:6711"), "http://127.0.0.1:6632"),
            "http://proxy:6711"
        );
        // Trimmed, the same rule `resolve_bin` follows.
        assert_eq!(
            child_queen_url(Some("  http://proxy:6711  "), "http://127.0.0.1:6632"),
            "http://proxy:6711"
        );
        assert_eq!(
            queen_url_source(Some("http://proxy:6711")),
            "QUEEN_URL (explicit)"
        );
    }

    /// `QUEEN_URL=` is an unset variable spelled by a Helm template that
    /// resolved to nothing. Inheriting it would send the child to its own
    /// boot-time refusal instead of to this broker.
    #[test]
    fn an_empty_queen_url_is_not_set() {
        for empty in ["", "   ", "\t"] {
            assert_eq!(
                child_queen_url(Some(empty), "http://127.0.0.1:6632"),
                "http://127.0.0.1:6632",
                "{empty:?}"
            );
            assert_eq!(queen_url_source(Some(empty)), "loopback (bound listener)");
        }
    }

    /// The OSS shape is unchanged: nothing set, the loopback we bound.
    #[test]
    fn no_queen_url_is_still_the_loopback_we_bound() {
        assert_eq!(
            child_queen_url(None, "http://127.0.0.1:6632"),
            "http://127.0.0.1:6632"
        );
        assert_eq!(queen_url_source(None), "loopback (bound listener)");
    }

    #[test]
    fn the_auth_advisory_fires_only_when_the_child_would_get_401s() {
        // Auth off: nothing to say.
        assert!(auth_advisory(false, None, None).is_none());
        // Auth on with a credential, in either of its two legitimate shapes.
        assert!(auth_advisory(true, Some("eyJ.a.b"), None).is_none());
        assert!(auth_advisory(true, None, Some("AKID:secret:tok")).is_none());
        // ...including when only ONE principal of several carries its own token:
        // the rest fall back to QUEEN_TOKEN, and this warning is about the case
        // where there is nothing to fall back to at all.
        assert!(auth_advisory(true, None, Some("A:s,B:s:tok")).is_none());
        // Auth on with neither — and a token-less credential list is not a
        // credential for the hop to the broker, only for the hop to the facade.
        for creds in [None, Some(""), Some("AKID:secret"), Some("AKID:secret:  ")] {
            let msg = auth_advisory(true, None, creds).unwrap();
            assert!(msg.contains("QUEEN_TOKEN"), "{msg}");
            assert!(msg.contains("QUEEN_SQS_CREDENTIALS"), "{msg}");
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
    /// untrusted input to whoever is tailing it.
    #[test]
    fn escape_sequences_and_control_bytes_never_reach_the_brokers_log() {
        let real = "\u{1b}[2m2026-08-30T18:14:50Z\u{1b}[0m \u{1b}[31mERROR\u{1b}[0m \
                    \u{1b}[2mboot\u{1b}[0m: FATAL: no credentials";
        let out = sanitize(real);
        assert!(!out.contains('\u{1b}'), "{out}");
        assert!(
            !out.contains('['),
            "the CSI body must go with the escape: {out}"
        );
        assert!(out.contains("FATAL: no credentials"), "{out}");
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
        // Nothing QUEEN_SQS_* is stripped: the passthrough is the contract, and
        // QUEEN_SQS_CREDENTIALS in particular is the child's whole identity
        // surface — it is a secret, and it is the CHILD's secret.
        assert!(!STRIPPED_ENV.iter().any(|k| k.starts_with("QUEEN_SQS_")));
    }
}
