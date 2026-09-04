//! EMBEDDED MODE for the S3 / data-lake sink connector (PLAN_S3_SINK.md §3, §10 P2).
//!
//! `QUEEN_S3_EMBEDDED=true` makes this broker process SPAWN and SUPERVISE the
//! `queen-s3` binary as a child, wired to this broker's own HTTP listener over
//! loopback. One deployment, two processes.
//!
//! This is the THIRD TWIN of `kafka_facade.rs` and `sqs_facade.rs`, deliberately
//! written as a twin and not as a shared generic: the three supervise different
//! binaries with different boot-time preconditions, and a change to one must be a
//! decision about that one. Everything below that is identical to the two facade
//! supervisors is identical ON PURPOSE — the discipline is the feature, and an
//! operator who has read one module has read all three.
//!
//! ## The one place this twin is NOT the facades
//! The shutdown grace is `QUEEN_S3_SHUTDOWN_GRACE_MS` and it defaults to
//! **30 seconds**, where the facades default to five. A stopping facade is
//! closing sockets: every offset, queue and delete-set it holds already lives in
//! Queen, so the window only has to cover a process that is mid-syscall. A
//! stopping SINK has an OPEN WINDOW — buffered records, an S3 upload possibly
//! in flight, and a KV compare-and-set that turns that upload into a committed
//! window (plan §4.3). Cut before the commit, the work is not lost (the next
//! start re-reads from the last committed pointer and rewrites the window) but
//! it IS repeated, and the orphaned object is rewritten under the same
//! deterministic key. Thirty seconds is the window that lets the common close
//! finish instead. Floored at 100 ms, like the twins, because a grace of zero is
//! a SIGKILL with extra steps.
//!
//! That is the one difference in BEHAVIOUR. [`preflight`] also carries one gate
//! the twins do not — the binary's execute bit — which is not a difference of
//! design but a check the twins would be no worse for having.
//!
//! ## Why a child process and not a library
//! The sink is a long-running writer with its own accept loop (`/healthz`,
//! `/metrics`), its own compression and Parquet arenas, its own S3 client and its
//! own crash modes. Linked in-process, an allocation failure in a writer sized
//! from `QUEEN_S3_TARGET_MB` would take the BROKER down with it — and
//! `panic = "abort"` (see the header of `obs.rs`) means "take down" is literal,
//! not "lose one task". A child keeps the blast radius at the sink: it dies, the
//! broker keeps serving, and this supervisor brings it back. The isolation is the
//! point; the packaging is the convenience.
//!
//! The sink is also a CLIENT and nothing more (plan §3): it holds no database
//! connection and owns nothing durable except two KV keys per (sink, queue) and
//! what it writes to the bucket. Embedded mode changes where the process is
//! started, not what it is allowed to do.
//!
//! ## What is guaranteed on shutdown, and what is best-effort
//! The child is put in its OWN process group (`setpgid`), so a stop signals the
//! whole group and any grandchild dies with it rather than being re-parented.
//!
//! * broker **SIGTERM / Ctrl-C** — guaranteed: `main` awaits [`Supervisor::shutdown`]
//!   after the serve loop drains, which sends SIGTERM to the group and escalates to
//!   SIGKILL after `QUEEN_S3_SHUTDOWN_GRACE_MS`.
//! * broker **panic / unexpected drop** — `kill_on_drop(true)`: tokio reaps the
//!   child when the `Child` handle drops.
//! * broker **SIGKILL** — platform-dependent, and the honest answer differs:
//!   on **Linux** the child sets `PR_SET_PDEATHSIG(SIGKILL)`, so the kernel kills it
//!   when the parent dies, whatever the parent died of. On **macOS/BSD** there is no
//!   equivalent: a SIGKILLed broker leaves the sink running, re-parented to init,
//!   still writing to the bucket. That is a dev-machine caveat, not a production
//!   one (production is Linux), and it is stated rather than papered over.
//!
//! ## One variable, ONE reader
//! Where `QUEEN_SQS_SHUTDOWN_GRACE_MS` is read twice — by the SQS supervisor and
//! again by the facade, which has a drain deadline of its own — this variable has
//! exactly one reader, and it is this file. The sink sets no deadline on itself:
//! it catches SIGTERM, finishes the window it is in, and exits
//! (`connectors/queen-s3/src/main.rs`, `shutdown_signal`). So this number is not
//! a deadline the two sides agree on, it IS the child's deadline, enforced from
//! outside by the SIGKILL that follows it. Which is the reason it is 30s and not
//! 5s: it has to be longer than a window close takes, because nothing else is
//! going to stop the child politely.
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

use crate::config::S3SinkConfig;

/// The binary this supervises, and the file name looked for next to the broker's
/// own executable when `QUEEN_S3_BIN` is unset. (The twins' `FACADE_BIN`; the
/// sink speaks no wire protocol, so it is not a facade and does not say so.)
pub const SINK_BIN: &str = "queen-s3";

/// Backoff ladder: the wait before re-spawning a child that exited. Doubling from
/// one second to a thirty second ceiling — long enough that a permanently broken
/// sink costs nothing, short enough that a one-off crash is invisible downstream
/// (the sink resumes from the committed pointer in Queen's KV, so a restart costs
/// lag and never a gap in the lake).
const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// A child that ran this long before exiting was HEALTHY, and its successor starts
/// the ladder again from one second. Without the reset, a sink that crashes once
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
/// configuration surface, `QUEEN_S3_*` included (`_SINK`, `_QUEUES`,
/// `_PARTITIONS`, `_ENDPOINT`, `_REGION`, `_BUCKET`, `_PREFIX`, `_ACCESS_KEY`,
/// `_SECRET_KEY`, `_PATH_STYLE`, `_SSE*`, `_FORMAT`, `_COMPRESSION`, `_LAYOUT`,
/// `_ALIGN`, `_TARGET_MB`, `_MAX_WINDOW_MS`, `_START`, `_CHECKPOINT_EVERY`,
/// `_MEMORY_MB`, `_FETCH_CONCURRENCY`, `_DISCOVERY_INTERVAL_MS`, `_SAFE_GUARD_MS`,
/// `_LISTEN`, `_INSTANCE`), plus `QUEEN_TOKEN`, `LOG_LEVEL`/`RUST_LOG` and
/// `QUEEN_LOG_JSON`. These four are the broker's own secrets (the set
/// `config::log_effective` masks) and the sink reads none of them: a database
/// password has no business being readable in a second process's environment just
/// because that process happens to be colocated.
///
/// `QUEEN_TOKEN` is deliberately NOT here. It is a secret, but it is the CHILD's
/// credential — the one it presents to this broker — so removing it would be
/// removing the feature. Neither are the sink's own S3 keys: they are the
/// child's credential for the OTHER side of the hop, and the child is the only
/// process in this tree that has any use for them.
const STRIPPED_ENV: &[&str] = &[
    "PG_PASSWORD",
    "JWT_SECRET",
    "QUEEN_ENCRYPTION_KEY",
    "QUEEN_SYNC_SECRET",
];

/// The variables the sink cannot start without, named together because they are
/// discovered together: an operator who set one has set none of the others by
/// accident. [`preflight`] gates on the bucket and names all four, rather than
/// gating on all four, because the child's own `Config::from_env` is the
/// authoritative parser (`connectors/queen-s3/src/config.rs`) and a second
/// half-parser in the broker would be a second thing to keep in step.
const REQUIRED_ENV: &str =
    "QUEEN_S3_ENDPOINT, QUEEN_S3_REGION, QUEEN_S3_BUCKET and QUEEN_S3_QUEUES";

// ---------------------------------------------------------------------------
// Boot-time resolution. All pure, all unit-tested: an operator's mistake must
// name itself at boot, and the code that decides that must be testable without
// spawning anything.
// ---------------------------------------------------------------------------

/// Where the sink binary is. `QUEEN_S3_BIN` when set, otherwise the file
/// named [`SINK_BIN`] NEXT TO the running broker executable — which is what
/// makes the Docker image work with no configuration at all (all four binaries
/// land in `/app/bin`).
pub fn resolve_bin(configured: &str, current_exe: Option<&Path>) -> PathBuf {
    if !configured.trim().is_empty() {
        return PathBuf::from(configured.trim());
    }
    match current_exe.and_then(Path::parent) {
        Some(dir) => dir.join(SINK_BIN),
        // No executable path (a platform that cannot answer `current_exe`): fall
        // back to the bare name so PATH resolution still has a chance, rather
        // than building a path relative to an unknown directory.
        None => PathBuf::from(SINK_BIN),
    }
}

/// Is this path something `exec` can actually run? A file whose execute bit was
/// lost — an unpacked archive, a `COPY` from a build context that came off a
/// filesystem with no permission bits, a volume mount with `noexec` semantics —
/// is the one spawn failure that looks exactly like a healthy configuration in
/// every log except the child's, and it repeats forever on the ladder.
///
/// Unix only. Elsewhere there is no bit to read and every regular file is a
/// candidate, so the answer is "yes" and the spawn is the test.
fn is_executable(bin: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        match std::fs::metadata(bin) {
            Ok(m) => m.permissions().mode() & 0o111 != 0,
            Err(_) => false,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = bin;
        true
    }
}

/// The misconfiguration that must be caught at BOOT rather than discovered as a
/// crash-loop. All three cases are conditions no amount of restarting can
/// resolve, and the broker's own convention for those is `obs::fatal` (see
/// `AuthConfig::validate`, `checked_bind_addr`, the KV trusted-proxy interlock).
///
/// The third gate is the sink's twin of the Kafka supervisor's advertised-address
/// gate: the settings the connector cannot default. A sink with no destination is
/// not a sink, and there is no bucket that is right more often than it is wrong.
/// Started that way the child exits 1 on every spawn forever, and the operator
/// reads a backoff ladder instead of the one sentence that fixes it.
///
/// `bucket` is `QUEEN_S3_BUCKET`, the one of the four that is checked, because
/// the four are set together or not at all: nobody arrives at a bucket name
/// without an endpoint. The message names all four so the fix is one edit.
pub fn preflight(bin: &Path, bucket: Option<&str>) -> Result<(), String> {
    if !bin.is_file() {
        return Err(format!(
            "QUEEN_S3_EMBEDDED=true but no {SINK_BIN} binary at {}. \
             Embedded mode spawns the S3 sink as a child process; it cannot spawn a file \
             that is not there. Point QUEEN_S3_BIN at the binary, or put {SINK_BIN} next \
             to the broker executable (the queen Docker image ships it in /app/bin), or unset \
             QUEEN_S3_EMBEDDED to run the broker alone.",
            bin.display()
        ));
    }
    if !is_executable(bin) {
        return Err(format!(
            "QUEEN_S3_EMBEDDED=true and {} exists but is not executable. Every spawn would \
             fail with the same permission error and be retried on the backoff ladder forever. \
             Restore the execute bit (chmod +x), or point QUEEN_S3_BIN at a binary that has \
             one, or unset QUEEN_S3_EMBEDDED.",
            bin.display()
        ));
    }
    if bucket.map(str::trim).filter(|v| !v.is_empty()).is_none() {
        return Err(format!(
            "QUEEN_S3_EMBEDDED=true without QUEEN_S3_BUCKET. The sink writes objects to an \
             object store, and it needs the whole destination before it can start: \
             {REQUIRED_ENV} (plus QUEEN_S3_ACCESS_KEY and QUEEN_S3_SECRET_KEY, which the \
             child reads from a Secret in every deployment that has one). There is no default \
             bucket, and inventing one would be a sink writing somebody else's data somewhere \
             nobody looked. Set them, or unset QUEEN_S3_EMBEDDED. It is refused here rather \
             than in the child because a child that can never boot is a crash-loop, not a \
             configuration error an operator can read."
        ));
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
/// is WRONG for a Queen Cloud cell, where the sink must reach the broker THROUGH
/// the cell proxy so that every fetch and every KV write crosses the proxy's
/// auth, tenant scoping, metering and quotas — and, for this connector
/// specifically, the KV carve-out that keeps a tenant over its storage quota
/// from wedging its own sink (plan §8). There is no way to express that with an
/// injected loopback, and an operator who sets `QUEEN_URL` has said something
/// specific that this supervisor has no better information than.
///
/// Empty is not "set": `QUEEN_URL=` is an unset variable spelled by a Helm
/// template that resolved to nothing, and inheriting it would send the child to
/// its own default (`connectors/queen-s3/src/config.rs`, `DEFAULT_QUEEN_URL`)
/// rather than to the listener this process bound. So the same
/// trim-and-reject-empty rule every other knob here follows.
pub fn child_queen_url(inherited: Option<&str>, loopback: &str) -> String {
    match inherited.map(str::trim).filter(|v| !v.is_empty()) {
        Some(explicit) => explicit.to_string(),
        None => loopback.to_string(),
    }
}

/// Which branch [`child_queen_url`] took, for the one line an operator greps
/// when they need to know whether the sink is hairpinning through a proxy or
/// talking to the listener in its own process.
fn queen_url_source(inherited: Option<&str>) -> &'static str {
    match inherited.map(str::trim).filter(|v| !v.is_empty()) {
        Some(_) => "QUEEN_URL (explicit)",
        None => "loopback (bound listener)",
    }
}

/// The one auth posture embedded mode cannot fix by itself, worded once.
///
/// With `JWT_ENABLED=true` the broker requires a token on every path the sink
/// uses (`partitions/changed`, `fetch`, `kv` — none of them are in the default
/// `JWT_SKIP_PATHS`), and the loopback hop is not exempt: embedded mode does not
/// get a private door into the broker, because a private door is exactly the kind
/// of thing that is later found open from somewhere else. So the child needs a
/// credential like any other client, and it has exactly ONE way to hold one —
/// `QUEEN_TOKEN` — because the sink has no inbound protocol of its own and
/// therefore no per-client identity to borrow (this is where the two facades have
/// a second option and it has none). With no token, every discovery and fetch
/// answers 401 — a WARN and not a fatal, because the operator may be mid-rollout
/// and the sink is not load-bearing for the broker.
pub fn auth_advisory(broker_auth_on: bool, token: Option<&str>) -> Option<String> {
    let has = |v: Option<&str>| v.map(str::trim).is_some_and(|v| !v.is_empty());
    if !broker_auth_on || has(token) {
        return None;
    }
    Some(
        "JWT_ENABLED=true with QUEEN_S3_EMBEDDED=true, but the sink has no credential: every \
         discovery, fetch and KV call it makes will be answered 401, so nothing is ever \
         written to the bucket. Give the child a QUEEN_TOKEN — a key with consume scope (it \
         reads the log) and kv write scope (the two pointer keys that make a window \
         exactly-once). The hop the child makes is authenticated like any other client's, \
         whether it is the loopback this supervisor injects or the QUEEN_URL an operator set \
         (see child_queen_url) — there is no exempt door."
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
/// sink's own subscriber colourises without testing for a tty, so without this
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
    /// sink is calling the proxy" out of the boot log.
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
/// Its own cell, separate from the two facade supervisors': the three children are
/// independent and a deployment may run any subset of them. Never set in the
/// library target — the embedded `queen::Broker` has no HTTP listener to point a
/// sink at — so `status_value` there is always `None` and `/status` renders
/// exactly as it always did.
static GLOBAL: std::sync::OnceLock<Arc<Supervisor>> = std::sync::OnceLock::new();

/// The `s3` block of `GET /status`, or `None` when embedded mode is off.
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
    ///
    /// The grace here is the sink's 30s and not the facades' 5s, so a broker
    /// stopping with a sink attached takes up to half a minute longer than one
    /// stopping with a facade — deliberately, because that is how long the
    /// child's open window has to become a committed one.
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
                target: "s3",
                grace_ms = self.grace.as_millis() as u64,
                "the queen-s3 child did not report gone within the grace window; \
                 the broker is exiting anyway (kill_on_drop reaps it)"
            );
        }
    }

    /// The command line, rebuilt per spawn so a replaced binary (an upgrade under
    /// a running broker) is picked up by the next restart.
    fn command(&self) -> Command {
        let mut cmd = Command::new(&self.bin);
        // The child inherits this process's environment — which is what makes
        // "every QUEEN_S3_* knob forwards verbatim" true without listing one of
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
                // kill(-pgid) and nothing the sink spawned outlives it.
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
                    target: "s3",
                    pid,
                    stream,
                    suppressed = n,
                    "queen-s3 output rate-limited"
                ),
            }
            // The child stamps its own line with its own timestamp and level; this
            // is a forward, not a re-format, so the line goes through as it was
            // written and the broker's fields say only where it came from.
            tracing::info!(target: "s3", pid, stream, "{}", sanitize(&line));
        }
    });
}

/// Start supervising. Call AFTER the HTTP listener is bound: `loopback` names it,
/// and a child that dialled before the socket existed would burn a restart on
/// nothing. Unlike the facades, this child calls the broker at boot rather than
/// waiting for a client to arrive — its first act is discovery (plan §3) — so the
/// ordering is load-bearing here rather than belt and braces.
///
/// `loopback` is the DEFAULT and not the answer: an explicitly set `QUEEN_URL` in
/// this process's own environment wins ([`child_queen_url`]), which is what makes
/// a Cloud cell possible — there the sink must reach the broker through the
/// proxy, and no address derived from the local listener can express that.
pub fn spawn(cfg: &S3SinkConfig, bin: PathBuf, loopback: String) -> Arc<Supervisor> {
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
                    target: "s3",
                    bin = %sup.bin.display(),
                    error = %e,
                    backoff_ms = wait.as_millis() as u64,
                    "cannot spawn the queen-s3 sink; retrying after backoff"
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
            target: "s3",
            pid,
            bin = %sup.bin.display(),
            queen_url = %sup.queen_url,
            queen_url_from = sup.queen_url_from,
            "queen-s3 sink started (embedded)"
        );

        let exit = tokio::select! {
            r = child.wait() => Some(r),
            _ = sup.stop.notified() => None,
        };

        let Some(status) = exit else {
            // Ordered shutdown: the broker is going away and takes the child with
            // it. Not a restart, and not an error.
            let how = terminate(&mut child, sup.grace).await;
            tracing::info!(target: "s3", pid, how, "queen-s3 sink stopped with the broker");
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
            target: "s3",
            pid,
            reason = %reason,
            uptime_ms = uptime.as_millis() as u64,
            restarts,
            backoff_ms = wait.as_millis() as u64,
            "queen-s3 sink EXITED; restarting after backoff"
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
            PathBuf::from("/app/bin/queen-s3")
        );
        // Present-but-empty is unset, the same rule every other knob follows.
        assert_eq!(
            resolve_bin("   ", Some(&exe)),
            PathBuf::from("/app/bin/queen-s3")
        );
        // ...and an explicit path wins, trimmed.
        assert_eq!(
            resolve_bin(" /usr/local/bin/qs3 ", Some(&exe)),
            PathBuf::from("/usr/local/bin/qs3")
        );
        // No executable path at all: the bare name, so PATH still has a chance.
        assert_eq!(resolve_bin("", None), PathBuf::from(SINK_BIN));
    }

    #[test]
    fn a_missing_binary_names_the_fix() {
        let err = preflight(Path::new("/nonexistent/queen-s3"), Some("lake")).unwrap_err();
        assert!(err.contains("QUEEN_S3_BIN"), "{err}");
        assert!(err.contains("/nonexistent/queen-s3"), "{err}");
        assert!(err.contains("QUEEN_S3_EMBEDDED"), "{err}");
    }

    /// A file with no execute bit is the spawn failure that reads as healthy
    /// configuration everywhere except the ladder. The twins do not check it
    /// because they never had a reason to; the check costs one `stat`.
    #[cfg(unix)]
    #[test]
    fn a_binary_without_the_execute_bit_is_refused_at_boot() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("queen-s3-preflight-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let bin = dir.join("queen-s3");
        std::fs::write(&bin, b"#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o644)).unwrap();
        let err = preflight(&bin, Some("lake")).unwrap_err();
        assert!(err.contains("not executable"), "{err}");
        assert!(err.contains("chmod +x"), "{err}");
        // With the bit, the same file passes both binary gates.
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(preflight(&bin, Some("lake")).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The sink's twin of the advertised-address gate: a destination the
    /// connector cannot default, refused before the crash-loop.
    #[test]
    fn a_missing_bucket_is_refused_at_boot_and_names_all_four_variables() {
        // The broker's own executable exists and runs, so both binary gates pass
        // and the third is the one under test.
        let exe = std::env::current_exe().unwrap();
        for absent in [None, Some(""), Some("   ")] {
            let err = preflight(&exe, absent).unwrap_err();
            assert!(err.contains("QUEEN_S3_ENDPOINT"), "{err}");
            assert!(err.contains("QUEEN_S3_REGION"), "{err}");
            assert!(err.contains("QUEEN_S3_BUCKET"), "{err}");
            assert!(err.contains("QUEEN_S3_QUEUES"), "{err}");
            assert!(err.contains("crash-loop"), "{err}");
        }
        assert!(preflight(&exe, Some("lake")).is_ok());
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

    /// The Cloud case. A cell's sink must call the broker THROUGH the proxy —
    /// for metering, for tenant scoping, and for the KV carve-out — and the only
    /// way to say so is an explicit `QUEEN_URL`.
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
    /// default rather than to this broker.
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
        assert!(auth_advisory(false, None).is_none());
        // Auth on with the one credential the sink can hold.
        assert!(auth_advisory(true, Some("eyJ.a.b")).is_none());
        // Auth on without it — and empty is not a credential.
        for token in [None, Some(""), Some("  ")] {
            let msg = auth_advisory(true, token).unwrap();
            assert!(msg.contains("QUEEN_TOKEN"), "{msg}");
            assert!(msg.contains("401"), "{msg}");
            // The scopes are the half an operator gets wrong: consume is not
            // enough, because the commit is a KV write.
            assert!(msg.contains("kv write"), "{msg}");
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
        let real = "\u{1b}[2m2026-09-04T18:14:50Z\u{1b}[0m \u{1b}[31mERROR\u{1b}[0m \
                    \u{1b}[2mboot\u{1b}[0m: FATAL: no bucket";
        let out = sanitize(real);
        assert!(!out.contains('\u{1b}'), "{out}");
        assert!(
            !out.contains('['),
            "the CSI body must go with the escape: {out}"
        );
        assert!(out.contains("FATAL: no bucket"), "{out}");
        assert!(
            out.contains("ERROR"),
            "the words survive, only the colour goes: {out}"
        );
        // An OSC hyperlink, and the classic line-hiding carriage return.
        assert_eq!(sanitize("a\u{1b}]8;;http://x\u{7}b"), "ab");
        assert_eq!(sanitize("visible\rhidden"), "visible hidden");
        assert_eq!(sanitize("tab\there"), "tab here");
    }

    /// The child's credentials must survive the strip; the broker's must not.
    #[test]
    fn the_brokers_secrets_do_not_reach_the_child_but_its_own_token_does() {
        assert!(STRIPPED_ENV.contains(&"PG_PASSWORD"));
        assert!(STRIPPED_ENV.contains(&"JWT_SECRET"));
        assert!(!STRIPPED_ENV.contains(&"QUEEN_TOKEN"));
        // Nothing QUEEN_S3_* is stripped: the passthrough is the contract, and
        // QUEEN_S3_SECRET_KEY in particular is a secret that is the CHILD's — it
        // is the credential for the hop this process never makes.
        assert!(!STRIPPED_ENV.iter().any(|k| k.starts_with("QUEEN_S3_")));
    }
}
