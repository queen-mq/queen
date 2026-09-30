//! Prefork workers: Laravel boots once, in a fork server, and every worker is
//! forked from it instead of booting on its own. Workers share the framework
//! and the opcache copy-on-write.
//!
//! The server is `php artisan queen:fork-server`, the Laravel package's
//! `ForkServer`. This module is the master's side of its protocol, one JSON
//! object per line:
//!
//! ```text
//! master -> server (stdin)  {"fork": {"id": 7, "argv": ["queen", "--queue=high", ...], "env": {"NAME": "value" | null}}}
//! server -> master (fd 3)   {"ready": "queen.fork-server/v1", "pid": 42}
//!                           {"forked": 7, "pid": 43} | {"failed": 7, "error": "..."}
//!                           {"exited": 43, "status": <raw waitpid status>}
//! ```
//!
//! Every forked worker leads its own session, so it is signalled and drained
//! exactly like a spawned one. The server is started with SIGTERM as its
//! parent-death signal: when this master dies, the server SIGKILLs every
//! worker it forked before it exits, the same hard fence spawned workers get
//! from their own parent-death signal. A SIGTERM while this master lives
//! (systemd signalling the whole unit) leaves the workers to the drain.

use crate::Config;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::fd::FromRawFd;
#[cfg(unix)]
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub(crate) const PROTOCOL: &str = "queen.fork-server/v1";
pub(crate) const BOOT_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const FORK_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_EVENT_BYTES: usize = 1024 * 1024;

pub(crate) struct ForkServer {
    child: Child,
    commands: Option<ChildStdin>,
    events: File,
    buffer: Vec<u8>,
    next_id: u64,
    exits: HashMap<u32, i32>,
    pending: Vec<serde_json::Value>,
    /// Fork requests this master stopped waiting for: a late reply must not
    /// leave a worker nobody counts or drains.
    abandoned: HashSet<u64>,
    /// Workers forked for an abandoned request, told to stop.
    strays: HashSet<u32>,
    fork_timeout: Duration,
    /// Set by a failed fork: new workers are spawned from then on, while the
    /// server keeps reporting the exits of the workers it already forked.
    pub(crate) disabled: bool,
}

impl ForkServer {
    /// Start `queen:fork-server` and wait until Laravel has booted in it, or
    /// until `running` turns false.
    #[cfg(unix)]
    pub(crate) fn start(
        config: &Config,
        running: &AtomicBool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut fds = [0 as libc::c_int; 2];
        // SAFETY: `fds` has room for the two descriptors pipe writes.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let [read_fd, write_fd] = fds;
        // SAFETY: pipe just returned these descriptors and nothing else owns them.
        let events = unsafe { File::from_raw_fd(read_fd) };
        // SAFETY: as above; dropped once the server holds its own copy.
        let write_end = unsafe { File::from_raw_fd(write_fd) };
        // SAFETY: fcntl only changes flags of descriptors this function owns.
        // Neither end may leak into a spawned worker; the server gets its own
        // copy of the write end at fd 3, which dup2 leaves open across exec.
        unsafe {
            libc::fcntl(read_fd, libc::F_SETFD, libc::FD_CLOEXEC);
            libc::fcntl(write_fd, libc::F_SETFD, libc::FD_CLOEXEC);
            let flags = libc::fcntl(read_fd, libc::F_GETFL);
            libc::fcntl(read_fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }

        let mut command = Command::new(&config.php_binary);
        command
            .current_dir(&config.cwd)
            .arg(&config.artisan)
            .arg("queen:fork-server")
            .stdin(Stdio::piped())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("QUEEN_FORK_SERVER", PROTOCOL)
            // The server is no worker: none of their variables may leak into it.
            .env_remove("QUEEN_SUPERVISOR_TELEMETRY_DIR");
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("QUEEN_LARAVEL_") {
                command.env_remove(name);
            }
        }
        #[cfg(target_os = "linux")]
        let supervisor_pid = unsafe { libc::getpid() };
        // SAFETY: the closure only calls async-signal-safe libc functions.
        unsafe {
            command.pre_exec(move || {
                if libc::dup2(write_fd, 3) < 0 || libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                #[cfg(target_os = "linux")]
                {
                    // SIGTERM, not SIGKILL: the server fences its workers
                    // before it exits.
                    if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    if libc::getppid() != supervisor_pid {
                        return Err(std::io::Error::other(
                            "supervisor exited while the fork server was starting",
                        ));
                    }
                }
                Ok(())
            });
        }
        let mut child = command.spawn()?;
        drop(write_end);
        let commands = child.stdin.take();
        let mut server = Self {
            child,
            commands,
            events,
            buffer: Vec::new(),
            next_id: 1,
            exits: HashMap::new(),
            pending: Vec::new(),
            abandoned: HashSet::new(),
            strays: HashSet::new(),
            fork_timeout: FORK_TIMEOUT,
            disabled: false,
        };
        if let Err(error) = server.await_event(
            |event| event.get("ready").and_then(serde_json::Value::as_str) == Some(PROTOCOL),
            BOOT_TIMEOUT,
            Some(running),
        ) {
            server.close(Duration::ZERO);
            return Err(error);
        }
        Ok(server)
    }

    #[cfg(not(unix))]
    pub(crate) fn start(
        _config: &Config,
        _running: &AtomicBool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Err("prefork needs a Unix host".into())
    }

    /// Ask for one worker; returns its pid.
    pub(crate) fn fork(
        &mut self,
        argv: &[String],
        environment: &[(String, Option<String>)],
    ) -> Result<u32, Box<dyn std::error::Error>> {
        let forked = self.request_fork(argv, environment);
        self.disabled |= forked.is_err();
        forked
    }

    fn request_fork(
        &mut self,
        argv: &[String],
        environment: &[(String, Option<String>)],
    ) -> Result<u32, Box<dyn std::error::Error>> {
        let id = self.next_id;
        self.next_id += 1;
        let env: serde_json::Map<String, serde_json::Value> = environment
            .iter()
            .map(|(name, value)| (name.clone(), serde_json::json!(value)))
            .collect();
        let mut line =
            serde_json::to_vec(&serde_json::json!({"fork": {"id": id, "argv": argv, "env": env}}))?;
        line.push(b'\n');
        self.commands
            .as_mut()
            .ok_or("the fork server is closed")?
            .write_all(&line)?;
        let reply = self
            .await_event(
                |event| {
                    event.get("forked").and_then(serde_json::Value::as_u64) == Some(id)
                        || event.get("failed").and_then(serde_json::Value::as_u64) == Some(id)
                },
                self.fork_timeout,
                None,
            )
            .inspect_err(|_| {
                self.abandoned.insert(id);
            })?;
        reply
            .get("pid")
            .and_then(serde_json::Value::as_u64)
            .filter(|pid| *pid > 0)
            .and_then(|pid| u32::try_from(pid).ok())
            .ok_or_else(|| {
                format!(
                    "the fork server could not fork: {}",
                    reply
                        .get("error")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown error")
                )
                .into()
            })
    }

    /// The raw wait status of an exited worker, or None while it runs.
    pub(crate) fn exit_status(&mut self, pid: u32) -> Option<i32> {
        let _ = self.poll();
        self.exits.remove(&pid)
    }

    pub(crate) fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Closing stdin tells the server its master is done: it fences whatever
    /// worker is left and exits.
    pub(crate) fn close(&mut self, timeout: Duration) {
        drop(self.commands.take());
        let deadline = Instant::now() + timeout;
        while self.is_alive() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        if self.is_alive() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }

    fn poll(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut chunk = [0_u8; 65536];
        loop {
            match self.events.read(&mut chunk) {
                Ok(0) => break,
                Ok(read) => {
                    self.buffer.extend_from_slice(&chunk[..read]);
                    if self.buffer.len() > MAX_EVENT_BYTES {
                        return Err("the fork server sent an oversized event".into());
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
        while let Some(newline) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = self.buffer.drain(..=newline).collect();
            let Ok(event) = serde_json::from_slice::<serde_json::Value>(&line) else {
                continue;
            };
            if let (Some(pid), Some(status)) = (
                event.get("exited").and_then(serde_json::Value::as_u64),
                event.get("status").and_then(serde_json::Value::as_i64),
            ) {
                if let (Ok(pid), Ok(status)) = (u32::try_from(pid), i32::try_from(status)) {
                    if !self.strays.remove(&pid) {
                        self.exits.insert(pid, status);
                    }
                }
            } else if let Some(id) = event
                .get("forked")
                .or_else(|| event.get("failed"))
                .and_then(serde_json::Value::as_u64)
                .filter(|id| self.abandoned.contains(id))
            {
                self.abandoned.remove(&id);
                if let Some(pid) = event
                    .get("forked")
                    .and(event.get("pid"))
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|pid| i32::try_from(pid).ok())
                    .filter(|pid| *pid > 0)
                {
                    self.stop_stray(pid);
                }
            } else {
                self.pending.push(event);
            }
        }
        Ok(())
    }

    /// A worker forked after its request timed out: nobody counts it, so it
    /// is told to stop like a drained one and its exit is not reported.
    fn stop_stray(&mut self, pid: i32) {
        self.strays.insert(pid.unsigned_abs());
        // SAFETY: plain signal delivery to the stray's process group and pid.
        unsafe {
            libc::kill(-pid, libc::SIGTERM);
            libc::kill(pid, libc::SIGTERM);
        }
    }

    fn await_event<F>(
        &mut self,
        matches: F,
        timeout: Duration,
        running: Option<&AtomicBool>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>>
    where
        F: Fn(&serde_json::Value) -> bool,
    {
        let deadline = Instant::now() + timeout;
        loop {
            self.poll()?;
            if let Some(index) = self.pending.iter().position(&matches) {
                return Ok(self.pending.remove(index));
            }
            if !self.is_alive() {
                return Err("the fork server exited".into());
            }
            if running.is_some_and(|running| !running.load(Ordering::SeqCst)) {
                return Err("stopped while waiting for the fork server".into());
            }
            if Instant::now() >= deadline {
                return Err("the fork server did not answer in time".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// A worker process, spawned or forked, behind the few operations the
/// supervisor needs.
pub(crate) enum WorkerProcess {
    Spawned(Child),
    Forked {
        pid: u32,
        server: Rc<RefCell<ForkServer>>,
        status: Option<ExitStatus>,
    },
}

impl WorkerProcess {
    pub(crate) fn id(&self) -> u32 {
        match self {
            Self::Spawned(child) => child.id(),
            Self::Forked { pid, .. } => *pid,
        }
    }

    pub(crate) fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        match self {
            Self::Spawned(child) => child.try_wait(),
            Self::Forked {
                pid,
                server,
                status,
            } => {
                if status.is_none() {
                    let mut server = server.borrow_mut();
                    *status = server.exit_status(*pid).map(ExitStatus::from_raw);
                    // SAFETY: signal 0 only checks that the process exists.
                    if status.is_none()
                        && !server.is_alive()
                        && unsafe { libc::kill(*pid as i32, 0) } != 0
                    {
                        // The server is gone and so is the worker: its exit
                        // status went with the server.
                        *status = Some(ExitStatus::from_raw(0));
                    }
                }
                Ok(*status)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn wait(&mut self) -> std::io::Result<ExitStatus> {
        if let Self::Spawned(child) = self {
            return child.wait();
        }
        loop {
            if let Some(status) = self.try_wait()? {
                return Ok(status);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[cfg(test)]
    pub(crate) fn kill(&mut self) -> std::io::Result<()> {
        match self {
            Self::Spawned(child) => child.kill(),
            Self::Forked { pid, .. } => {
                // SAFETY: plain signal delivery to a pid this master tracks.
                if unsafe { libc::kill(*pid as i32, libc::SIGKILL) } == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    /// The Laravel package's protocol fixture; None when PHP or the
    /// package's dependencies are not installed on this host.
    fn fixture_config() -> Option<Config> {
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../clients/client-php/tests/Fixtures/Prefork/fork_server.php");
        let autoload =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../clients/client-php/vendor/autoload.php");
        let php_available = Command::new("php")
            .arg("-r")
            .arg("exit(function_exists('pcntl_fork') && function_exists('posix_setsid') ? 0 : 1);")
            .status()
            .is_ok_and(|status| status.success());
        if !php_available || !fixture.exists() || !autoload.exists() {
            eprintln!("skipped: PHP with pcntl/posix and the Laravel package are required");
            return None;
        }
        let mut config: Config = serde_json::from_value(serde_json::json!({
            "version": 2,
            "cwd": fixture.parent().unwrap(),
            "php_binary": "php",
            "artisan": fixture,
            "state_directory": "/tmp/queen-prefork-test",
            "poll_interval": 3,
            "http_timeout": 5,
            "shutdown_grace": 5,
            "telemetry_ttl": 300,
            "process_limit": 8,
            "queen": {"url": "http://127.0.0.1:6632"},
            "supervisors": {}
        }))
        .unwrap();
        config.prefork = true;
        Some(config)
    }

    fn report() -> PathBuf {
        std::env::temp_dir().join(format!(
            "queen-prefork-rust-{}-{}.json",
            std::process::id(),
            Instant::now().elapsed().as_nanos() + u128::from(rand_suffix())
        ))
    }

    fn rand_suffix() -> u32 {
        let mut bytes = [0_u8; 4];
        let _ = File::open("/dev/urandom").and_then(|mut file| file.read_exact(&mut bytes));
        u32::from_ne_bytes(bytes)
    }

    fn forked(
        server: &Rc<RefCell<ForkServer>>,
        argv: &[&str],
        env: &[(&str, Option<&str>)],
    ) -> WorkerProcess {
        let argv: Vec<String> = argv.iter().map(|argument| (*argument).to_owned()).collect();
        let env: Vec<(String, Option<String>)> = env
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.map(str::to_owned)))
            .collect();
        let pid = server.borrow_mut().fork(&argv, &env).unwrap();
        WorkerProcess::Forked {
            pid,
            server: Rc::clone(server),
            status: None,
        }
    }

    fn wait_for_report(report: &Path) -> serde_json::Value {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(text) = std::fs::read_to_string(report) {
                if let Ok(value) = serde_json::from_str(&text) {
                    return value;
                }
            }
            assert!(Instant::now() < deadline, "the worker never reported");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn a_forked_worker_gets_its_arguments_environment_and_own_session() {
        let Some(config) = fixture_config() else {
            return;
        };
        let server = Rc::new(RefCell::new(
            ForkServer::start(&config, &AtomicBool::new(true)).unwrap(),
        ));
        let report = report();
        let path = report.to_str().unwrap();

        let mut worker = forked(
            &server,
            &["exit", path, "3"],
            &[
                ("QUEEN_TEST_VALUE", Some("per-worker")),
                ("QUEEN_TEST_REMOVED", None),
            ],
        );
        let status = worker.wait().unwrap();

        assert_eq!(status.code(), Some(3));
        let seen = wait_for_report(&report);
        assert_eq!(seen["argv"], serde_json::json!(["exit", path, "3"]));
        assert_eq!(seen["value"], "per-worker");
        assert_eq!(seen["removed"], false);
        assert_eq!(seen["pid"], seen["pgid"]);
        assert_eq!(seen["pid"], worker.id());
        server.borrow_mut().close(Duration::from_secs(5));
        let _ = std::fs::remove_file(report);
    }

    #[test]
    fn a_forked_worker_is_signalled_and_fenced_like_a_spawned_one() {
        let Some(config) = fixture_config() else {
            return;
        };
        let server = Rc::new(RefCell::new(
            ForkServer::start(&config, &AtomicBool::new(true)).unwrap(),
        ));
        let killed = report();
        let fenced = report();

        let mut worker = forked(&server, &["sleep", killed.to_str().unwrap()], &[]);
        wait_for_report(&killed);
        assert!(worker.try_wait().unwrap().is_none());
        crate::signal_process_group(&mut worker, libc::SIGKILL);
        assert_eq!(worker.wait().unwrap().signal(), Some(libc::SIGKILL));

        let survivor = forked(&server, &["sleep", fenced.to_str().unwrap()], &[]);
        wait_for_report(&fenced);
        server.borrow_mut().close(Duration::from_secs(5));
        let deadline = Instant::now() + Duration::from_secs(10);
        // SAFETY: signal 0 only checks that the process exists.
        while unsafe { libc::kill(survivor.id() as i32, 0) } == 0 {
            assert!(
                Instant::now() < deadline,
                "closing the server did not fence its worker"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = std::fs::remove_file(killed);
        let _ = std::fs::remove_file(fenced);
    }

    #[test]
    fn a_worker_forked_after_its_request_timed_out_is_stopped() {
        let Some(config) = fixture_config() else {
            return;
        };
        let server = Rc::new(RefCell::new(
            ForkServer::start(&config, &AtomicBool::new(true)).unwrap(),
        ));
        let late = report();
        server.borrow_mut().fork_timeout = Duration::ZERO;
        let timed_out = server.borrow_mut().fork(
            &["sleep".to_owned(), late.to_str().unwrap().to_owned()],
            &[],
        );
        assert!(timed_out.is_err());

        // The late reply precedes this one on the pipe.
        server.borrow_mut().fork_timeout = FORK_TIMEOUT;
        let exited = report();
        let mut worker = forked(&server, &["exit", exited.to_str().unwrap(), "0"], &[]);
        let stray = *server
            .borrow()
            .strays
            .iter()
            .next()
            .expect("a stray worker");

        let deadline = Instant::now() + Duration::from_secs(10);
        while !server.borrow().strays.is_empty() {
            assert!(
                Instant::now() < deadline,
                "the stray worker was not stopped"
            );
            assert_eq!(server.borrow_mut().exit_status(stray), None);
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(worker.wait().unwrap().code(), Some(0));
        server.borrow_mut().close(Duration::from_secs(5));
        let _ = std::fs::remove_file(late);
        let _ = std::fs::remove_file(exited);
    }

    #[test]
    fn a_failed_fork_disables_prefork_but_keeps_the_server() {
        let Some(config) = fixture_config() else {
            return;
        };
        let mut server = ForkServer::start(&config, &AtomicBool::new(true)).unwrap();
        server.commands = None;

        assert!(server.fork(&["exit".to_owned()], &[]).is_err());
        assert!(server.disabled);
        server.close(Duration::from_secs(5));
    }
}
