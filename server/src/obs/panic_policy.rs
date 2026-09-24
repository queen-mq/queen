//! W1 panic policy (PLAN_SINGLE_BINARY.md): the binary builds with
//! `panic = "unwind"`, and this hook decides which panics still take the
//! process down.
//!
//! * A panic on a **core** thread aborts: the planner/batcher, the log writer
//!   and syncer, apply, the checkpoint writer, the segment writers, the qlog
//!   codec pool, the openraft runtime. The core keeps its doctrine — crash, then
//!   replay from durable state — because a core loop that dies silently wedges
//!   every request behind it (a lost apply thread is a broker that accepts
//!   pushes and never answers them).
//! * Every other thread or task unwinds: a panic in a tokio task (an HTTP
//!   connection, a facade session, a console request) kills that task only.
//!
//! "Core" is decided at the moment of the panic, on the panicking thread:
//!
//! 1. a thread-local core depth (> 0 inside [`core`] futures, [`core_scope`]
//!    closures, and forever on a thread that called
//!    [`mark_current_thread_core`]) — for core work with no stable thread name,
//!    such as the batcher driver, which is a tokio task on the main runtime;
//! 2. else a thread-local exemption depth (> 0 inside [`non_core`]) — for
//!    non-core work that runs on a core-named runtime (an RPC handler on the
//!    `queen-raft` runtime);
//! 3. else the thread name, against [`CORE_THREAD_PREFIXES`].
//!
//! **Poisoned locks.** Under abort a `std::sync::Mutex` could never be seen
//! poisoned; under unwind it can. A poisoned lock is shared state a panicking
//! thread left mid-update, so by default ([`install`]) a panic whose message is
//! a `PoisonError` unwrap is escalated to an abort as well: that is the pre-W1
//! behaviour (crash and replay), reached one request later, instead of a
//! cascade of panics that wedges every task touching the lock. Locks that core
//! threads share with non-core code are read with [`LockExt`] /
//! [`RwLockExt`] (poison-tolerant), so a non-core panic never crashes the core
//! later. `QUEEN_PANIC_POISON=unwind` turns the escalation off.
//!
//! Core code must not rely on `catch_unwind`: the hook runs before any catch,
//! so a caught panic on a core thread still aborts.

// An API surface: the binary target uses a subset of it (the library target
// allows dead code crate-wide).
#![allow(dead_code)]

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::task::{Context, Poll};

/// Name prefixes of the core threads (grep `.name(` / `thread_name(` under
/// `src/rsm/`). A new core thread either gets a name listed here or calls
/// [`mark_current_thread_core`] first thing.
///
/// Deliberately NOT core: `queen-rsm-trace` (timing trace writer) and
/// `queen-dash-collector` (dashboard sampler) — losing either loses
/// diagnostics, not data.
pub const CORE_THREAD_PREFIXES: &[&str] = &[
    // rsm/batcher.rs — the planner thread behind the batcher driver.
    "queen-planner",
    // rsm/batcher_lanes.rs — per-lane planners.
    "queen-lane-",
    // rsm/apply.rs — apply, its checkpoint writer and the segment preflusher.
    "queen-rsm-apply",
    "queen-rsm-ckpt",
    "queen-rsm-preflush",
    // rsm/replicator/local.rs — the single-node log writer and its syncer.
    "queen-rsm-log",
    "queen-rsm-sync",
    // rsm/segments/mod.rs — segment writer pool.
    "queen-rsm-segwriter-",
    // rsm/replicator/raft — the openraft runtime workers (`queen-raft`), the
    // raft log writer/syncer (`queen-raft-log`, `queen-raft-sync`) and the
    // snapshot sender/receiver (`queen-raft-snap-*`).
    "queen-raft",
    // rsm/qlog/codec.rs — the zstd pool: a job that dies leaves its slot empty
    // and the log writer waiting on it.
    "qlog-zstd-",
];

/// Whether `name` is a core thread's name.
pub fn is_core_thread_name(name: &str) -> bool {
    CORE_THREAD_PREFIXES.iter().any(|p| name.starts_with(p))
}

thread_local! {
    static CORE_DEPTH: Cell<u32> = const { Cell::new(0) };
    static EXEMPT_DEPTH: Cell<u32> = const { Cell::new(0) };
}

fn depth(key: &'static std::thread::LocalKey<Cell<u32>>) -> u32 {
    // `try_with`: a panic during thread-local teardown must not panic again.
    key.try_with(Cell::get).unwrap_or(0)
}

fn bump(key: &'static std::thread::LocalKey<Cell<u32>>, up: bool) {
    let _ = key.try_with(|c| {
        c.set(if up {
            c.get() + 1
        } else {
            c.get().saturating_sub(1)
        })
    });
}

/// Whether a panic raised right now, on this thread, is a core panic.
pub fn is_core_context() -> bool {
    if depth(&CORE_DEPTH) > 0 {
        return true;
    }
    if depth(&EXEMPT_DEPTH) > 0 {
        return false;
    }
    std::thread::current()
        .name()
        .is_some_and(is_core_thread_name)
}

/// Mark the calling thread core for the rest of its life (an unnamed or scoped
/// thread doing core work).
pub fn mark_current_thread_core() {
    bump(&CORE_DEPTH, true);
}

/// RAII core scope: a panic while it is alive, on this thread, aborts.
pub struct CoreScope {
    _not_send: std::marker::PhantomData<*const ()>,
}

impl CoreScope {
    pub fn enter() -> CoreScope {
        bump(&CORE_DEPTH, true);
        CoreScope {
            _not_send: std::marker::PhantomData,
        }
    }
}

impl Drop for CoreScope {
    fn drop(&mut self) {
        bump(&CORE_DEPTH, false);
    }
}

/// Run `f` as core work on the current thread.
pub fn core_scope<R>(f: impl FnOnce() -> R) -> R {
    let _g = CoreScope::enter();
    f()
}

struct ExemptScope;

impl ExemptScope {
    fn enter() -> ExemptScope {
        bump(&EXEMPT_DEPTH, true);
        ExemptScope
    }
}

impl Drop for ExemptScope {
    fn drop(&mut self) {
        bump(&EXEMPT_DEPTH, false);
    }
}

/// Run `f` as NON-core work even on a core-named thread (a `spawn_blocking`
/// read on the `queen-raft` runtime's blocking pool). Core scopes nested
/// inside still abort.
pub fn non_core_scope<R>(f: impl FnOnce() -> R) -> R {
    let _g = ExemptScope::enter();
    f()
}

/// A future whose every poll is core work (see [`core`]).
pub struct Core<F> {
    inner: F,
}

/// Wrap a future so a panic while it is being polled aborts the process, on
/// whichever runtime worker polls it. For core loops that are tokio tasks:
/// `tokio::spawn(panic_policy::core(driver.run()))`.
pub fn core<F: Future>(inner: F) -> Core<F> {
    Core { inner }
}

impl<F: Future> Future for Core<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let _g = CoreScope::enter();
        // SAFETY: structural pinning of `inner`: it is never moved out of
        // `self`, and `Core` has no Drop impl and no `Unpin` impl of its own.
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        inner.poll(cx)
    }
}

/// A future whose polls are NOT core even on a core-named thread (see
/// [`non_core`]).
pub struct NonCore<F> {
    inner: F,
}

/// Wrap a future that runs on a core-named runtime (the `queen-raft` runtime
/// also serves the raft RPC listener) but is not core work: a panic in it
/// unwinds and kills only this task. Core scopes nested inside still abort.
pub fn non_core<F: Future>(inner: F) -> NonCore<F> {
    NonCore { inner }
}

impl<F: Future> Future for NonCore<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let _g = ExemptScope::enter();
        // SAFETY: as in `Core::poll`.
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        inner.poll(cx)
    }
}

// ---------------------------------------------------------------------------
// The hook
// ---------------------------------------------------------------------------

static INSTALLED: AtomicBool = AtomicBool::new(false);
static POISON_ABORT: AtomicBool = AtomicBool::new(false);

/// How the hook treats a panic that is not core.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    /// Escalate a `PoisonError` unwrap to an abort (see the module docs).
    pub poison_abort: bool,
}

impl Policy {
    /// The binary's policy: `QUEEN_PANIC_POISON=unwind` (or `0`/`false`/`off`)
    /// turns the poison escalation off; anything else keeps it on.
    pub fn from_env() -> Policy {
        let v = std::env::var("QUEEN_PANIC_POISON").unwrap_or_default();
        let off = matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "unwind" | "0" | "false" | "off" | "no"
        );
        Policy { poison_abort: !off }
    }
}

/// Install the process policy (the binary): core panics abort, poisoned-lock
/// panics abort unless `QUEEN_PANIC_POISON=unwind`, everything else unwinds.
/// Chains the previous hook (message + backtrace still print). Idempotent: a
/// second call only updates the policy.
pub fn install() {
    install_with(Policy::from_env());
}

/// The embedded library's policy: core panics abort, nothing else changes —
/// the host application owns its own locks and its own panics.
pub fn install_embedded() {
    install_with(Policy {
        poison_abort: false,
    });
}

pub fn install_with(policy: Policy) {
    POISON_ABORT.store(policy.poison_abort, Ordering::Relaxed);
    if INSTALLED.swap(true, Ordering::AcqRel) {
        return;
    }
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let msg = payload_str(info.payload());
        let core = is_core_context();
        let poisoned = !core && POISON_ABORT.load(Ordering::Relaxed) && msg.contains("PoisonError");
        let location = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let thread = std::thread::current()
            .name()
            .unwrap_or("unnamed")
            .to_string();
        let verdict = if core {
            "core panic: aborting (crash, then replay from durable state)"
        } else if poisoned {
            "poisoned lock: aborting (shared state left mid-update)"
        } else {
            "task panicked: unwinding, only this task/connection dies"
        };
        tracing::error!(
            target: "panic",
            location = %location,
            thread = %thread,
            core,
            fatal = core || poisoned,
            "{verdict}: {msg}"
        );
        prev(info);
        if core || poisoned {
            // Past `prev`, so the message and backtrace are already out.
            eprintln!("queen: {verdict} (thread {thread}, {location})");
            std::process::abort();
        }
    }));
}

fn payload_str(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string())
}

// ---------------------------------------------------------------------------
// Poison-tolerant lock access
// ---------------------------------------------------------------------------

/// `Mutex::lock` that takes the guard of a poisoned lock instead of panicking.
/// For locks core threads share with non-core code: a non-core panic while
/// holding one must not crash (or, with the poison escalation, abort) the core
/// on its next lock. Use it only where the protected data stays valid at every
/// point a holder could panic (counters, maps updated by single inserts,
/// caches that tolerate a lost update).
pub trait LockExt<T: ?Sized> {
    fn lock_unpoisoned(&self) -> MutexGuard<'_, T>;
}

impl<T: ?Sized> LockExt<T> for Mutex<T> {
    fn lock_unpoisoned(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The `RwLock` twin of [`LockExt`]. Only a panic under a WRITE guard poisons.
pub trait RwLockExt<T: ?Sized> {
    fn read_unpoisoned(&self) -> RwLockReadGuard<'_, T>;
    fn write_unpoisoned(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T: ?Sized> RwLockExt<T> for RwLock<T> {
    fn read_unpoisoned(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap_or_else(PoisonError::into_inner)
    }
    fn write_unpoisoned(&self) -> RwLockWriteGuard<'_, T> {
        self.write().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    //! The hook is process-wide, so every test that installs it runs in a
    //! CHILD process: the test re-executes this test binary filtered to itself
    //! with `QUEEN_PANIC_POLICY_CHILD` set, and the parent asserts how the
    //! child ended. The parent test process never installs the hook.
    use super::*;
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Command, ExitStatus};

    const CHILD_ENV: &str = "QUEEN_PANIC_POLICY_CHILD";

    /// A panic with a `()` type, so closures and futures built on it do not
    /// lean on never-type fallback.
    fn injected(msg: &'static str) {
        if std::hint::black_box(true) {
            panic!("{msg}");
        }
    }

    fn test_path(name: &str) -> String {
        // module_path!() = "queen::obs::panic_policy::tests"; libtest names
        // drop the crate segment.
        let m = module_path!();
        let m = m.split_once("::").map(|(_, rest)| rest).unwrap_or(m);
        format!("{m}::{name}")
    }

    /// Run `name` as a child with `case`, return how it ended + its stderr.
    fn run_child(name: &str, case: &str, extra_env: &[(&str, &str)]) -> (ExitStatus, String) {
        let exe = std::env::current_exe().expect("test binary path");
        let mut cmd = Command::new(exe);
        cmd.args([
            test_path(name).as_str(),
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_ENV, case)
        .env("RUST_BACKTRACE", "0");
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        let out = cmd.output().expect("spawn child test");
        (
            out.status,
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    }

    fn child_case() -> Option<String> {
        std::env::var(CHILD_ENV).ok()
    }

    fn assert_aborted(status: ExitStatus, stderr: &str) {
        assert_eq!(
            status.signal(),
            Some(libc::SIGABRT),
            "expected the child to die by SIGABRT, got {status:?}; stderr:\n{stderr}"
        );
    }

    fn assert_survived(status: ExitStatus, stderr: &str) {
        assert!(
            status.success(),
            "expected the child to exit 0, got {status:?}; stderr:\n{stderr}"
        );
    }

    #[test]
    fn panic_policy_core_names() {
        for n in [
            "queen-planner",
            "queen-lane-3",
            "queen-rsm-apply",
            "queen-rsm-ckpt",
            "queen-rsm-preflush",
            "queen-rsm-log",
            "queen-rsm-sync",
            "queen-rsm-segwriter-0",
            "queen-raft",
            "queen-raft-log",
            "queen-raft-sync",
            "queen-raft-snap-recv",
            "qlog-zstd-2",
        ] {
            assert!(is_core_thread_name(n), "{n} should be core");
        }
        for n in [
            "tokio-runtime-worker",
            "main",
            "queen-rsm-trace",
            "queen-dash-collector",
            "panic_policy_core_names",
            "",
        ] {
            assert!(!is_core_thread_name(n), "{n} should not be core");
        }
    }

    #[test]
    fn panic_policy_scopes_nest() {
        // No hook involved: the classification itself.
        assert!(!is_core_context());
        core_scope(|| {
            assert!(is_core_context());
            let _g = CoreScope::enter();
            assert!(is_core_context());
        });
        assert!(!is_core_context());
        std::thread::Builder::new()
            .name("queen-raft".into())
            .spawn(|| {
                assert!(is_core_context());
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                rt.block_on(non_core(async {
                    assert!(!is_core_context());
                    // A core scope inside an exempt one is still core.
                    core_scope(|| assert!(is_core_context()));
                }));
                assert!(is_core_context());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn panic_policy_poison_tolerant_locks() {
        let m = std::sync::Arc::new(Mutex::new(1u32));
        let rw = std::sync::Arc::new(RwLock::new(1u32));
        let (m2, rw2) = (m.clone(), rw.clone());
        let _ = std::thread::spawn(move || {
            let _a = m2.lock().unwrap();
            let _b = rw2.write().unwrap();
            injected("poison both (expected in this test)");
        })
        .join();
        assert!(m.is_poisoned() && rw.is_poisoned());
        *m.lock_unpoisoned() += 1;
        *rw.write_unpoisoned() += 1;
        assert_eq!(*m.lock_unpoisoned(), 2);
        assert_eq!(*rw.read_unpoisoned(), 2);
    }

    /// W1 gate: a panic inside a spawned tokio task, and inside an HTTP handler
    /// (one connection), leaves the process serving.
    #[test]
    fn panic_policy_task_panic_keeps_serving() {
        if child_case().as_deref() == Some("task") {
            install();
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                // 1. A detached task panics: its JoinHandle reports it, the
                //    runtime keeps going.
                let j = tokio::spawn(async { injected("injected facade-task panic") });
                let e = j.await.expect_err("the task panicked");
                assert!(e.is_panic());

                // 2. An HTTP handler panics: that connection dies, the next
                //    one is served.
                use axum::routing::get;
                let app = axum::Router::new()
                    .route(
                        "/boom",
                        get(|| async {
                            injected("injected handler panic");
                            "unreachable"
                        }),
                    )
                    .route("/ok", get(|| async { "ok" }));
                let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = l.local_addr().unwrap();
                tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
                let boom = http_get(addr, "/boom").await;
                assert!(
                    !boom.starts_with("HTTP/1.1 200"),
                    "the panicking handler answered: {boom}"
                );
                let ok = http_get(addr, "/ok").await;
                assert!(
                    ok.starts_with("HTTP/1.1 200"),
                    "broker stopped serving: {ok}"
                );
                assert!(ok.ends_with("ok"));
            });
            return;
        }
        if child_case().is_some() {
            return;
        }
        let (status, stderr) = run_child("panic_policy_task_panic_keeps_serving", "task", &[]);
        assert_survived(status, &stderr);
        assert!(stderr.contains("injected facade-task panic"), "{stderr}");
    }

    async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
        s.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        let _ = s.read_to_end(&mut buf).await;
        String::from_utf8_lossy(&buf).into_owned()
    }

    /// W1 gate: a panic on a thread named like a core thread aborts.
    #[test]
    fn panic_policy_core_thread_panic_aborts() {
        if child_case().as_deref() == Some("core_thread") {
            install();
            let _ = std::thread::Builder::new()
                .name("queen-rsm-apply".into())
                .spawn(|| injected("injected apply panic"))
                .unwrap()
                .join();
            // Only reached if the hook did not abort.
            std::process::exit(0);
        }
        if child_case().is_some() {
            return;
        }
        let (status, stderr) =
            run_child("panic_policy_core_thread_panic_aborts", "core_thread", &[]);
        assert_aborted(status, &stderr);
        assert!(stderr.contains("injected apply panic"), "{stderr}");
    }

    /// The batcher driver is a tokio task on the main runtime: the `core`
    /// wrapper, not the thread name, makes it core.
    #[test]
    fn panic_policy_core_future_panic_aborts() {
        if child_case().as_deref() == Some("core_future") {
            install();
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let _ = tokio::spawn(core(async { injected("injected batcher panic") })).await;
            });
            std::process::exit(0);
        }
        if child_case().is_some() {
            return;
        }
        let (status, stderr) =
            run_child("panic_policy_core_future_panic_aborts", "core_future", &[]);
        assert_aborted(status, &stderr);
    }

    /// An unnamed thread that marked itself core aborts; the same panic on an
    /// unmarked unnamed thread unwinds.
    #[test]
    fn panic_policy_marked_thread_aborts() {
        match child_case().as_deref() {
            Some("marked") => {
                install();
                let _ = std::thread::spawn(|| {
                    mark_current_thread_core();
                    injected("injected write-lane panic")
                })
                .join();
                std::process::exit(0);
            }
            Some("unmarked") => {
                install();
                let r = std::thread::spawn(|| injected("injected plain-thread panic")).join();
                assert!(r.is_err());
                return;
            }
            Some(_) => return,
            None => {}
        }
        let (status, stderr) = run_child("panic_policy_marked_thread_aborts", "marked", &[]);
        assert_aborted(status, &stderr);
        let (status, stderr) = run_child("panic_policy_marked_thread_aborts", "unmarked", &[]);
        assert_survived(status, &stderr);
    }

    /// A non-core panic poisons a lock: the next plain `.lock().unwrap()`
    /// escalates to an abort (default), a `lock_unpoisoned` does not, and
    /// `QUEEN_PANIC_POISON=unwind` turns the escalation off.
    #[test]
    fn panic_policy_poisoned_lock_escalates() {
        fn poison() -> std::sync::Arc<Mutex<u32>> {
            let m = std::sync::Arc::new(Mutex::new(0u32));
            let m2 = m.clone();
            let r = std::thread::spawn(move || {
                let _g = m2.lock().unwrap();
                injected("injected panic under a lock");
            })
            .join();
            assert!(r.is_err(), "the first panic unwinds");
            m
        }
        match child_case().as_deref() {
            Some("poison_unwrap") => {
                install();
                let m = poison();
                let _ = std::thread::spawn(move || *m.lock().unwrap() += 1).join();
                std::process::exit(0);
            }
            Some("poison_tolerant") => {
                install();
                let m = poison();
                std::thread::spawn(move || *m.lock_unpoisoned() += 1)
                    .join()
                    .unwrap();
                return;
            }
            Some("poison_off") => {
                install();
                let m = poison();
                let r = std::thread::spawn(move || *m.lock().unwrap() += 1).join();
                assert!(r.is_err());
                return;
            }
            Some(_) => return,
            None => {}
        }
        let name = "panic_policy_poisoned_lock_escalates";
        let (status, stderr) = run_child(name, "poison_unwrap", &[]);
        assert_aborted(status, &stderr);
        let (status, stderr) = run_child(name, "poison_tolerant", &[]);
        assert_survived(status, &stderr);
        let (status, stderr) = run_child(name, "poison_off", &[("QUEEN_PANIC_POISON", "unwind")]);
        assert_survived(status, &stderr);
    }

    /// `install_embedded` aborts on core panics but never escalates poison:
    /// the host application's locks are its own business.
    #[test]
    fn panic_policy_embedded_no_poison_escalation() {
        match child_case().as_deref() {
            Some("embedded") => {
                install_embedded();
                let m = std::sync::Arc::new(Mutex::new(0u32));
                let m2 = m.clone();
                let _ = std::thread::spawn(move || {
                    let _g = m2.lock().unwrap();
                    injected("host panic under a lock");
                })
                .join();
                let r = std::thread::spawn(move || *m.lock().unwrap() += 1).join();
                assert!(r.is_err());
                let _ = std::thread::Builder::new()
                    .name("queen-planner".into())
                    .spawn(|| injected("injected planner panic"))
                    .unwrap()
                    .join();
                std::process::exit(0);
            }
            Some(_) => return,
            None => {}
        }
        let (status, stderr) = run_child(
            "panic_policy_embedded_no_poison_escalation",
            "embedded",
            &[],
        );
        assert_aborted(status, &stderr);
        assert!(stderr.contains("injected planner panic"), "{stderr}");
    }
}
