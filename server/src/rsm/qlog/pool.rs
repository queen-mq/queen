//! The log writer's helper threads: run BORROWED jobs on a fixed set of
//! threads for a caller that waits until every one of them has finished — what
//! `std::thread::scope` does, without a thread spawned per job.
//!
//! The raft log writer writes one group into every log it touches, and the logs
//! of different shards (or lanes) are independent files: their records are
//! encoded, written and indexed in parallel, and the group's fsync ticket is
//! taken once all of them are done. Spawning a scoped thread per shard per group
//! cost ~15-20 µs a spawn — at ~1,000 groups/s and 8-16 shards, a sizeable part
//! of a core on the one thread every write waits for.

use std::panic::AssertUnwindSafe;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Counts down the jobs of one [`run_scoped`] call.
struct Latch {
    left: Mutex<usize>,
    done: Condvar,
}

impl Latch {
    fn count_down(&self) {
        let mut g = self.left.lock().unwrap_or_else(|p| p.into_inner());
        *g -= 1;
        if *g == 0 {
            self.done.notify_all();
        }
    }

    fn wait(&self) {
        let mut g = self.left.lock().unwrap_or_else(|p| p.into_inner());
        while *g > 0 {
            g = self.done.wait(g).unwrap_or_else(|p| p.into_inner());
        }
    }
}

/// Counts its job down when dropped: after the job ran, when it panicked, and
/// when it was dropped without running (a pool that went away).
struct CountDown(Arc<Latch>);

impl Drop for CountDown {
    fn drop(&mut self) {
        self.0.count_down();
    }
}

/// Blocks the [`run_scoped`] frame until every job is done — on the way out
/// of an unwind too — which is what makes lending its borrows to other
/// threads sound.
struct WaitAll<'l>(&'l Latch);

impl Drop for WaitAll<'_> {
    fn drop(&mut self) {
        self.0.wait();
    }
}

/// `QUEEN_QLOG_WRITE_THREADS` (default 8, at most 64): the helper threads.
fn threads() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("QUEEN_QLOG_WRITE_THREADS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(8)
            .clamp(1, 64)
    })
}

/// The pool's queue, started on first use.
fn pool() -> &'static Mutex<Sender<Job>> {
    static POOL: OnceLock<Mutex<Sender<Job>>> = OnceLock::new();
    POOL.get_or_init(|| {
        let (tx, rx) = channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        for i in 0..threads() {
            let rx = rx.clone();
            let spawned = std::thread::Builder::new()
                .name(format!("queen-qlog-write-{i}"))
                .spawn(move || {
                    // W1: part of the core log writer.
                    crate::obs::panic_policy::mark_current_thread_core();
                    loop {
                        let job = {
                            let guard = rx.lock().unwrap_or_else(|p| p.into_inner());
                            guard.recv()
                        };
                        let Ok(job) = job else { return };
                        job();
                    }
                });
            if spawned.is_err() {
                break;
            }
        }
        Mutex::new(tx)
    })
}

/// Forget the lifetime of a job's borrows (see [`run_scoped`]'s SAFETY).
///
/// # Safety
///
/// The caller must not let anything the job borrows go away before the job
/// has run or been dropped.
unsafe fn erase<'x>(f: Box<dyn FnOnce() + Send + 'x>) -> Job {
    // SAFETY: same layout; only the lifetime bound differs (the caller's
    // contract above).
    unsafe { std::mem::transmute::<Box<dyn FnOnce() + Send + 'x>, Job>(f) }
}

/// Run every job, the first on this thread and the others on the pool, and
/// return once ALL of them are done, their results in order. A job that
/// panics answers `Err` (the panic stays on its thread); a pool that cannot
/// take a job runs it here.
pub(crate) fn run_scoped<'a, R: Send + 'a>(
    jobs: Vec<Box<dyn FnOnce() -> R + Send + 'a>>,
) -> Vec<std::thread::Result<R>> {
    let n = jobs.len();
    let slots: Vec<Mutex<Option<std::thread::Result<R>>>> =
        (0..n).map(|_| Mutex::new(None)).collect();
    if n == 0 {
        return Vec::new();
    }
    let latch = Arc::new(Latch {
        left: Mutex::new(n - 1),
        done: Condvar::new(),
    });
    let mut jobs = jobs.into_iter();
    let first = jobs.next().expect("n > 0");
    {
        let wait = WaitAll(&latch);
        for (i, job) in jobs.enumerate() {
            let slot = &slots[i + 1];
            let count = CountDown(latch.clone());
            let run: Box<dyn FnOnce() + Send + '_> = Box::new(move || {
                let _count = count;
                let r = std::panic::catch_unwind(AssertUnwindSafe(job));
                *slot.lock().unwrap_or_else(|p| p.into_inner()) = Some(r);
            });
            // SAFETY: the job borrows data that outlives this call (the
            // caller's, and `slots`). `wait` (dropped at the end of this block,
            // and on an unwind out of it) blocks until every job has run or
            // been dropped — each holds a `CountDown` — so no job can touch a
            // borrow once this frame moves on.
            let run: Job = unsafe { erase(run) };
            let sent = pool().lock().unwrap_or_else(|p| p.into_inner()).send(run);
            if let Err(e) = sent {
                (e.0)();
            }
        }
        let r = std::panic::catch_unwind(AssertUnwindSafe(first));
        *slots[0].lock().unwrap_or_else(|p| p.into_inner()) = Some(r);
        drop(wait);
    }
    slots
        .into_iter()
        .map(|s| {
            s.into_inner()
                .unwrap_or_else(|p| p.into_inner())
                .unwrap_or_else(|| Err(Box::new("a qlog write job was dropped unrun")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn every_borrowed_job_runs_before_the_call_returns() {
        let data: Vec<u64> = (0..100).collect();
        let hits = AtomicUsize::new(0);
        for _ in 0..50 {
            let jobs: Vec<Box<dyn FnOnce() -> u64 + Send + '_>> = data
                .chunks(7)
                .map(|c| {
                    let hits = &hits;
                    Box::new(move || {
                        hits.fetch_add(1, Ordering::Relaxed);
                        c.iter().sum::<u64>()
                    }) as Box<dyn FnOnce() -> u64 + Send + '_>
                })
                .collect();
            let n = jobs.len();
            let out = run_scoped(jobs);
            assert_eq!(out.len(), n);
            let sums: Vec<u64> = out.into_iter().map(|r| r.unwrap()).collect();
            let want: Vec<u64> = data.chunks(7).map(|c| c.iter().sum()).collect();
            assert_eq!(sums, want, "results come back in job order");
        }
        assert_eq!(hits.load(Ordering::Relaxed), 50 * data.chunks(7).count());
    }

    #[test]
    fn a_panicking_job_is_an_error_and_the_others_still_run() {
        let ran = AtomicUsize::new(0);
        let jobs: Vec<Box<dyn FnOnce() -> usize + Send + '_>> = (0..6usize)
            .map(|i| {
                let ran = &ran;
                Box::new(move || {
                    if i == 3 {
                        panic!("job 3 fails");
                    }
                    ran.fetch_add(1, Ordering::Relaxed);
                    i
                }) as Box<dyn FnOnce() -> usize + Send + '_>
            })
            .collect();
        let out = run_scoped(jobs);
        assert!(out[3].is_err());
        for (i, r) in out.iter().enumerate().filter(|(i, _)| *i != 3) {
            assert_eq!(*r.as_ref().unwrap(), i);
        }
        assert_eq!(ran.load(Ordering::Relaxed), 5);
    }
}
