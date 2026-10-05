//! Process counters shared by the dashboard collector (rsm/dashboard/collector.rs):
//! the per-interval op-counter diff, the parked-pop average and getrusage.

use std::time::Duration;

use crate::metrics::{Counters, Metrics};

// Minute-average of the 1 Hz parked samples for this interval: sum of samples
// divided by the interval's seconds (a queue parked for the whole minute with
// one consumer averages 1; parked 30s averages 0 after integer rounding-down
// only when < half — round to nearest instead).
pub(crate) fn drain_parked_avg(
    metrics: &Metrics,
    interval: Duration,
) -> std::collections::HashMap<String, i32> {
    let secs = interval.as_secs().max(1);
    metrics
        .parked
        .drain()
        .into_iter()
        .map(|(q, (sum, _n))| (q, ((sum as f64 / secs as f64).round()) as i32))
        .collect()
}

pub(crate) fn delta(prev: &Counters, now: &Counters) -> Counters {
    // Counters are monotone; saturating_sub guards a counter reset (never expected
    // in-process, but keeps a delta non-negative if it ever happens).
    Counters {
        push_requests: now.push_requests.saturating_sub(prev.push_requests),
        push_messages: now.push_messages.saturating_sub(prev.push_messages),
        pop_requests: now.pop_requests.saturating_sub(prev.pop_requests),
        pop_messages: now.pop_messages.saturating_sub(prev.pop_messages),
        ack_requests: now.ack_requests.saturating_sub(prev.ack_requests),
        ack_messages: now.ack_messages.saturating_sub(prev.ack_messages),
        ack_success: now.ack_success.saturating_sub(prev.ack_success),
        ack_failed: now.ack_failed.saturating_sub(prev.ack_failed),
        transactions: now.transactions.saturating_sub(prev.transactions),
        dlq_moved: now.dlq_moved.saturating_sub(prev.dlq_moved),
        db_errors: now.db_errors.saturating_sub(prev.db_errors),
        // PLAN_CONFLATION §6.1/§6.2 — deltas, like everything else here.
        conflated: now.conflated.saturating_sub(prev.conflated),
        conflation_conflicts: now
            .conflation_conflicts
            .saturating_sub(prev.conflation_conflicts),
    }
}

// getrusage(2): cumulative CPU (µs) + RSS in bytes.
//
// The RSS reported here is the CURRENT resident set, not ru_maxrss. ru_maxrss is
// the process high-water mark and never decreases, so charting it as "Memory
// Usage" turns any transient spike into a permanent plateau that reads as a
// leak. ru_maxrss is kept only as the last-resort fallback when the live figure
// is unavailable, and it is at least an upper bound then.
pub(crate) fn rusage() -> (u64, u64, u64) {
    unsafe {
        let mut u: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut u) != 0 {
            return (0, 0, 0);
        }
        let user_us = u.ru_utime.tv_sec as u64 * 1_000_000 + u.ru_utime.tv_usec as u64;
        let sys_us = u.ru_stime.tv_sec as u64 * 1_000_000 + u.ru_stime.tv_usec as u64;
        let maxrss = u.ru_maxrss as u64;
        let peak_bytes = if cfg!(target_os = "macos") {
            maxrss
        } else {
            maxrss.saturating_mul(1024)
        };
        (user_us, sys_us, current_rss_bytes().unwrap_or(peak_bytes))
    }
}

// Live resident-set size in bytes, or None when the platform hook is unavailable.
#[cfg(target_os = "linux")]
fn current_rss_bytes() -> Option<u64> {
    // /proc/self/statm field 2 = resident pages.
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return None;
    }
    Some(pages.saturating_mul(page_size as u64))
}

#[cfg(target_os = "macos")]
fn current_rss_bytes() -> Option<u64> {
    // proc_pidinfo(PROC_PIDTASKINFO) -> pti_resident_size (bytes).
    unsafe {
        let mut ti: libc::proc_taskinfo = std::mem::zeroed();
        let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
        let n = libc::proc_pidinfo(
            libc::getpid(),
            libc::PROC_PIDTASKINFO,
            0,
            &mut ti as *mut _ as *mut libc::c_void,
            size,
        );
        if n == size {
            Some(ti.pti_resident_size)
        } else {
            None
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn current_rss_bytes() -> Option<u64> {
    None
}

/// The memory this process holds itself: its resident set without the file
/// pages it maps. Those (the store's LMDB file, read whole at boot; the binary)
/// are page cache the kernel drops under pressure before it kills anything, so
/// the resident set overstates what can run a node out of memory by about the
/// size of its store. None where the platform has no such split.
#[cfg(target_os = "linux")]
pub(crate) fn anon_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("RssAnon:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    Some(kb.saturating_mul(1024))
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn anon_bytes() -> Option<u64> {
    None
}

/// The CPUs this process may run on: its affinity and, on Linux, a cgroup CPU
/// quota (std reads both), so a broker held to 4 cores of 16 says 4.
pub(crate) fn cpus() -> Option<usize> {
    std::thread::available_parallelism().ok().map(|n| n.get())
}

/// The memory this process may use before the kernel steps in: its cgroup's
/// limit when the container has one, else the machine's RAM. Read once: a
/// limit does not move under a running process in practice.
pub(crate) fn memory_limit_bytes() -> Option<u64> {
    static LIMIT: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| cgroup_memory_limit().or_else(physical_memory))
}

// cgroup v2 `memory.max`, else v1 `memory.limit_in_bytes`. "max" does not
// parse, which is v2 saying "no limit"; v1 says it with a number past the RAM.
#[cfg(target_os = "linux")]
fn cgroup_memory_limit() -> Option<u64> {
    let read = |p: &str| {
        std::fs::read_to_string(p)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
    };
    let physical = physical_memory();
    read("/sys/fs/cgroup/memory.max")
        .or_else(|| read("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
        .filter(|&limit| limit > 0 && physical.is_none_or(|p| limit < p))
}

#[cfg(not(target_os = "linux"))]
fn cgroup_memory_limit() -> Option<u64> {
    None
}

#[cfg(unix)]
fn physical_memory() -> Option<u64> {
    // SAFETY: sysconf has no preconditions.
    let (pages, page) = unsafe {
        (
            libc::sysconf(libc::_SC_PHYS_PAGES),
            libc::sysconf(libc::_SC_PAGESIZE),
        )
    };
    (pages > 0 && page > 0).then(|| (pages as u64).saturating_mul(page as u64))
}

#[cfg(not(unix))]
fn physical_memory() -> Option<u64> {
    None
}

/// The filesystem holding `path`: its size and the bytes still available to
/// an unprivileged writer (`statvfs`), which is what the disk gate measures.
// The statvfs fields are u32 on some targets and u64 on others.
#[cfg(unix)]
#[allow(clippy::unnecessary_cast)]
pub(crate) fn filesystem_usage(path: &std::path::Path) -> Option<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `statvfs` initializes `stat` on 0.
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: the successful call above initialized every field.
    let stat = unsafe { stat.assume_init() };
    let frsize = stat.f_frsize as u64;
    Some((
        (stat.f_blocks as u64).saturating_mul(frsize),
        (stat.f_bavail as u64).saturating_mul(frsize),
    ))
}

#[cfg(not(unix))]
pub(crate) fn filesystem_usage(_path: &std::path::Path) -> Option<(u64, u64)> {
    None
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_host_probes_answer_on_this_machine() {
        assert!(super::cpus().is_some_and(|n| n >= 1));
        assert!(super::memory_limit_bytes().is_some_and(|b| b > 0));
        if cfg!(target_os = "linux") {
            let anon = super::anon_bytes().expect("RssAnon in /proc/self/status");
            let rss = super::rusage().2;
            assert!(anon > 0 && anon <= rss, "anon {anon} rss {rss}");
        }
        if cfg!(unix) {
            let (total, available) =
                super::filesystem_usage(&std::env::temp_dir()).expect("statvfs of the temp dir");
            assert!(total > 0 && available <= total, "{total} {available}");
        }
    }
}
