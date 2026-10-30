//! The memory guard: a safety net for memory pressure.
//!
//! Every keyspace lives in RAM on every node, and many request paths can
//! multiply their input many times over in memory. On 2026-10-29 such a
//! multiplier took the leader past its pod limit and the kernel killed it, and
//! then each new leader. Nothing in the broker looked at its own memory: every
//! amplifier ran until the OOM kill.
//!
//! A background task reads, every [`TICK`], the memory limit (cgroup v2
//! `memory.max`, else `QUEEN_MEMORY_LIMIT_BYTES`; neither: the guard is off)
//! and this process's anonymous resident memory (`RssAnon` in
//! `/proc/self/status`; not Linux: off). Anonymous memory is the part the
//! kernel cannot reclaim: the page cache of the segment files is counted by
//! the cgroup too, but is dropped before anyone is killed.
//!
//! Two levels, in percent of the limit:
//! - soft (`QUEEN_MEMGUARD_SOFT_PCT`, 80): heavy reads are refused
//!   (`503 memory_pressure` + `Retry-After`);
//! - hard (`QUEEN_MEMGUARD_HARD_PCT`, 90): writes that grow the stored state
//!   are refused too.
//!
//! What drains memory is never refused: pops, acks, lease renewals, every
//! DELETE, health, metrics, raft and auth routes ([`classify`]). The HTTP
//! middleware is `handlers::raft::memory_guard`.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::Duration;

/// How often the guard reads the process's memory.
pub const TICK: Duration = Duration::from_millis(250);

/// No pressure.
pub const LEVEL_OK: u8 = 0;
/// Over the soft level: heavy reads are refused.
pub const LEVEL_SOFT: u8 = 1;
/// Over the hard level: storage-growing writes are refused too.
pub const LEVEL_HARD: u8 = 2;

/// Percentage points a level is left below its threshold, so a process
/// hovering at the line does not flip between levels every tick.
const HYSTERESIS_PCT: u64 = 2;

static LEVEL: AtomicU8 = AtomicU8::new(LEVEL_OK);
static LIMIT_BYTES: AtomicU64 = AtomicU64::new(0);
static RSS_BYTES: AtomicU64 = AtomicU64::new(0);
static REFUSED_PRESSURE: AtomicU64 = AtomicU64::new(0);
static REFUSED_BUSY: AtomicU64 = AtomicU64::new(0);

/// The current level ([`LEVEL_OK`], [`LEVEL_SOFT`], [`LEVEL_HARD`]).
pub fn level() -> u8 {
    LEVEL.load(Ordering::Relaxed)
}

/// The memory limit the guard works against, in bytes (0: none known).
pub fn limit_bytes() -> u64 {
    LIMIT_BYTES.load(Ordering::Relaxed)
}

/// The process's anonymous resident memory at the last tick, in bytes.
pub fn rss_bytes() -> u64 {
    RSS_BYTES.load(Ordering::Relaxed)
}

/// Requests refused: `(memory_pressure, busy)`.
pub fn refused_totals() -> (u64, u64) {
    (
        REFUSED_PRESSURE.load(Ordering::Relaxed),
        REFUSED_BUSY.load(Ordering::Relaxed),
    )
}

/// Count one request refused for memory pressure.
pub fn note_refused_pressure() {
    REFUSED_PRESSURE.fetch_add(1, Ordering::Relaxed);
}

/// Count one heavy read refused because every heavy-read slot stayed taken.
pub fn note_refused_busy() {
    REFUSED_BUSY.fetch_add(1, Ordering::Relaxed);
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
}

/// `QUEEN_MEMGUARD_SOFT_PCT` (default 80, 1 to 100).
pub fn soft_pct() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        env_u64("QUEEN_MEMGUARD_SOFT_PCT")
            .filter(|v| *v > 0)
            .unwrap_or(80)
            .min(100)
    })
}

/// `QUEEN_MEMGUARD_HARD_PCT` (default 90, never under the soft level).
pub fn hard_pct() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        env_u64("QUEEN_MEMGUARD_HARD_PCT")
            .filter(|v| *v > 0)
            .unwrap_or(90)
            .clamp(soft_pct(), 100)
    })
}

/// `QUEEN_HEAVY_READS_MAX` (default 32): heavy reads served at once on this
/// node; more wait up to [`HEAVY_READ_WAIT`], then answer `503 busy`.
pub fn heavy_reads_max() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        env_u64("QUEEN_HEAVY_READS_MAX")
            .filter(|v| *v > 0)
            .map_or(32, |v| v.min(1 << 20) as usize)
    })
}

/// How long a heavy read waits for a slot before it is refused `busy`.
pub const HEAVY_READ_WAIT: Duration = Duration::from_secs(2);

/// The node's heavy-read slots ([`heavy_reads_max`]).
pub fn heavy_reads() -> &'static std::sync::Arc<tokio::sync::Semaphore> {
    static S: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
        std::sync::OnceLock::new();
    S.get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(heavy_reads_max())))
}

/// The level for `rss` against `limit`, from the level the guard is at
/// (`prev`): a level is entered at its threshold and left only
/// [`HYSTERESIS_PCT`] points below it. No limit: [`LEVEL_OK`].
pub fn next_level(prev: u8, rss: u64, limit: u64, soft: u64, hard: u64) -> u8 {
    if limit == 0 {
        return LEVEL_OK;
    }
    // Per mille, in u128: a limit near u64::MAX must not overflow.
    let pct_x10 = (u128::from(rss) * 1000 / u128::from(limit)) as u64;
    let over = |pct: u64, held: bool| {
        let line = if held {
            pct.saturating_sub(HYSTERESIS_PCT)
        } else {
            pct
        };
        pct_x10 >= line * 10
    };
    if over(hard, prev >= LEVEL_HARD) {
        LEVEL_HARD
    } else if over(soft, prev >= LEVEL_SOFT) {
        LEVEL_SOFT
    } else {
        LEVEL_OK
    }
}

/// The cgroup v2 limit (`/sys/fs/cgroup/memory.max`), `None` when unlimited
/// or not readable.
fn cgroup_limit() -> Option<u64> {
    let raw = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    let v = raw.trim();
    if v == "max" {
        return None;
    }
    v.parse::<u64>().ok().filter(|n| *n > 0)
}

/// The limit the guard works against: the cgroup's, else
/// `QUEEN_MEMORY_LIMIT_BYTES`.
fn memory_limit() -> Option<u64> {
    cgroup_limit().or_else(|| env_u64("QUEEN_MEMORY_LIMIT_BYTES").filter(|n| *n > 0))
}

/// `RssAnon` of `/proc/self/status`, in bytes (`None`: not Linux).
fn rss_anon() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_rss_anon(&status)
}

/// `RssAnon:   123456 kB` → bytes.
fn parse_rss_anon(status: &str) -> Option<u64> {
    let line = status.lines().find(|l| l.starts_with("RssAnon:"))?;
    let kb = line
        .trim_start_matches("RssAnon:")
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?;
    Some(kb.saturating_mul(1024))
}

/// Start the guard (once per process; later calls do nothing). Must run
/// inside a tokio runtime. With no limit known, or no `/proc`, it logs once
/// and stays at [`LEVEL_OK`].
pub fn spawn() {
    static STARTED: std::sync::Once = std::sync::Once::new();
    STARTED.call_once(|| {
        let (Some(limit), Some(_)) = (memory_limit(), rss_anon()) else {
            tracing::info!(
                target: "boot",
                "memory guard off: no cgroup memory.max or QUEEN_MEMORY_LIMIT_BYTES, or no \
                 /proc/self/status"
            );
            return;
        };
        let (soft, hard) = (soft_pct(), hard_pct());
        LIMIT_BYTES.store(limit, Ordering::Relaxed);
        tracing::info!(
            target: "boot",
            limit_bytes = limit,
            soft_pct = soft,
            hard_pct = hard,
            heavy_reads_max = heavy_reads_max(),
            "memory guard on"
        );
        tokio::spawn(async move {
            let mut ticks = 0u64;
            let mut limit = limit;
            loop {
                tokio::time::sleep(TICK).await;
                ticks = ticks.wrapping_add(1);
                // The limit can change under a live pod (a resize): read it
                // again every few seconds.
                if ticks % 16 == 0 {
                    if let Some(l) = memory_limit() {
                        limit = l;
                        LIMIT_BYTES.store(l, Ordering::Relaxed);
                    }
                }
                let Some(rss) = rss_anon() else {
                    continue;
                };
                RSS_BYTES.store(rss, Ordering::Relaxed);
                let prev = LEVEL.load(Ordering::Relaxed);
                let next = next_level(prev, rss, limit, soft, hard);
                if next == prev {
                    continue;
                }
                LEVEL.store(next, Ordering::Relaxed);
                let pct = (u128::from(rss) * 100 / u128::from(limit.max(1))) as u64;
                let name = match next {
                    LEVEL_HARD => "hard",
                    LEVEL_SOFT => "soft",
                    _ => "ok",
                };
                if next > prev {
                    tracing::warn!(
                        target: "memguard",
                        level = name,
                        rss_anon_bytes = rss,
                        limit_bytes = limit,
                        pct,
                        "memory pressure: refusing heavy reads{}",
                        if next == LEVEL_HARD {
                            " and storage-growing writes"
                        } else {
                            ""
                        }
                    );
                } else {
                    tracing::info!(
                        target: "memguard",
                        level = name,
                        rss_anon_bytes = rss,
                        limit_bytes = limit,
                        pct,
                        "memory pressure eased"
                    );
                }
            }
        });
    });
}

/// What the guard does with a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteClass {
    /// Served whatever the level: it drains memory, or it is small and
    /// operational (pops, acks, renewals, deletes, health, metrics, raft,
    /// auth).
    Free,
    /// A write that grows the stored state: refused at the hard level.
    Grows,
    /// A read whose answer can be large (pages, scans, states): refused from
    /// the soft level, and served [`heavy_reads_max`] at a time.
    HeavyRead,
}

/// `path` is `base` or below it.
fn under(path: &str, base: &str) -> bool {
    path.strip_prefix(base)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

/// The guard's class of a request.
pub fn classify(method: &axum::http::Method, path: &str) -> RouteClass {
    use axum::http::Method;
    if *method == Method::DELETE {
        return RouteClass::Free;
    }
    if *method == Method::POST {
        return match path {
            "/api/v1/push"
            | "/api/v1/transaction"
            | "/streams/v1/cycle"
            | "/api/v1/kv"
            | "/api/v1/timers"
            | "/api/v1/traces" => RouteClass::Grows,
            "/streams/v1/state/get" | "/api/v1/fetch" | "/api/v1/resources/kv/list" => {
                RouteClass::HeavyRead
            }
            _ => RouteClass::Free,
        };
    }
    if *method == Method::PUT {
        return if under(path, "/api/v1/kv") {
            RouteClass::Grows
        } else {
            RouteClass::Free
        };
    }
    if *method == Method::GET {
        let heavy = under(path, "/api/v1/messages")
            || under(path, "/api/v1/dlq")
            || under(path, "/api/v1/traces")
            || under(path, "/api/v1/analytics")
            || under(path, "/api/v1/status")
            || under(path, "/api/v1/resources")
            || under(path, "/api/v1/consumer-groups");
        return if heavy {
            RouteClass::HeavyRead
        } else {
            RouteClass::Free
        };
    }
    RouteClass::Free
}

/// `Retry-After` for a refusal: 1 to 5 s, spread so refused clients do not
/// come back together.
pub fn retry_after_s() -> u64 {
    use rand::Rng;
    rand::thread_rng().gen_range(1..=5)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Method;

    #[test]
    fn levels_enter_at_the_line_and_leave_below_it() {
        let gib = 1u64 << 30;
        let l = 10 * gib;
        assert_eq!(next_level(LEVEL_OK, 7 * gib, l, 80, 90), LEVEL_OK);
        assert_eq!(next_level(LEVEL_OK, 8 * gib, l, 80, 90), LEVEL_SOFT);
        assert_eq!(next_level(LEVEL_OK, 9 * gib, l, 80, 90), LEVEL_HARD);
        // 89%: still hard (left at 88%), soft from ok.
        let r89 = l / 100 * 89;
        assert_eq!(next_level(LEVEL_HARD, r89, l, 80, 90), LEVEL_HARD);
        assert_eq!(next_level(LEVEL_OK, r89, l, 80, 90), LEVEL_SOFT);
        assert_eq!(next_level(LEVEL_SOFT, l / 100 * 77, l, 80, 90), LEVEL_OK);
        assert_eq!(next_level(LEVEL_HARD, 9 * gib, 0, 80, 90), LEVEL_OK);
        assert_eq!(next_level(LEVEL_OK, u64::MAX, u64::MAX, 80, 90), LEVEL_HARD);
    }

    #[test]
    fn rss_anon_parses() {
        let s = "Name:\tqueen\nRssAnon:\t  2048 kB\nRssFile:\t 10 kB\n";
        assert_eq!(parse_rss_anon(s), Some(2048 * 1024));
        assert_eq!(parse_rss_anon("Name: x\n"), None);
    }

    #[test]
    fn drains_are_never_refused() {
        let c = |m: Method, p: &str| classify(&m, p);
        assert_eq!(c(Method::GET, "/api/v1/pop/queue/q"), RouteClass::Free);
        assert_eq!(c(Method::POST, "/api/v1/ack"), RouteClass::Free);
        assert_eq!(c(Method::POST, "/api/v1/ack/batch"), RouteClass::Free);
        assert_eq!(c(Method::POST, "/api/v1/lease/x/extend"), RouteClass::Free);
        assert_eq!(c(Method::DELETE, "/api/v1/kv/ns/k"), RouteClass::Free);
        assert_eq!(c(Method::DELETE, "/api/v1/timers/q/k"), RouteClass::Free);
        assert_eq!(c(Method::GET, "/health"), RouteClass::Free);
        assert_eq!(c(Method::GET, "/metrics/prometheus"), RouteClass::Free);
        assert_eq!(c(Method::GET, "/api/v1/raft/status"), RouteClass::Free);
        assert_eq!(
            c(Method::GET, "/api/v1/system/raft/membership"),
            RouteClass::Free
        );
        assert_eq!(c(Method::GET, "/auth/me"), RouteClass::Free);
        assert_eq!(c(Method::POST, "/api/v1/push"), RouteClass::Grows);
        assert_eq!(c(Method::POST, "/streams/v1/cycle"), RouteClass::Grows);
        assert_eq!(c(Method::PUT, "/api/v1/kv/ns/k"), RouteClass::Grows);
        assert_eq!(c(Method::GET, "/api/v1/messages"), RouteClass::HeavyRead);
        assert_eq!(
            c(Method::GET, "/api/v1/messages/q/p"),
            RouteClass::HeavyRead
        );
        assert_eq!(c(Method::GET, "/api/v1/messagesx"), RouteClass::Free);
        assert_eq!(
            c(Method::POST, "/streams/v1/state/get"),
            RouteClass::HeavyRead
        );
        assert_eq!(c(Method::POST, "/api/v1/fetch"), RouteClass::HeavyRead);
        assert_eq!(
            c(Method::GET, "/api/v1/resources/queues"),
            RouteClass::HeavyRead
        );
    }
}
