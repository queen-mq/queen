//! Process and filesystem counters, without pulling in a dependency.
//!
//! RSS and bytes-written come from the kernel's own per-process counters:
//! `/proc/self/{status,io}` on Linux, `proc_pid_rusage(RUSAGE_INFO_V2)` on
//! macOS. Directory sizes are reported as ALLOCATED bytes (st_blocks * 512),
//! not apparent length: LMDB's data file is sparse and its apparent length is
//! the map size, which would make every write-amplification number nonsense.

use std::path::Path;

#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Default, Clone, Copy)]
struct RusageInfoV2 {
    ri_uuid: [u8; 16],
    ri_user_time: u64,
    ri_system_time: u64,
    ri_pkg_idle_wkups: u64,
    ri_interrupt_wkups: u64,
    ri_pageins: u64,
    ri_wired_size: u64,
    ri_resident_size: u64,
    ri_phys_footprint: u64,
    ri_proc_start_abstime: u64,
    ri_proc_exit_abstime: u64,
    ri_child_user_time: u64,
    ri_child_system_time: u64,
    ri_child_pkg_idle_wkups: u64,
    ri_child_interrupt_wkups: u64,
    ri_child_pageins: u64,
    ri_child_elapsed_abstime: u64,
    ri_diskio_bytesread: u64,
    ri_diskio_byteswritten: u64,
}

#[cfg(target_os = "macos")]
extern "C" {
    fn proc_pid_rusage(
        pid: libc::c_int,
        flavor: libc::c_int,
        buffer: *mut libc::c_void,
    ) -> libc::c_int;
}

#[cfg(target_os = "macos")]
fn rusage_v2() -> Option<RusageInfoV2> {
    let mut ri = RusageInfoV2::default();
    let rc = unsafe {
        proc_pid_rusage(
            std::process::id() as libc::c_int,
            2,
            &mut ri as *mut _ as *mut libc::c_void,
        )
    };
    if rc == 0 {
        Some(ri)
    } else {
        None
    }
}

/// Resident set size of this process, in bytes. 0 when unavailable.
pub fn rss_bytes() -> u64 {
    #[cfg(target_os = "macos")]
    {
        return rusage_v2().map(|r| r.ri_resident_size).unwrap_or(0);
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
            for l in s.lines() {
                if let Some(rest) = l.strip_prefix("VmRSS:") {
                    let kb: u64 = rest
                        .split_whitespace()
                        .next()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                    return kb * 1024;
                }
            }
        }
        0
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

/// Bytes this process has sent to the storage layer since it started.
///
/// Linux: `write_bytes` from `/proc/self/io` — bytes the process caused to go
/// to the block layer (it does not count writes that the page cache later
/// dropped, and it does count writeback of our dirty pages).
/// macOS: `ri_diskio_byteswritten`.
/// Returns 0 when the counter is unavailable; callers must say so rather than
/// report a write amplification of zero.
pub fn disk_written_bytes() -> u64 {
    #[cfg(target_os = "macos")]
    {
        return rusage_v2().map(|r| r.ri_diskio_byteswritten).unwrap_or(0);
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/self/io") {
            for l in s.lines() {
                if let Some(rest) = l.strip_prefix("write_bytes:") {
                    return rest.trim().parse().unwrap_or(0);
                }
            }
        }
        0
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

#[allow(dead_code)]
pub fn disk_read_bytes() -> u64 {
    #[cfg(target_os = "macos")]
    {
        return rusage_v2().map(|r| r.ri_diskio_bytesread).unwrap_or(0);
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/self/io") {
            for l in s.lines() {
                if let Some(rest) = l.strip_prefix("read_bytes:") {
                    return rest.trim().parse().unwrap_or(0);
                }
            }
        }
        0
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

/// Allocated bytes under `path` (st_blocks * 512), recursively.
pub fn dir_alloc_bytes(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(p) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&p) else {
            continue;
        };
        for e in rd.flatten() {
            let Ok(md) = e.metadata() else { continue };
            if md.is_dir() {
                stack.push(e.path());
            } else {
                total += md.blocks() * 512;
            }
        }
    }
    total
}

/// Apparent bytes under `path` (file lengths), recursively.
#[allow(dead_code)]
pub fn dir_apparent_bytes(path: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(p) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&p) else {
            continue;
        };
        for e in rd.flatten() {
            let Ok(md) = e.metadata() else { continue };
            if md.is_dir() {
                stack.push(e.path());
            } else {
                total += md.len();
            }
        }
    }
    total
}

pub fn file_count(path: &Path) -> u64 {
    let mut n = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(p) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&p) else {
            continue;
        };
        for e in rd.flatten() {
            let Ok(md) = e.metadata() else { continue };
            if md.is_dir() {
                stack.push(e.path());
            } else {
                n += 1;
            }
        }
    }
    n
}

/// 256 append-only segment buckets plus the engine's own files need more
/// descriptors than the macOS default of 256.
pub fn raise_nofile(target: u64) -> Result<u64, String> {
    unsafe {
        let mut rl: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) != 0 {
            return Err("getrlimit failed".into());
        }
        let hard = rl.rlim_max as u64;
        let want = target.min(if hard == libc::RLIM_INFINITY as u64 {
            target
        } else {
            hard
        });
        if (rl.rlim_cur as u64) >= want {
            return Ok(rl.rlim_cur as u64);
        }
        rl.rlim_cur = want as libc::rlim_t;
        if libc::setrlimit(libc::RLIMIT_NOFILE, &rl) != 0 {
            return Err(format!(
                "setrlimit(NOFILE, {want}) failed (cur={}, max={hard})",
                rl.rlim_cur
            ));
        }
        Ok(want)
    }
}

pub fn now_micros() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

#[allow(dead_code)]
pub fn fsync_dir(path: &Path) -> std::io::Result<()> {
    let f = std::fs::File::open(path)?;
    f.sync_all()
}

pub fn host_line() -> String {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let host = std::process::Command::new("uname")
        .arg("-a")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_default();
    format!("{os}/{arch} {}", host.trim())
}
