//! A store file that does not hold its pages is refused with a FATAL line
//! naming it and exit 1, instead of a bare bus error
//! ([`crate::rsm::store::sigbus`]); a bus error anywhere else still kills the
//! process by the signal, as before.
//!
//! A bus error cannot be caught, only reported, so it happens in a child: this
//! same test binary, re-executed with [`sigbus_child`] selected and
//! [`CHILD_DIR_ENV`] set (the pattern of `store_crash`). Without the variable
//! the child returns at once, so an ordinary run does nothing there.
//!
//! A file already short when it is mapped faults on Linux (Jepsen's case: a
//! truncated `data.mdb` at boot), while macOS maps zeros past its end, which
//! the load's own checks then refuse as a corrupt value. So the boot case runs
//! on Linux only; a file cut under an open store faults on both.

use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::rsm::store::sigbus::{MapWatch, Notice};
use crate::rsm::store::{HeedStore, Keyspace, Store, StoreOpts, TypedWrites, Writes};

/// Set by the parent: the directory the child works in.
const CHILD_DIR_ENV: &str = "QUEEN_RSM_SIGBUS_DIR";
/// `open`, `cut-running` or `unwatched`: see [`sigbus_child`].
const CHILD_MODE_ENV: &str = "QUEEN_RSM_SIGBUS_MODE";
/// The child's own name, as libtest filters it.
const CHILD_TEST: &str = "rsm::tests::store_sigbus::sigbus_child";
/// What the child prints when it did NOT die.
const SURVIVED: &str = "SIGBUS-CHILD-SURVIVED";

fn opts() -> StoreOpts {
    StoreOpts {
        map_bytes: Some(64 << 20),
        ..Default::default()
    }
}

static SEQ: AtomicU64 = AtomicU64::new(0);

/// A fresh directory, removed on drop.
struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Dir {
        let d = std::env::temp_dir().join(format!(
            "queen-rsm-sigbus-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        Dir(d)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A closed store of many pages in `dir`; its `data.mdb`.
fn filled_store(dir: &Path) -> PathBuf {
    let s = HeedStore::open(dir, &opts()).unwrap();
    {
        let mut w = s.write().unwrap();
        for i in 0..4000u32 {
            w.put_raw(Keyspace::Cursors, &i.to_be_bytes(), &[i as u8; 400])
                .unwrap();
        }
        w.set_applied(1, 1).unwrap();
        w.durable_commit().unwrap();
    }
    s.close();
    let data = dir.join("data.mdb");
    let len = std::fs::metadata(&data).unwrap().len();
    assert!(len > 1 << 20, "a store of many pages: {len} B");
    data
}

/// Cut `data` to half its length, as a disk that lost its tail: the B-tree's
/// newest pages (its root among them, copy-on-write) go with it.
fn cut_in_half(data: &Path) {
    let f = std::fs::OpenOptions::new().write(true).open(data).unwrap();
    let len = f.metadata().unwrap().len();
    f.set_len(len / 2).unwrap();
}

fn run_child(dir: &Path, mode: &str) -> Output {
    let exe = std::env::current_exe().expect("the test binary");
    Command::new(exe)
        .args(["--exact", CHILD_TEST, "--nocapture", "--test-threads", "1"])
        .env(CHILD_DIR_ENV, dir)
        .env(CHILD_MODE_ENV, mode)
        .output()
        .expect("run the child")
}

fn assert_refused_with_a_fatal_line(out: &Output, data: &Path) {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stdout.contains(SURVIVED),
        "the walk must not get past a missing page: {stdout}"
    );
    assert_eq!(
        (out.status.code(), out.status.signal()),
        (Some(1), None),
        "a refusal's exit, not a bus error: {stderr}"
    );
    assert!(
        stderr.contains("FATAL store: bus error (SIGBUS) reading")
            && stderr.contains(&data.display().to_string()),
        "one FATAL line naming the file: {stderr}"
    );
    assert!(
        stderr.contains("rejoin from a peer"),
        "the operator is told what to do: {stderr}"
    );
}

/// `open`: open the store in the directory (the boot). `cut-running`: open
/// it, cut its file in half, scrub it (a walk under a running node).
/// `unwatched`: fault outside any watch, once the handler is installed.
#[test]
fn sigbus_child() {
    let Ok(dir) = std::env::var(CHILD_DIR_ENV) else {
        // The ordinary run: nothing to do.
        return;
    };
    let dir = PathBuf::from(dir);
    match std::env::var(CHILD_MODE_ENV).as_deref() {
        Ok("open") => {
            let opened = HeedStore::open(&dir, &opts()).map(|s| s.close());
            println!("{SURVIVED} open: {opened:?}");
        }
        Ok("cut-running") => {
            let s = HeedStore::open(&dir, &opts()).unwrap();
            cut_in_half(&dir.join("data.mdb"));
            let scrubbed = s.scrub().map(|_| ());
            println!("{SURVIVED} scrub: {scrubbed:?}");
        }
        Ok("unwatched") => {
            // The handler goes in with the first watch; this one is gone
            // before the fault.
            drop(MapWatch::new(&Notice::for_dir(&dir)));
            let path = dir.join("plain.bin");
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            f.set_len(256 << 10).unwrap();
            // SAFETY: a private file only this process uses.
            let map = unsafe { memmap2::Mmap::map(&f) }.unwrap();
            f.set_len(0).unwrap();
            // Past the end of the file now: a bus error.
            let b = unsafe { std::ptr::read_volatile(&map[128 << 10]) };
            println!("{SURVIVED} read {b}");
        }
        other => panic!("unknown child mode {other:?}"),
    }
}

#[cfg(target_os = "linux")]
#[test]
fn a_store_file_cut_short_is_refused_at_the_open_with_a_fatal_line() {
    let d = Dir::new("boot");
    let store_dir = d.0.join("store");
    let data = filled_store(&store_dir);
    cut_in_half(&data);
    assert_refused_with_a_fatal_line(&run_child(&store_dir, "open"), &data);
}

#[test]
fn a_store_file_cut_under_a_running_store_is_refused_with_a_fatal_line() {
    let d = Dir::new("running");
    let store_dir = d.0.join("store");
    let data = filled_store(&store_dir);
    assert_refused_with_a_fatal_line(&run_child(&store_dir, "cut-running"), &data);
}

#[test]
fn a_bus_error_outside_a_store_walk_still_kills_by_the_signal() {
    let d = Dir::new("unwatched");
    let out = run_child(&d.0, "unwatched");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !String::from_utf8_lossy(&out.stdout).contains(SURVIVED),
        "the read past the end must fault"
    );
    assert_eq!(
        out.status.signal(),
        Some(libc::SIGBUS),
        "the action from before handles it: {:?} {stderr}",
        out.status
    );
    assert!(!stderr.contains("FATAL store"), "{stderr}");
}
