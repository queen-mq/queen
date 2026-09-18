//! A real `kill -9` around a roll.
//!
//! The parent re-executes THIS test binary as a child (`--exact` on the child
//! test below, driven by the environment), lets it append and roll, and the
//! child sends itself `SIGKILL`. The parent then recovers the tree against the
//! last state the child managed to record and checks that everything that
//! state believes in is still readable.
//!
//! What this can and cannot prove. A process kill does not drop the page
//! cache, so bytes written and not fsynced still survive it: this test
//! falsifies the BOOKKEEPING — recorded lengths, the seal, the `.qidx`, the
//! file the store calls active — and not the durability of unsynced bytes.
//! That is finding R-02 of RAFT_STATUS.md, and the run that can falsify
//! durability is the dropped-unflushed-writes one on the Linux VM (§13.6),
//! which is WP-1.11's, not this WP's.
//!
//! The child stands in for the store with a `RECORDED` file, written
//! atomically after every durable point exactly as §11.4 step 2 writes the
//! file lengths into the store commit. A kill between the fsyncs and that
//! write leaves the state BEHIND the files, which is the ordinary case
//! recovery truncates.

use std::io::Write;
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::Command;

use super::super::*;
use super::{blob, hashes, TmpDir};

const DIR: &str = "QUEEN_SEG_CRASH_DIR";
const KILL_AFTER: &str = "QUEEN_SEG_CRASH_KILL_AFTER";
const SEGMENT_BYTES: u64 = 4096;
const BLOB_LEN: usize = 900;
const DURABLE_EVERY: u64 = 3;
const CHILD: &str = "rsm::segments::tests::crash::child_appends_until_it_is_killed";

fn state_path(root: &Path) -> std::path::PathBuf {
    root.join("RECORDED")
}

/// The store's `files` keyspace plus the positions apply had made durable,
/// written the way §11.4 step 2 writes them: one atomic commit after the
/// fsyncs.
fn write_state(root: &Path, s: &Segments, durable: &[(Position, Pid, u64)]) {
    let mut out = String::new();
    // The WHOLE row of §6.2, liveness included: a store that keeps only the
    // lengths reopens believing every sealed file is dead (see `FileState`).
    for (bucket, file_id, m) in s.files() {
        out.push_str(&format!(
            "F {bucket} {file_id} {} {} {} {} {} {} {} {}\n",
            m.durable_bytes,
            m.durable_bytes,
            u8::from(m.sealed),
            m.frames,
            m.retained_frames,
            m.retained_bytes,
            m.window_frames,
            m.snapshot_refs,
        ));
    }
    for (p, pid, base) in durable {
        out.push_str(&format!(
            "P {} {} {} {} {pid} {base}\n",
            p.bucket, p.file_id, p.offset, p.len
        ));
    }
    let tmp = root.join("RECORDED.tmp");
    {
        let mut f = std::fs::File::create(&tmp).expect("state tmp");
        f.write_all(out.as_bytes()).expect("write state");
        f.sync_all().expect("sync state");
    }
    std::fs::rename(&tmp, state_path(root)).expect("rename state");
    if let Ok(d) = std::fs::File::open(root) {
        let _ = d.sync_all();
    }
}

#[allow(clippy::type_complexity)]
fn read_state(root: &Path) -> (Vec<FileState>, Vec<(Position, Pid, u64)>) {
    let Ok(text) = std::fs::read_to_string(state_path(root)) else {
        return (Vec::new(), Vec::new());
    };
    let mut files = Vec::new();
    let mut pos = Vec::new();
    for line in text.lines() {
        let f: Vec<&str> = line.split(' ').collect();
        match f.first() {
            Some(&"F") if f.len() == 11 => files.push(FileState {
                bucket: f[1].parse().expect("bucket"),
                file_id: f[2].parse().expect("file"),
                len: f[3].parse().expect("len"),
                durable_len: f[4].parse().expect("durable len"),
                sealed: f[5] == "1",
                frames: f[6].parse().expect("frames"),
                retained_frames: f[7].parse().expect("retained frames"),
                retained_bytes: f[8].parse().expect("retained bytes"),
                window_frames: f[9].parse().expect("window frames"),
                snapshot_refs: f[10].parse().expect("snapshot refs"),
            }),
            Some(&"P") if f.len() == 7 => pos.push((
                Position {
                    bucket: f[1].parse().expect("bucket"),
                    file_id: f[2].parse().expect("file"),
                    offset: f[3].parse().expect("offset"),
                    len: f[4].parse().expect("len"),
                },
                f[5].parse::<Pid>().expect("pid"),
                f[6].parse::<u64>().expect("base"),
            )),
            _ => panic!("unreadable state line {line:?}"),
        }
    }
    (files, pos)
}

fn kill_self() -> ! {
    let rc = unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
    // `kill(2)` RETURNS before the signal lands: delivery happens when this
    // thread next returns to user space, which is not necessarily before the
    // next instruction. Racing it — `kill` then `abort` — really does lose,
    // and cost this test a confusing SIGABRT. So wait for the signal instead,
    // and only give up after long enough that "it did not arrive" is a fact
    // about the platform and not about scheduling.
    for _ in 0..1000 {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    eprintln!("SIGKILL (kill returned {rc}) was not delivered in 10 s");
    std::process::abort();
}

/// The child. Does nothing unless the parent asked for it.
#[test]
#[ignore = "driven by kill_9_around_a_roll, which sets the environment"]
fn child_appends_until_it_is_killed() {
    let Ok(root) = std::env::var(DIR) else { return };
    let root = std::path::PathBuf::from(root);
    let kill_after: u64 = std::env::var(KILL_AFTER)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let (files, _) = read_state(&root);
    let (mut s, _) = Segments::open(&root.join("seg"), Options::testing(SEGMENT_BYTES), &files)
        .expect("child opens the tree");

    if kill_after == 0 {
        // The timed kill: a watchdog fires somewhere inside the append loop,
        // which is the only way to land INSIDE a roll (between the `.qidx`
        // rename and the create of the next file) without a fault point.
        let delay = 3 + (std::process::id() as u64 % 17);
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(delay));
            kill_self();
        });
    }

    let mut durable: Vec<(Position, Pid, u64)> = Vec::new();
    let mut pending: Vec<(Position, Pid, u64)> = Vec::new();
    for i in 0..400u64 {
        if kill_after != 0 && i == kill_after {
            kill_self();
        }
        let pid = 1 + i % 2;
        let base = i;
        let p = s
            .append(
                0,
                pid,
                base,
                1,
                1_700_000_000_000_000 + i as i64,
                &hashes(pid ^ base, 1),
                &blob(pid ^ base, BLOB_LEN),
            )
            .expect("child append");
        pending.push((p, pid, base));
        if (i + 1) % DURABLE_EVERY == 0 {
            s.durable_point().expect("child durable point");
            durable.append(&mut pending);
            write_state(&root, &s, &durable);
        }
    }
    kill_self();
}

#[test]
fn kill_9_around_a_roll() {
    if std::env::var(DIR).is_ok() {
        return; // we ARE a child
    }
    let exe = std::env::current_exe().expect("test binary");
    // ~4 frames of 900 bytes per 4 KiB file and a durable point every 3, so
    // these counts straddle a roll, a durable point, and both at once. 0 is
    // the timed kill, which can land inside `roll` itself.
    let points = [4u64, 5, 6, 9, 0, 0];
    let mut rolled_at_least_once = false;
    let mut durable_frames = 0usize;
    let mut raced_the_state_write = false;

    for kill in points {
        let d = TmpDir::new(&format!("crash-{kill}"));
        let out = Command::new(&exe)
            .args(["--exact", CHILD, "--include-ignored", "--nocapture"])
            .env(DIR, d.path())
            .env(KILL_AFTER, kill.to_string())
            .output()
            .expect("spawn the child");
        assert_eq!(
            out.status.signal(),
            Some(9),
            "the child must die of SIGKILL, not exit ({:?}); stderr: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );

        let (files, positions) = read_state(d.path());
        let (s, rep) = Segments::open(&d.seg(), Options::testing(SEGMENT_BYTES), &files)
            .unwrap_or_else(|e| panic!("kill after {kill}: recovery refused: {e}"));

        // Everything the state recorded as durable is still there, and is the
        // frame the state thinks it is.
        durable_frames += positions.len();
        if !rep.truncated.is_empty() || !rep.deleted.is_empty() {
            raced_the_state_write = true;
        }
        for (p, pid, base) in &positions {
            let f = s
                .read(*p)
                .unwrap_or_else(|e| panic!("kill after {kill}: {p:?} did not read back: {e}"));
            assert_eq!((f.pid, f.base_offset, f.count), (*pid, *base, 1));
            assert_eq!(f.blob, blob(pid ^ base, BLOB_LEN));
            assert_eq!(f.hashes, hashes(pid ^ base, 1));
        }

        // Every file the state calls sealed has a usable index — written at
        // the seal, or rebuilt by recovery.
        let sealed: Vec<&FileState> = files.iter().filter(|f| f.sealed).collect();
        if !sealed.is_empty() {
            rolled_at_least_once = true;
        }
        for f in &sealed {
            let v = index::View::open(
                &d.seg()
                    .join(format!("b{:03}", f.bucket))
                    .join(format!("f{:010}.qidx", f.file_id)),
                Some(f.len),
            )
            .unwrap_or_else(|e| {
                panic!(
                    "kill after {kill}: sealed b{}/f{} has no index: {e}",
                    f.bucket, f.file_id
                )
            });
            v.check_identity(f.bucket, f.file_id).expect("identity");
        }

        // The recovered tree keeps working: a lookup over the sealed files
        // and an append that lands after the recorded length.
        let ids: Vec<u32> = {
            let mut v: Vec<u32> = sealed
                .iter()
                .filter(|f| f.bucket == 0)
                .map(|f| f.file_id)
                .collect();
            v.sort_unstable();
            v
        };
        let mut s = s;
        let before = s.active_len(0);
        let p = s
            .append(0, 1, 9_999, 1, 1, &hashes(1, 1), &blob(1, 32))
            .expect("append after recovery");
        assert_eq!(p.offset, before, "kill after {kill}: the tail moved");
        assert_eq!(s.read(p).expect("read back").base_offset, 9_999);
        for (pos, pid, base) in positions.iter().take(3) {
            if let Some(l) = s.locate(0, *pid, *base, &ids).expect("locate") {
                assert_eq!(l.position, *pos);
            }
        }
        // Truncations and deletions of unrecorded files are legal and
        // expected (the kill can land between a roll and the state write, or
        // before the first state write at all, which makes the whole tree a
        // leftover). What must NEVER happen is deleting a file that holds a
        // frame the state recorded as durable.
        for (p, _, _) in &positions {
            assert!(
                !rep.deleted.contains(&(p.bucket, p.file_id)),
                "kill after {kill}: b{:03}/f{} held a durable frame and was deleted",
                p.bucket,
                p.file_id
            );
        }
    }
    assert!(
        rolled_at_least_once,
        "no run crossed a roll: the test did not test what it says"
    );
    assert!(
        durable_frames > 0,
        "no run got far enough to record a durable frame"
    );
    assert!(
        raced_the_state_write,
        "no run was killed between a write and the state that records it"
    );
}
