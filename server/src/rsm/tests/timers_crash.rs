//! WP-2.3 — `kill -9` of a node holding timers, with NO durable point after its
//! open, then the reopen: the timers exist only in the WAL (the per-queue logs
//! and the system log, Phase C), and replay must bring back exactly what the
//! node had answered.
//!
//! The child (this same test binary re-executed with [`CRASH_DIR_ENV`] set)
//! opens a real [`RaftFacade`] with the durable cadence pushed out to an hour,
//! schedules two timers in one call — `a` already due, `b` due 1.5 s later —
//! waits until `a` has FIRED (its fire entry applied), says `READY`, and is
//! killed. The parent then checks, in order:
//!
//! 1. the store's checkpoint does not hold either timer: nothing reached a
//!    durable point, so whatever comes back comes back through replay;
//! 2. the reopened node fires `b` (scheduled before the kill, replayed from the
//!    system log) — and does NOT fire `a` again (its fire entry replays exactly
//!    once, with the append and the timer's removal together);
//! 3. the queue holds exactly two messages, `tx-a` then `tx-b`.
//!
//! As in `replicator_crash.rs`, `kill -9` keeps the page cache, so this proves
//! the bookkeeping (replay boundary, exactly-once apply of the fire entry), not
//! unsynced bytes; the dropped-writes run belongs to the VM.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use super::apply::store_opts;
use super::timers::{apply, build_ctx, fast_cfg, pop_all, scratch, wait_gone};
use crate::rsm::facade::real::RaftFacade;
use crate::rsm::store::{HeedStore, Store, TypedReads};

const CRASH_DIR_ENV: &str = "QUEEN_RSM_TIMER_CRASH_DIR";
const CHILD_TEST: &str = "rsm::tests::timers_crash::crash_child_timers";
const QUEUE: &str = "crash-q";
const TENANT: &str = "default";

fn sched(key: &str, delay_ms: i64, txn: &str) -> serde_json::Value {
    use base64::Engine;
    serde_json::json!({
        "op": "schedule", "queue": QUEUE, "timerKey": key, "delayMs": delay_ms,
        "txn": txn,
        "payload": base64::engine::general_purpose::STANDARD.encode(format!("{{\"k\":\"{key}\"}}")),
    })
}

fn say(line: &str) {
    use std::io::Write;
    println!("{line}");
    let _ = std::io::stdout().flush();
}

/// The child: schedule, let `a` fire, report, wait to be killed. Does nothing
/// unless the parent set [`CRASH_DIR_ENV`].
#[test]
fn crash_child_timers() {
    let Ok(dir) = std::env::var(CRASH_DIR_ENV) else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("child runtime");
    rt.block_on(async move {
        let f = RaftFacade::open_with(&build_ctx(&dir), fast_cfg()).expect("child: open");
        let r = apply(&f, vec![sched("a", 0, "tx-a"), sched("b", 1_500, "tx-b")]).await;
        assert_eq!(r.len(), 2, "child: {r:?}");
        assert!(
            wait_gone(&f, QUEUE, "a", Duration::from_secs(10)).await,
            "child: `a` never fired"
        );
        say("READY");
        std::thread::sleep(Duration::from_secs(120));
        panic!("child: was not killed");
    });
}

struct Kid {
    child: Child,
    lines: mpsc::Receiver<String>,
}

impl Kid {
    fn spawn(dir: &std::path::Path) -> Kid {
        let exe = std::env::current_exe().expect("test binary");
        let mut child = Command::new(&exe)
            .args(["--exact", CHILD_TEST, "--nocapture", "--test-threads", "1"])
            .env(CRASH_DIR_ENV, dir)
            // No durable point for the child's whole life (§11.4): everything
            // after its open is in the WAL only when the kill lands.
            .env("QUEEN_RAFT_DURABLE_EVERY_MS", "3600000")
            .env("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the child");
        let stdout = child.stdout.take().expect("piped stdout");
        let (tx, lines) = mpsc::channel::<String>();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout)
                .lines()
                .map_while(std::result::Result::ok)
            {
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        Kid { child, lines }
    }

    fn wait_for(&self, marker: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            match self.lines.recv_timeout(Duration::from_secs(5)) {
                Ok(line) if line.contains(marker) => return true,
                Ok(_) => continue,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => return false,
            }
        }
        false
    }

    fn kill(mut self) {
        self.child.kill().expect("kill -9");
        let status = self.child.wait().expect("reap");
        assert!(!status.success(), "the child was supposed to be killed");
    }
}

#[test]
fn a_killed_node_replays_its_timers_and_each_fires_exactly_once() {
    let dir = scratch("crash");
    std::fs::create_dir_all(&dir).expect("dir");

    let kid = Kid::spawn(&dir);
    assert!(kid.wait_for("READY"), "the child never got `a` to fire");
    kid.kill();

    // (1) The checkpoint holds neither timer: no durable point covered the
    // schedule, so both come back through replay or not at all.
    {
        let store = HeedStore::open(&dir.join("store"), &store_opts()).expect("open checkpoint");
        let (a, b) = store
            .read(|r| {
                Ok((
                    r.timer(TENANT, QUEUE, "a")?.is_some(),
                    r.timer(TENANT, QUEUE, "b")?.is_some(),
                ))
            })
            .expect("read checkpoint");
        assert!(
            !a && !b,
            "the checkpoint already held the timers (a={a}, b={b}): the kill did not \
             land without a durable point"
        );
        store.close();
    }

    // (2) + (3) Reopen: replay, then the leader loop fires what is due.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let f = RaftFacade::open_with(&build_ctx(&dir), fast_cfg()).expect("reopen the killed dir");
        assert!(
            wait_gone(&f, QUEUE, "b", Duration::from_secs(10)).await,
            "`b` was scheduled before the kill and never fired after it"
        );
        // Room for a wrong second fire of `a` to show up.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let msgs = pop_all(&f, QUEUE).await;
        let txns: Vec<&str> = msgs
            .iter()
            .map(|m| m["transactionId"].as_str().unwrap_or(""))
            .collect();
        assert_eq!(
            txns,
            vec!["tx-a", "tx-b"],
            "each timer delivered exactly once, in fire order: {msgs:?}"
        );
        f.shutdown().await;
    });
    let _ = std::fs::remove_dir_all(&dir);
}
