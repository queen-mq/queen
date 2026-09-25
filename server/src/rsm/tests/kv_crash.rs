//! WP-2.2 — KV survives `kill -9` WITHOUT a durable point: the WAL replays it.
//!
//! Phase C made the per-queue logs the only write-ahead log and the store a RAM
//! image whose LMDB file is a CHECKPOINT written at the durable point. An entry
//! that touches no queue — every KV entry — goes to the SYSTEM log (`q0/`),
//! fsynced before its answer (I4). So a KV write acknowledged after the last
//! durable point exists ONLY in that log until the next checkpoint, and this
//! test proves the log is enough.
//!
//! The child opens a real [`RaftFacade`] with the durable-point cadence pushed
//! out of reach (`QUEEN_RAFT_DURABLE_EVERY_MS` = 1 h), drives a KV workload
//! through it — puts, a CAS, an incr counter, deletes, a TTL'd marker — prints
//! the state it was answered with, and waits to be killed. The parent then:
//!
//! 1. opens the STORE alone and shows the checkpoint does NOT hold that state:
//!    its applied index is below the child's and its `kv` keyspace is empty —
//!    whatever comes back can only have come from the WAL;
//! 2. reopens the facade (recovery replays the logs from the checkpoint, each
//!    later entry exactly once) and reads every key back: the same values, the
//!    same versions, the same expiry and update stamps; the counter and the
//!    version sequence continue from where they were.
//!
//! As in `replicator_crash.rs`, `kill -9` keeps the page cache, so this
//! falsifies the replay bookkeeping (what is in the log, from where it replays,
//! that apply is exact under replay), not unsynced bytes; that is the VM's.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{Deadline, KvReq, ReqCtx, Rsm, RsmBuildCtx};
use crate::rsm::store::{HeedStore, Keyspace, Reads, Store, TypedReads};

const CRASH_DIR_ENV: &str = "QUEEN_RSM_KV_CRASH_DIR";
const CHILD_TEST: &str = "rsm::tests::kv_crash::crash_child_kv";

/// The keys the workload leaves behind, read back whole on both sides.
const KEYS: [&str; 8] = [
    "k00", "k01", "k05", "k06", "k10", "k19", "counter", "marker",
];

fn build_ctx(dir: &Path) -> RsmBuildCtx {
    RsmBuildCtx {
        data_dir: dir.display().to_string(),
        notifier: crate::notify::Notifier::new(false),
        disk_high_pct: 85.0,
        disk_low_pct: 80.0,
    }
}

fn ctx() -> ReqCtx {
    ReqCtx::new("default", Deadline::after(Duration::from_secs(20)))
}

async fn call(f: &RaftFacade, ops: Value) -> Vec<Value> {
    f.kv(
        ctx(),
        KvReq {
            ops: ops.as_array().expect("ops").clone(),
        },
    )
    .await
    .unwrap_or_else(|e| panic!("kv call failed: {e:?}"))
    .results
}

/// Every surviving key, whole (value, version, expiresAt, updatedAt).
async fn snapshot(f: &RaftFacade) -> Value {
    let r = call(f, json!([{"op":"getMany","ns":"crash","keys":KEYS}])).await;
    r[0].clone()
}

fn say(line: &str) {
    use std::io::Write;
    println!("{line}");
    let _ = std::io::stdout().flush();
}

// ---------------------------------------------------------------------------
// The child
// ---------------------------------------------------------------------------

#[test]
fn crash_child_kv() {
    let Ok(dir) = std::env::var(CRASH_DIR_ENV) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("child runtime");
    rt.block_on(async move {
        let f = RaftFacade::open(&build_ctx(&dir)).expect("child: open facade");
        // Twenty puts, in two batches of ten (one entry each).
        for base in [0, 10] {
            let ops: Vec<Value> = (base..base + 10)
                .map(|i| {
                    json!({"op":"put","ns":"crash","key":format!("k{i:02}"),
                           "value":{"i":i},"forever":true})
                })
                .collect();
            call(&f, json!(ops)).await;
        }
        // A CAS on k10, answered with the version replay must reproduce.
        let g = call(&f, json!([{"op":"get","ns":"crash","key":"k10"}])).await;
        let v = g[0]["version"].as_u64().expect("k10 version");
        let cas = call(
            &f,
            json!([{"op":"put","ns":"crash","key":"k10","value":"cas-won","forever":true,
                    "expect":v}]),
        )
        .await;
        assert_eq!(cas[0]["applied"], true, "child: the CAS must win");
        // A counter, one entry per increment.
        for _ in 0..25 {
            call(
                &f,
                json!([{"op":"incr","ns":"crash","key":"counter","delta":1,"ttlSeconds":86400}]),
            )
            .await;
        }
        // Deletes, and a TTL'd marker.
        let dels: Vec<Value> = (2..5)
            .chain(11..19)
            .map(|i| json!({"op":"delete","ns":"crash","key":format!("k{i:02}")}))
            .collect();
        call(&f, json!(dels)).await;
        call(
            &f,
            json!([{"op":"putIfAbsent","ns":"crash","key":"marker","value":{"m":1},
                    "ttlSeconds":3600}]),
        )
        .await;

        say(&format!("STATE {}", snapshot(&f).await));
        say(&format!("APPLIED {}", f.health().applied));
        say("DONE");
        std::thread::sleep(Duration::from_secs(120));
        panic!("child: was not killed");
    });
}

// ---------------------------------------------------------------------------
// The parent
// ---------------------------------------------------------------------------

#[test]
fn kv_state_survives_a_kill_without_a_durable_point() {
    let dir = std::env::temp_dir().join(format!("queen-rsm-kv-crash-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("crash dir");

    let exe = std::env::current_exe().expect("test binary");
    let mut child = Command::new(&exe)
        .args(["--exact", CHILD_TEST, "--nocapture", "--test-threads", "1"])
        .env(CRASH_DIR_ENV, &dir)
        // No durable point after open: the checkpoint stays behind every KV
        // write, so the reopen can only get them from the WAL.
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

    let mut state: Option<Value> = None;
    let mut applied: Option<u64> = None;
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut done = false;
    // libtest prints `test <name> ... ` without a newline before the child's
    // own output, so a marker is found anywhere in its line.
    while Instant::now() < deadline && !done {
        match lines.recv_timeout(Duration::from_secs(5)) {
            Ok(l) if l.contains("STATE ") => {
                let at = l.find("STATE ").unwrap() + "STATE ".len();
                state = Some(serde_json::from_str(&l[at..]).expect("STATE JSON"))
            }
            Ok(l) if l.contains("APPLIED ") => {
                let at = l.find("APPLIED ").unwrap() + "APPLIED ".len();
                applied = l[at..].trim().parse().ok();
            }
            Ok(l) if l.contains("DONE") => done = true,
            Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    child.kill().expect("kill -9");
    let status = child.wait().expect("reap");
    assert!(done, "the child never finished its workload");
    assert!(!status.success(), "the child was supposed to be killed");
    let state = state.expect("the child printed its state");
    let applied = applied.expect("the child printed its applied index");

    // What the child was answered with: the surviving keys, the CAS value,
    // the counter and the marker.
    let rows = state["rows"].as_array().expect("rows");
    let keys: Vec<&str> = rows.iter().map(|r| r["key"].as_str().unwrap()).collect();
    assert_eq!(
        keys,
        vec!["counter", "k00", "k01", "k05", "k06", "k10", "k19", "marker"],
        "{state}"
    );
    assert_eq!(state["missing"], json!([]));
    assert_eq!(rows[0]["value"], 25);
    assert_eq!(rows[5]["value"], "cas-won");

    // 1. The checkpoint alone does not hold it.
    {
        let store = HeedStore::open(&dir.join("store"), &super::apply::store_opts())
            .expect("open the killed store");
        let (ck_applied, ck_kv, ck_idx) = store
            .read(|r| {
                Ok((
                    r.applied_index()?,
                    r.count(Keyspace::Kv)?,
                    r.count(Keyspace::KvExpiry)?,
                ))
            })
            .expect("read the checkpoint");
        assert!(
            ck_applied < applied,
            "the checkpoint ({ck_applied}) already covered the workload ({applied}): \
             no durable point was supposed to happen"
        );
        assert_eq!(
            (ck_kv, ck_idx),
            (0, 0),
            "the checkpoint holds none of the KV writes, so the reopen must replay them"
        );
        store.close();
    }

    // 2. The reopen replays the WAL.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let f = RaftFacade::open(&build_ctx(&dir)).expect("reopen the killed node");
        assert!(
            f.health().applied >= applied,
            "every acknowledged entry is back: {} < {applied}",
            f.health().applied
        );
        let back = snapshot(&f).await;
        assert_eq!(
            back, state,
            "the replayed KV state is the acknowledged one, byte for byte"
        );
        // 20 puts − 11 deletes + the counter + the marker; those two expire.
        assert_eq!(
            f.kv_rows_physical(),
            (11, 2),
            "every row, and exactly the index"
        );

        let n = call(
            &f,
            json!([{"op":"incr","ns":"crash","key":"counter","delta":1,"ttlSeconds":60}]),
        )
        .await;
        assert_eq!(n[0]["value"], 26, "the counter continues after the replay");
        let max_before = rows
            .iter()
            .filter_map(|r| r["version"].as_u64())
            .max()
            .unwrap();
        assert!(
            n[0]["version"].as_u64().unwrap() > max_before,
            "and the version sequence never re-issues (I18)"
        );
        let m = call(
            &f,
            json!([{"op":"putIfAbsent","ns":"crash","key":"marker","value":0,"ttlSeconds":5}]),
        )
        .await;
        assert_eq!(m[0]["reason"], "exists", "the marker survived: {}", m[0]);
        f.shutdown().await;
    });
    let _ = std::fs::remove_dir_all(&dir);
}
