//! The openraft replicator hands committed entries to the apply thread ahead
//! of openraft's own apply call (`replicator/raft/state_machine.rs`, the
//! feeder), and must stay honest while it does.
//!
//! What this file proves, on a single voter over the real apply thread with
//! proposals in flight eight at a time (the batcher's pipeline):
//!
//! - **openraft never runs ahead of the store**
//!   ([`pipelined_proposals_keep_openraft_behind_the_store`]): a sampler reads
//!   openraft's `last_applied`, then the store's applied index, the whole run;
//!   the first is never above the second. The role's I13 check, a leader's
//!   linearizable read and the purge driver all read openraft's value.
//! - **an answer means applied**: every proposal returns only once its entry
//!   is applied, at the index its submission order gave it.
//! - **same entries, same state**: the replicated state equals a run that
//!   proposed the same entries one at a time.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::rsm::apply::{self, state_digest, StateDigest, SystemClock};
use crate::rsm::entry::encode_entry;
use crate::rsm::replicator::local::{NoWaker, OpenConfig};
use crate::rsm::replicator::log::{Fsync, LogOptions};
use crate::rsm::replicator::raft::RaftReplicator;
use crate::rsm::replicator::Replicator;
use crate::rsm::store::{HeedStore, Store};

use super::apply::{cfg, seg_opts, store_opts, Workload};

static SEQ: AtomicU64 = AtomicU64::new(0);

const SEED: u64 = 0x0_0A0F_7000_00A4;

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-raft-ahead-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn repl_config(dir: &Path) -> OpenConfig {
    OpenConfig {
        node_id: 1,
        log_dir: dir.join("log"),
        log_opts: LogOptions {
            segment_bytes: 64 << 10,
            fsync: Fsync::Off,
        },
        seg_root: dir.join("seg"),
        seg_opts: seg_opts(),
        apply_cfg: apply::ApplyConfig {
            qlog: true,
            ..cfg()
        },
        // Small, so the feeder also meets a full apply channel.
        apply_channel_capacity: 4,
        replay_deadline: Duration::from_secs(30),
        writer_pipeline: false,
    }
}

fn open_raft(dir: &Path) -> RaftReplicator<HeedStore> {
    let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
    RaftReplicator::open(
        store,
        repl_config(dir),
        Arc::new(NoWaker),
        Arc::new(SystemClock),
    )
    .expect("open the openraft replicator")
}

fn workload(seed: u64, n: u64) -> Vec<Bytes> {
    let mut w = Workload::new(seed);
    (0..n)
        .map(|_| Bytes::from(encode_entry(&w.next().entry).expect("encode")))
        .collect()
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

/// Every replicated keyspace but `meta`, which also holds node-local
/// positions (the queue logs' recorded tails, the durable points) whose
/// timing differs between two runs of the same entries.
fn replicated(d: &StateDigest) -> Vec<(&'static str, u128, u64)> {
    d.per_keyspace
        .iter()
        .filter(|(name, _, _)| *name != "meta")
        .cloned()
        .collect()
}

fn digest_and_close(store: Arc<HeedStore>) -> StateDigest {
    let store = Arc::try_unwrap(store).unwrap_or_else(|_| panic!("the store is still shared"));
    let d = store
        .read(|r| Ok(state_digest(r).expect("digest")))
        .expect("read");
    store.close();
    d
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelined_proposals_keep_openraft_behind_the_store() {
    const N: u64 = 480;
    const IN_FLIGHT: usize = 8;
    let entries = workload(SEED, N);

    let sequential = {
        let dir = scratch("one-at-a-time");
        let repl = open_raft(&dir);
        for e in entries.clone() {
            repl.propose(e, deadline()).await.expect("propose");
        }
        let (_s, store) = repl.shutdown().expect("shutdown");
        let d = digest_and_close(store);
        let _ = std::fs::remove_dir_all(&dir);
        d
    };

    let dir = scratch("pipelined");
    let repl = Arc::new(open_raft(&dir));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let repl = repl.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut samples = 0u64;
            while !stop.load(Ordering::Acquire) {
                // openraft's first: the store's only grows after it.
                let theirs = repl.openraft_applied_for_test();
                let ours = repl.applied_index();
                assert!(
                    theirs <= ours,
                    "openraft reports entry {theirs} applied, the store has applied {ours}"
                );
                samples += 1;
                std::thread::sleep(Duration::from_micros(50));
            }
            samples
        })
    };

    // The leader's blank entry of its term is applied before open returns.
    let mut last = repl.applied_index();
    for chunk in entries.chunks(IN_FLIGHT) {
        let mut calls: Vec<_> = chunk
            .iter()
            .map(|e| Box::pin(repl.propose(e.clone(), deadline())))
            .collect();
        // One poll each, in order: a proposal takes its place in the log at
        // its first poll (the batcher's contract), then all are in flight.
        for c in calls.iter_mut() {
            std::future::poll_fn(|cx| {
                let _ = std::future::Future::poll(c.as_mut(), cx);
                std::task::Poll::Ready(())
            })
            .await;
        }
        let answers = tokio::time::timeout(
            Duration::from_secs(60),
            futures_util::future::join_all(calls),
        )
        .await
        .expect("the proposals answered within 60 s");
        for at in answers {
            let at = at.expect("propose");
            assert_eq!(at.index, last + 1, "indexes follow the submission order");
            assert!(
                at.index <= repl.applied_index(),
                "answered before its entry was applied"
            );
            last = at.index;
        }
    }
    stop.store(true, Ordering::Release);
    let samples = sampler.join().expect("the sampler found openraft ahead");
    assert!(samples > 0);

    let repl = Arc::try_unwrap(repl).unwrap_or_else(|_| panic!("the replicator is still shared"));
    let (_s, store) = repl.shutdown().expect("shutdown");
    let pipelined = digest_and_close(store);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        replicated(&pipelined),
        replicated(&sequential),
        "pipelined proposals built a different state than one at a time"
    );
}
