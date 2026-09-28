//! Per-file queue-log retention through [`crate::rsm::maintenance::reclaim_qlogs`]
//! (2026-09-28): a sealed file is judged by the partitions that have records in
//! it, their watermarks read from the store, instead of by a map of every
//! partition on the node rebuilt on every pass.

use crate::rsm::qlog::set::QLogSet;
use crate::rsm::qlog::{QLogOptions, RecordInput};
use crate::rsm::store::rows::PartitionRow;
use crate::rsm::store::{HeedStore, Store, TypedWrites, Writes};

struct Dir(std::path::PathBuf);

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn dir(tag: &str) -> Dir {
    let d = std::env::temp_dir().join(format!("queen-rsm-retention-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    Dir(d)
}

fn write(set: &mut QLogSet, log: u64, seq: u64, pid: u64, base: u64) {
    let hashes = [seq as u8; 16];
    let payload = [pid as u8; 64];
    set.write_group_for_qid(
        log,
        &[RecordInput {
            seq,
            pid,
            base_offset: base,
            count: 1,
            created_at_us: 1_000 + seq as i64,
            txn: None,
            hashes: &hashes,
            payload: &payload,
        }],
    )
    .unwrap();
}

#[test]
fn reclaim_qlogs_judges_files_by_the_partition_rows_they_name() {
    let d = dir("rows");
    let store = HeedStore::open(&d.0.join("store"), &super::apply::store_opts()).unwrap();
    let mut set = QLogSet::new(d.0.join("qlog"), QLogOptions::testing(300));
    set.reopen_all().unwrap();
    set.set_recovery_floor(u64::MAX);

    // pids 1 and 2 are live; 3 expired (its txns watermark is past its
    // records); 4 has no row any more; 5 belongs to another queue, so its
    // records in this log are strays.
    {
        let mut w = store.write().unwrap();
        for (pid, queue, watermark) in [
            (1u64, "q", 0u64),
            (2, "q", 0),
            (3, "q", 1_000),
            (5, "other", 0),
        ] {
            let mut row = PartitionRow::new([pid as u8; 16], "t", queue, &format!("p{pid}"), 1);
            row.log_start = watermark;
            row.txns_start = watermark;
            w.put_partition(pid, &row).unwrap();
        }
        w.commit().unwrap();
    }
    let log = set.log_id_for(QLogSet::queue_id_of("t", "q"), 1);
    assert_ne!(
        log,
        set.log_id_for(QLogSet::queue_id_of("t", "other"), 5),
        "the stray partition routes to another log"
    );
    let mut seq = 0;
    for pid in [3u64, 4, 5, 1, 2] {
        for base in 0..4 {
            seq += 1;
            write(&mut set, log, seq, pid, base);
        }
    }
    set.sync().unwrap();

    let reader = set.reader();
    let mut changed = 0;
    for _ in 0..16 {
        changed += store
            .read(|r| crate::rsm::maintenance::reclaim_qlogs(r, &reader))
            .unwrap();
    }
    assert!(
        changed > 0,
        "the expired, missing and stray records were reclaimed"
    );

    let q = set.log(log).unwrap();
    let q = q.read().unwrap();
    for pid in [1u64, 2] {
        for base in 0..4 {
            assert!(
                q.read_payload(pid, base).unwrap().is_some(),
                "live pid {pid} offset {base} survives"
            );
        }
    }
    let active = q.files().last().unwrap().id;
    for pid in [3u64, 4, 5] {
        for base in 0..4 {
            if let Some(loc) = q.locate(pid, base) {
                assert_eq!(
                    loc.file_id, active,
                    "dead pid {pid} offset {base} is still in a sealed file"
                );
            }
        }
    }
}

/// What a retention pass costs on a store with a million partitions: the old
/// whole-map build (every partition row decoded and mapped, on every pass)
/// against the per-file pass, whose files name 400 partitions. Debug build, so
/// compare the two, not the absolute figures:
/// `cargo test --lib -- --ignored retention_pass_cost --nocapture`.
#[test]
#[ignore]
fn retention_pass_cost_at_a_million_partitions() {
    use crate::rsm::qlog::set::QLogReader;
    use crate::rsm::store::{keys, rows, Keyspace, Reads, StoreOpts};
    use std::collections::HashMap;

    const N: u64 = 1_000_000;
    let d = dir("cost");
    let opts = StoreOpts {
        map_bytes: Some(4 << 30),
        ..super::apply::store_opts()
    };
    let store = HeedStore::open(&d.0.join("store"), &opts).unwrap();
    {
        let mut w = store.write().unwrap();
        for pid in 1..=N {
            let row = PartitionRow::new([0; 16], "t", "q", &format!("p{pid}"), 1);
            w.put_partition(pid, &row).unwrap();
        }
        w.commit().unwrap();
    }
    let mut set = QLogSet::new(d.0.join("qlog"), QLogOptions::testing(4096));
    set.reopen_all().unwrap();
    set.set_recovery_floor(u64::MAX);
    let log = set.log_id_for(QLogSet::queue_id_of("t", "q"), 1);
    for i in 0..400u64 {
        write(&mut set, log, i + 1, 1 + i * 2_500, 0);
    }
    set.sync().unwrap();
    let reader = set.reader();

    let t = std::time::Instant::now();
    let mut maps: HashMap<u64, HashMap<u64, u64>> = HashMap::new();
    store
        .read(|r| {
            r.scan_raw(
                Keyspace::Partitions,
                &[],
                &[],
                usize::MAX,
                &mut |key, value| {
                    if let (Some(pid), Ok(row)) = (keys::pid_of(key), rows::partition_decode(value))
                    {
                        let queue_id = QLogReader::queue_id_of(&row.tenant, &row.queue);
                        maps.entry(reader.log_id_for(queue_id, pid))
                            .or_default()
                            .insert(pid, row.txns_start);
                    }
                    true
                },
            )
        })
        .unwrap();
    let whole_map = t.elapsed();
    assert_eq!(maps.values().map(HashMap::len).sum::<usize>(), N as usize);

    let t = std::time::Instant::now();
    store
        .read(|r| crate::rsm::maintenance::reclaim_qlogs(r, &reader))
        .unwrap();
    let per_file = t.elapsed();
    eprintln!(
        "retention pass, {N} partitions, files naming 400: whole-map build {whole_map:?}, per-file pass {per_file:?}"
    );
    assert!(per_file < whole_map);
}
