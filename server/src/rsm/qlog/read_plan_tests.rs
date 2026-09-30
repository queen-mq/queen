//! Lock lifetime and immutable-file pinning of the pop payload read.
use super::*;
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
thread_local! { static READ_PROBE: RefCell<Option<Box<dyn Fn()>>> = RefCell::new(None); }
pub(super) fn probe_payload_read() {
    READ_PROBE.with(|p| {
        if let Some(f) = p.borrow().as_ref() {
            f();
        }
    });
}
struct ProbeGuard;
impl Drop for ProbeGuard {
    fn drop(&mut self) {
        READ_PROBE.with(|p| p.borrow_mut().take());
    }
}
struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let p = std::env::temp_dir().join(format!(
            "queen-qlog-read-plan-{tag}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn the_pop_reader_releases_the_log_lock_before_reading_and_decoding() {
    for compressed in [false, true] {
        let dir = Dir::new("lock");
        let mut logs = set::QLogSet::new(dir.0.clone(), QLogOptions::testing(1 << 20));
        logs.set_lanes(1);
        logs.set_shards(0);
        let raw = vec![42; 65536];
        let payload = if compressed {
            zstd::bulk::compress(&raw, 1).unwrap()
        } else {
            raw.clone()
        };
        let rec = RecordInput {
            seq: 1,
            pid: 1,
            base_offset: 0,
            count: 1,
            created_at_us: 123,
            txn: None,
            hashes: &[3; 16],
            payload: &payload,
        };
        let wr = if compressed {
            WriteRecord::Zstd(rec)
        } else {
            WriteRecord::Msg(rec)
        };
        logs.write_mixed_for_qid(42, &[wr]).unwrap();
        let reader = logs.reader();
        let log = logs.log(42).unwrap();
        let probes = Arc::new(AtomicUsize::new(0));
        let seen = probes.clone();
        READ_PROBE.with(|p| {
            *p.borrow_mut() = Some(Box::new(move || {
                let _writer = log
                    .try_write()
                    .expect("payload I/O must not hold the queue log read lock");
                seen.fetch_add(1, Ordering::Relaxed);
            }))
        });
        let _guard = ProbeGuard;
        let got = reader.read_owned(42, 1, 0).unwrap().unwrap();
        assert_eq!(probes.load(Ordering::Relaxed), 1);
        assert_eq!(got.payload, raw);
        assert_eq!(got.hashes, vec![3; 16]);
        assert_eq!((got.pid, got.count, got.created_at_us), (1, 1, 123));
        assert!(reader.read_owned(42, 99, 0).unwrap().is_none());
    }
}

fn mixed_log(dir: &Path) -> QLog {
    let (mut q, _) = QLog::open(dir, 1, QLogOptions::testing(300)).unwrap();
    for i in 0..20u64 {
        q.append_group(&[RecordInput {
            seq: i + 1,
            pid: 10 + i % 2,
            base_offset: i / 2,
            count: 1,
            created_at_us: i as i64,
            txn: None,
            hashes: &[3; 16],
            payload: &vec![i as u8; 96],
        }])
        .unwrap();
    }
    assert!(q.file_count() > 2);
    q
}

#[test]
fn a_payload_plan_survives_retention_unlinking_its_sealed_file() {
    let dir = Dir::new("unlink");
    let mut q = mixed_log(&dir.0);
    let plan = q.owned_read_plan(10, 0).unwrap().unwrap();
    q.unlink_dead_files(|m| m.sealed).unwrap();
    assert!(q.read_owned(10, 0).unwrap().is_none());
    assert_eq!(plan.finish().unwrap().payload, vec![0; 96]);
}

#[test]
fn a_payload_plan_survives_compaction_replacing_its_inode() {
    let dir = Dir::new("compact");
    let mut q = mixed_log(&dir.0);
    let plan = q.owned_read_plan(10, 0).unwrap().unwrap();
    let starts = std::collections::HashMap::from([(10, u64::MAX), (11, 0)]);
    loop {
        if !q.unlink_below_txns_bounded(&starts, 1).unwrap().more {
            break;
        }
    }
    assert!(q.read_owned(10, 0).unwrap().is_none());
    assert!(q.read_owned(11, 0).unwrap().is_some());
    assert_eq!(plan.finish().unwrap().payload, vec![0; 96]);
}

#[test]
fn a_payload_plan_still_checks_the_record_checksum() {
    let dir = Dir::new("checksum");
    let q = mixed_log(&dir.0);
    let plan = q.owned_read_plan(10, 0).unwrap().unwrap();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(file_path(&q.dir, plan.loc.file_id))
        .unwrap();
    let offset = plan.loc.offset + u64::from(plan.loc.len) - 1;
    let mut byte = [0];
    plan.file.read_exact_at(&mut byte, offset).unwrap();
    byte[0] ^= 0x80;
    file.write_all_at(&byte, offset).unwrap();
    assert!(
        plan.finish().is_err(),
        "a pinned fd must not bypass record verification"
    );
}
