//! The dead letters' caps per queue (maintenance's `plan_dlq_caps`): what a
//! maintenance pass deletes from a store holding one queue's dead letters of
//! known sizes, oldest first.

use crate::rsm::effect::Effect;
use crate::rsm::maintenance::{self, DlqCaps};
use crate::rsm::store::keys::{self, Counter};
use crate::rsm::store::{HeedStore, Keyspace, Reads, Store, TypedWrites, Writes};

const T: &str = crate::config::DEFAULT_TENANT;

struct Dir(std::path::PathBuf);

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A dead letter's id: a UUIDv7 minted `i` ms after the first, so the queue's
/// rows come oldest first.
fn id(i: usize) -> [u8; 16] {
    let ms = 1_700_000_000_000u64 + i as u64;
    let mut id = [0u8; 16];
    id[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
    id[6] = 0x70;
    id[8..16].copy_from_slice(&(i as u64).to_be_bytes());
    id
}

/// A store holding queue `q`'s dead letters, `sizes[i]` bytes of value each,
/// and its dead-letter count.
fn store(tag: &str, sizes: &[usize]) -> (Dir, HeedStore) {
    let d = std::env::temp_dir().join(format!("queen-rsm-dlq-caps-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let store = HeedStore::open(&d.join("store"), &super::apply::store_opts()).unwrap();
    {
        let mut w = store.write().unwrap();
        for (i, n) in sizes.iter().enumerate() {
            w.put_raw(Keyspace::Dlq, &keys::dlq(T, "q", &id(i)), &vec![b'x'; *n])
                .unwrap();
        }
        w.set_counter(
            &keys::counter_queue_buf(T, "q", Counter::DlqCount),
            sizes.len() as i64,
        )
        .unwrap();
        w.commit().unwrap();
    }
    (Dir(d), store)
}

/// The ids one maintenance pass deletes under `caps`, and whether it says
/// more is due.
fn pass(store: &HeedStore, caps: DlqCaps) -> (Vec<[u8; 16]>, bool) {
    let cfg = maintenance::Config {
        dlq: caps,
        ..maintenance::Config::default()
    };
    let planned = store
        .read(|r| maintenance::plan(r, 1_800_000_000_000_000, &cfg))
        .unwrap();
    let ids = planned
        .effects
        .iter()
        .filter_map(|e| match e {
            Effect::DlqDelete { dlq_id, .. } => Some(*dlq_id),
            _ => None,
        })
        .collect();
    (ids, planned.more)
}

/// Every row's stored size (key and value, as maintenance counts them), oldest
/// first.
fn stored(store: &HeedStore) -> Vec<u64> {
    store
        .read(|r| {
            let mut out = Vec::new();
            r.scan_raw(
                Keyspace::Dlq,
                &[],
                &keys::dlq_prefix(T, "q"),
                usize::MAX,
                &mut |k, v| {
                    out.push((k.len() + v.len()) as u64);
                    true
                },
            )?;
            Ok(out)
        })
        .unwrap()
}

fn bytes_cap(max_bytes: u64) -> DlqCaps {
    DlqCaps {
        max_rows: 0,
        max_bytes,
        max_age_s: 0,
    }
}

/// The dead-letter budget files a queue's first failures with their bodies
/// and the next ones without: 64 oldest rows of 64 KiB, 3,000 newer of 100 B,
/// about 4.5 MB in all. Their oldest alone say 196 MB, past a 16 MiB cap; what
/// every dead letter holds bounds the estimate, and nothing goes.
#[test]
fn a_queue_whose_newer_dead_letters_lost_their_bodies_is_not_judged_by_its_oldest() {
    let mut sizes = vec![64 << 10; 64];
    sizes.extend(std::iter::repeat_n(100, 3_000));
    let (_d, s) = store("bounded", &sizes);
    let total: u64 = stored(&s).iter().sum();
    assert!(total < 16 << 20, "{total}");
    let (deleted, more) = pass(&s, bytes_cap(16 << 20));
    assert!(deleted.is_empty() && !more, "{} deleted", deleted.len());
}

/// Past the byte cap, the oldest go by their own size until the rest fits:
/// no more of them, and none of the small newer rows.
#[test]
fn past_the_byte_cap_the_oldest_go_by_their_own_size() {
    let mut sizes = vec![64 << 10; 64];
    sizes.extend(std::iter::repeat_n(100, 3_000));
    let (_d, s) = store("by-size", &sizes);
    let rows = stored(&s);
    let total: u64 = rows.iter().sum();
    let cap = 2u64 << 20;
    let (deleted, _) = pass(&s, bytes_cap(cap));
    let n = deleted.len();
    assert_eq!(
        deleted,
        (0..n).map(id).collect::<Vec<_>>(),
        "the oldest first"
    );
    let left: u64 = total - rows[..n].iter().sum::<u64>();
    assert!(left <= cap, "{left} left past the cap {cap}");
    // The bound counts each stored value's 8-byte checksum, which a scan
    // leaves out: the estimate is at most that much over.
    let slack = 8 * rows.len() as u64;
    assert!(
        left + rows[n - 1] + slack > cap,
        "one fewer would have fitted: {n} deleted"
    );
    assert!(n < 64, "only rows with a body went: {n}");
}

/// Rows of one size: the oldest past the cap go, at most 1,024 a pass, as
/// before the bound.
#[test]
fn dead_letters_of_one_size_past_the_byte_cap_go_oldest_first() {
    let (_d, s) = store("uniform", &[1_000; 3_000]);
    let (deleted, more) = pass(&s, bytes_cap(1 << 20));
    assert_eq!(deleted, (0..1_024).map(id).collect::<Vec<_>>());
    assert!(more, "more is due");
}

/// The row cap is not an estimate: the oldest past it go.
#[test]
fn past_the_row_cap_the_oldest_go() {
    let (_d, s) = store("rows", &[100; 3_000]);
    let (deleted, more) = pass(
        &s,
        DlqCaps {
            max_rows: 2_500,
            max_bytes: 0,
            max_age_s: 0,
        },
    );
    assert_eq!(deleted, (0..500).map(id).collect::<Vec<_>>());
    assert!(!more);
}
