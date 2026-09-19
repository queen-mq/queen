//! PERF-F: the bucket count is a node-local knob (`QUEEN_RAFT_BUCKETS`), pinned
//! in the tree's `MANIFEST` for the life of the data dir, and a name FOLDS into
//! it (`logical % nbuckets`). These run open / append / read / roll / seal /
//! `.qidx` / recover / GC at 1, 16 and 256 buckets, prove the fold matches a
//! direct reduction, and prove a reopen with a different count is refused.

use super::super::*;
use super::*;

/// The three counts the PERF-F matrix runs.
const COUNTS: [usize; 3] = [1, 16, 256];

fn open_at(dir: &TmpDir, segment_bytes: u64, nbuckets: usize) -> Segments {
    let (s, rec) = Segments::open(
        &dir.seg(),
        Options::testing_buckets(segment_bytes, nbuckets),
        &[],
    )
    .expect("open a fresh tree at the chosen bucket count");
    assert_eq!(rec, Recovery::default(), "a fresh tree recovers nothing");
    s
}

/// How many `bNNN` directories the tree has on disk.
fn count_bucket_dirs(dir: &TmpDir) -> usize {
    std::fs::read_dir(dir.seg())
        .expect("read seg dir")
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|n| {
                    n.len() == 4 && n.starts_with('b') && n[1..].bytes().all(|c| c.is_ascii_digit())
                })
                .unwrap_or(false)
        })
        .count()
}

/// The bucket count the manifest records, if any.
fn manifest_says(dir: &TmpDir) -> Option<usize> {
    let s = std::fs::read_to_string(dir.seg().join("MANIFEST")).ok()?;
    s.lines().find_map(|l| {
        l.trim()
            .strip_prefix("buckets=")
            .and_then(|v| v.trim().parse().ok())
    })
}

#[test]
fn open_pins_the_count_in_the_manifest_and_makes_that_many_dirs() {
    for n in COUNTS {
        let d = TmpDir::new(&format!("open-{n}"));
        let s = open_at(&d, 4096, n);
        assert_eq!(s.nbuckets(), n, "the tree keeps the requested count");
        assert_eq!(s.files().len(), n, "one active file per bucket at n={n}");
        assert_eq!(count_bucket_dirs(&d), n, "exactly n bucket dirs on disk");
        assert_eq!(
            manifest_says(&d),
            Some(n),
            "the count is pinned in the manifest"
        );
    }
}

#[test]
fn a_name_folds_into_logical_mod_count_and_reads_back() {
    // The planner emits a logical bucket in 0..256; the node folds it into its
    // own count. Every legal count divides 256, so the folded bucket equals a
    // direct reduction of the name hash — "bucket_of = hash mod that count".
    for n in COUNTS {
        let d = TmpDir::new(&format!("fold-{n}"));
        let mut s = open_at(&d, 1 << 20, n);
        for logical in [0u16, 1, 15, 16, 17, 200, 255] {
            let pid = 1000 + logical as u64;
            let pos = push(&mut s, logical, pid, 0, 2, 128);
            assert_eq!(
                pos.bucket as usize,
                logical as usize % n,
                "logical {logical} folds to {} at n={n}",
                logical as usize % n
            );
            let f = s.read(pos).expect("read the folded frame back");
            assert_eq!((f.pid, f.base_offset, f.count), (pid, 0, 2));
            assert_eq!(f.blob, blob(pid, 128), "the payload survives the fold");
        }
    }
}

#[test]
fn reopening_with_a_different_count_is_refused() {
    let d = TmpDir::new("mismatch");
    drop(open_at(&d, 4096, 16));
    // The same count reopens.
    let (_s, _r) = Segments::open(&d.seg(), Options::testing_buckets(4096, 16), &[])
        .expect("the same count reopens");
    // A different count is refused before a single file is touched: a data dir
    // keeps its count for life (changing it would misfile every old position).
    let e = Segments::open(&d.seg(), Options::testing_buckets(4096, 32), &[])
        .expect_err("a different count is refused");
    assert!(matches!(e, SegError::Refused(_)), "{e}");
}

#[test]
fn append_roll_seal_qidx_recover_and_gc_at_each_count() {
    for n in COUNTS {
        let d = TmpDir::new(&format!("life-{n}"));
        // Small files so the fill rolls several times; logical bucket 0 folds to
        // local 0 at every count, so we know exactly where the frames land.
        let mut s = open_at(&d, 4096, n);
        let mut frames = Vec::new();
        let mut base = 0u64;
        for _ in 0..40u64 {
            frames.push(push(&mut s, 0, 7, base, 2, 300));
            base += 2;
        }
        s.durable_point().expect("durable point");

        // The fill sealed files, each with a rebuilt-able `.qidx` on disk.
        let states = s.file_states();
        let sealed: Vec<u32> = states
            .iter()
            .filter(|f| f.bucket == 0 && f.sealed)
            .map(|f| f.file_id)
            .collect();
        assert!(
            !sealed.is_empty(),
            "the fill sealed at least one file at n={n}"
        );
        for id in &sealed {
            assert!(
                qidx_file(&d, 0, *id).exists(),
                "sealed f{id} has a .qidx at n={n}"
            );
        }
        for p in &frames {
            assert!(s.read(*p).is_ok(), "a frame reads before reopen at n={n}");
        }

        // Reopen against the recorded state: clean recovery, count unchanged,
        // every payload still readable.
        let state = s.file_states();
        drop(s);
        let (mut s, rep) = Segments::open(&d.seg(), Options::testing_buckets(4096, n), &state)
            .expect("reopen at the same count");
        assert!(rep.deleted.is_empty(), "nothing leftover at n={n}: {rep:?}");
        assert_eq!(s.nbuckets(), n);
        for p in &frames {
            assert!(s.read(*p).is_ok(), "{p:?} survived the reopen at n={n}");
        }

        // GC: release every frame, and the first sealed file dies and unlinks.
        for p in &frames {
            s.release(*p, Release::Both);
        }
        s.durable_point().expect("durable point");
        let first = sealed[0];
        assert!(s.file_meta(0, first).expect("meta").is_dead(), "n={n}");
        assert!(s.gc_candidates(usize::MAX).contains(&(0, first)), "n={n}");
        assert!(s.unlink(0, first).expect("unlink"), "n={n}");
        assert!(
            !seg_file(&d, 0, first).exists(),
            "the payload is gone at n={n}"
        );
    }
}

#[test]
fn a_legacy_tree_without_a_manifest_is_adopted_as_256_only() {
    // A data dir from before PERF-F has 256 bucket dirs and no MANIFEST. It is
    // the legacy 256 format: reopened at 256 it is adopted (and its manifest
    // written), reopened at any other count it is refused rather than re-folded.
    let d = TmpDir::new("legacy");
    drop(open_at(&d, 4096, 256));
    std::fs::remove_file(d.seg().join("MANIFEST")).expect("remove the manifest");

    let (_s, _r) = Segments::open(&d.seg(), Options::testing_buckets(4096, 256), &[])
        .expect("a legacy tree is adopted as 256");
    assert_eq!(manifest_says(&d), Some(256), "and its manifest is written");

    std::fs::remove_file(d.seg().join("MANIFEST")).expect("remove the manifest again");
    let e = Segments::open(&d.seg(), Options::testing_buckets(4096, 16), &[])
        .expect_err("a legacy tree at a smaller count is refused");
    assert!(matches!(e, SegError::Refused(_)), "{e}");
}

#[test]
fn the_bucket_count_is_clamped_to_a_power_of_two_that_divides_256() {
    // The fold `(h % 256) % n == h % n` needs `n | 256` — a power of two in
    // 1..=256 — so a requested count is rounded down to one and the fold stays
    // exact.
    assert_eq!(clamp_buckets(0), 1);
    assert_eq!(clamp_buckets(1), 1);
    assert_eq!(clamp_buckets(3), 2);
    assert_eq!(clamp_buckets(16), 16);
    assert_eq!(clamp_buckets(17), 16);
    assert_eq!(clamp_buckets(255), 128);
    assert_eq!(clamp_buckets(256), 256);
    assert_eq!(clamp_buckets(100_000), 256);

    for n in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        for h in [0u64, 1, 200, 255, 256, 257, 4095, u32::MAX as u64] {
            let logical = (h % LOGICAL_BUCKETS as u64) as usize;
            assert_eq!(
                logical % n,
                (h % n as u64) as usize,
                "the fold is exact at n={n}, h={h}"
            );
        }
    }
}
