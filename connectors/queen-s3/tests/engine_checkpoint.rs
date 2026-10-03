//! The position-cache codec (plan §4.5(1)), including the one number the plan
//! asserts without measuring: what a million-partition checkpoint costs.

use queen_s3::checkpoint::{decode, encode, positions_from, should_checkpoint};
use queen_s3::types::{Checkpoint, Micros, Position};

fn cp(k: u64, positions: Vec<Position>) -> Checkpoint {
    Checkpoint {
        k,
        t_end: Micros(1_788_480_000_000_000),
        positions,
    }
}

/// A uuid-shaped id with real entropy: what the broker's partition ids are,
/// and the worst case for the compressor.
fn uuid_from(x: u64, y: u64) -> String {
    format!(
        "{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
        x >> 32,
        (x >> 16) & 0xffff,
        x & 0xfff,
        y >> 52,
        y & 0xffff_ffff_ffff
    )
}

#[test]
fn round_trips_through_the_object_bytes() {
    let c = cp(
        1842,
        (0..1_000)
            .map(|i| match i % 2 {
                0 => Position::with_id(
                    format!("cust-{i:06}"),
                    i as i64 * 7 - 1,
                    uuid_from(i as u64, !(i as u64)),
                ),
                // A position restored from a 1.5.0 checkpoint has no id.
                _ => Position::new(format!("cust-{i:06}"), i as i64 * 7 - 1),
            })
            .collect(),
    );
    let bytes = encode(&c);
    let back = decode(&bytes).unwrap();
    assert_eq!(back, c);
}

#[test]
fn the_object_is_a_function_of_the_positions_only() {
    let mut a: Vec<Position> = (0..500)
        .map(|i| Position::new(format!("p{i:04}"), i as i64))
        .collect();
    let mut b = a.clone();
    // Any permutation of the same map must encode identically: the engine hands
    // over a BTreeMap today, but a HashMap tomorrow must not change the object.
    b.reverse();
    b.swap(3, 400);
    assert_eq!(encode(&cp(9, a.clone())), encode(&cp(9, b)));
    assert_eq!(encode(&cp(9, a.clone())), encode(&cp(9, a.clone())));
    // ...and a different position is a different object.
    a[7].offset += 1;
    assert_ne!(
        encode(&cp(9, a.clone())),
        encode(&cp(
            9,
            positions_from((0..500).map(|i| Position::new(format!("p{i:04}"), i as i64)))
        ))
    );
    // ...and so is the same offset under another incarnation.
    let mut c = a.clone();
    c[7].id = Some("another-incarnation".into());
    assert_ne!(encode(&cp(9, a)), encode(&cp(9, c)));
}

#[test]
fn names_that_need_escaping_survive() {
    let odd = vec![
        Position::new("a/b space", 1),
        Position::new("emoji 🦆", 2),
        Position::new("quote\"back\\slash", 3),
        Position::with_id("newline\nin\tname", 4, "id \"with\" quotes"),
        Position::new("", 5),
    ];
    let back = decode(&encode(&cp(1, odd.clone()))).unwrap();
    let mut want = odd;
    want.sort_by(|a, b| a.partition.cmp(&b.partition));
    assert_eq!(back.positions, want);
}

#[test]
fn a_million_entries_fit_in_a_sensible_object() {
    // Plan §4.5 claims "1M entries ~ 10 MB compressed" — for the 1.5.0 map,
    // which had no partition ids. Measure it rather than repeat it: this is the
    // structure that scales with cardinality, and it is written every
    // CHECKPOINT_EVERY windows. Every position here carries a uuid-shaped id of
    // random bits, which no compressor shrinks below its entropy.
    const N: usize = 1_000_000;
    // Sequential names (what an entity-partitioned queue looks like) but
    // high-entropy offsets: a modular pattern would compress to nothing and
    // flatter the number.
    let mut seed = 0x2545_F491_4F6C_DD1Du64;
    let mut next = || {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        seed
    };
    let positions: Vec<Position> = (0..N)
        .map(|i| {
            let offset = (next() >> 24) as i64 % 10_000_000;
            let id = uuid_from(next(), next());
            Position::with_id(format!("cust-{i:09}"), offset, id)
        })
        .collect();
    let c = cp(1842, positions);

    let bytes = encode(&c);
    let json = serde_json::to_vec(&c).unwrap();
    eprintln!(
        "1M-entry checkpoint: {} B JSON, {} B zstd ({:.1}x, {:.2} B/entry)",
        json.len(),
        bytes.len(),
        json.len() as f64 / bytes.len() as f64,
        bytes.len() as f64 / N as f64
    );
    // The id-less 1.5.0 map stayed under 24 MiB here. Each id is 120 random bits
    // in this test (122 in a real uuid), and no compressor stores them in fewer
    // than 15 bytes, so the map with ids gets 15 MiB more room — and no more:
    // anything above that would be the encoding's fault, not the ids'.
    assert!(
        bytes.len() < 39 * 1024 * 1024,
        "a 1M-partition checkpoint is {} bytes",
        bytes.len()
    );
    assert!(
        bytes.len() > N * 15,
        "{} bytes for a million ids of 120 random bits each: the ids are not in the object",
        bytes.len()
    );

    let back = decode(&bytes).unwrap();
    assert_eq!(back.positions.len(), N);
    assert_eq!(back.k, 1842);
    assert_eq!(
        back.positions[0].partition, "cust-000000000",
        "sorted by name"
    );
    assert_eq!(back, c);
}

#[test]
fn positions_from_accepts_the_maps_the_engine_holds() {
    use std::collections::{BTreeMap, HashMap};

    let mut hash: HashMap<std::sync::Arc<str>, i64> = HashMap::new();
    let mut tree: BTreeMap<String, i64> = BTreeMap::new();
    for i in 0..200 {
        hash.insert(format!("p{i:03}").into(), i as i64);
        tree.insert(format!("p{i:03}"), i as i64);
    }
    let from_hash = positions_from(hash.iter().map(|(k, v)| Position::new(k.as_ref(), *v)));
    let from_tree = positions_from(tree.iter().map(|(k, v)| Position::new(k.as_str(), *v)));
    assert_eq!(from_hash, from_tree);
    assert!(from_hash
        .windows(2)
        .all(|w| w[0].partition < w[1].partition));
}

#[test]
fn the_cadence_is_every_n_windows() {
    let ks: Vec<u64> = (1..=41).filter(|k| should_checkpoint(*k, 20)).collect();
    assert_eq!(ks, vec![20, 40]);
    assert!((1..100).all(|k| !should_checkpoint(k, 0)), "0 disables it");
}

#[test]
fn a_corrupt_object_is_an_error_not_a_panic() {
    let good = encode(&cp(1, vec![Position::new("p", 1)]));
    assert!(decode(&good[..good.len() / 2]).is_err(), "truncated");
    assert!(decode(b"").is_err(), "empty");
    assert!(decode(b"\x28\xb5\x2f\xfd not really").is_err(), "bad frame");
    let mut flipped = good.clone();
    let n = flipped.len();
    flipped[n - 1] ^= 0xff;
    // Either the frame check or the JSON parse rejects it; neither may panic.
    let _ = decode(&flipped);
}
