//! Rendezvous hashing (highest random weight), and the two things it decides:
//! which node leads a partition, and which node coordinates a group.
//!
//! ## Why rendezvous and not a ring
//!
//! Both give minimal disruption; a ring needs virtual nodes and a built
//! structure to get there, and rendezvous needs neither. Adding or removing a
//! node moves only the items whose argmax changed — about 1/N of them — and no
//! other item moves at all. The answer is a PURE FUNCTION of the live set and
//! the item, which is the property the whole design rests on: two facades with
//! the same view always name the same owner, without having agreed on anything
//! beyond the view itself.
//!
//! ## Why the hash is hand-rolled
//!
//! `std::collections::hash_map::DefaultHasher` is explicitly not stable across
//! Rust releases. A leadership map that reshuffled on a toolchain bump would be
//! a silent, unattributable rebalance storm — the worst kind of incident,
//! because nothing in the deployment changed. FNV-1a is byte-stable for ever
//! and splitmix64's finalizer gives it the avalanche FNV lacks (FNV alone
//! leaves the low bits of short, similar inputs correlated, which is exactly
//! what a partition index is). Neither needs a dependency, and
//! `protocols/queen-kafka/Cargo.toml` gains none for this.
//!
//! The scheme is pinned by a test-vector table below. It is not there to check
//! the arithmetic — it is there so that CHANGING the arithmetic fails a test
//! instead of moving every group in a running cluster.

/// The domain tag of partition leadership. It keeps a topic named like a group
/// from sharing a derivation with it — without it, a group `orders` and a
/// one-partition topic `orders` would always be owned by the same node, which
/// is a correlation nobody asked for and nobody could see.
pub const DOMAIN_PARTITION: u8 = 0x01;
/// The domain tag of group ownership.
pub const DOMAIN_GROUP: u8 = 0x02;
/// The domain tag of `transactional.id` ownership — **defined and unused in
/// v1** (M9, [`crate::txn`]).
///
/// Ownership of a `transactional.id` is a rendezvous over the live set exactly
/// as group ownership is, and it needs its own domain byte so that a topic, a
/// group and a transactional id spelled the same are not correlated onto one
/// node — the same reason [`DOMAIN_GROUP`] exists.
///
/// Nothing calls it, and that is the honest state of things rather than an
/// oversight: transactions are single-node because the STAGE cannot move
/// between facades, and no ownership rule fixes that — a producer sends
/// `Produce` to the partition leader and `EndTxn` to the coordinator, which in
/// a cluster are different processes. The constant is here so that the day the
/// stage is durable, the change is a module swap and not a protocol change; and
/// its rows are in the pinned table below, which is additive by construction
/// because the domain byte is the first byte of the hashed buffer.
pub const DOMAIN_TXN: u8 = 0x03;

/// FNV-1a, 64-bit. Offset basis and prime are the published constants.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// splitmix64's finalizer: the avalanche step. FNV's own diffusion is poor in
/// the low bits, and the low bits are precisely what distinguishes `orders/0`
/// from `orders/1`.
pub fn mix64(mut z: u64) -> u64 {
    z ^= z >> 30;
    z = z.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z ^= z >> 27;
    z = z.wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The weight of one node for one item.
///
/// The node id is written as ASCII DECIMAL rather than as little-endian bytes,
/// for one reason: the pinned table below is then readable, and there is no
/// endianness question to get wrong twice.
pub fn score(domain: u8, item: &[u8], node_id: i32) -> u64 {
    let mut buf = Vec::with_capacity(item.len() + 8);
    buf.push(domain);
    buf.push(0);
    buf.extend_from_slice(item);
    buf.push(0);
    buf.extend_from_slice(node_id.to_string().as_bytes());
    mix64(fnv1a64(&buf))
}

/// The node of `nodes` that owns `item`, or `None` when the set is empty.
///
/// Ties are broken by the LOWER node id. A tie is a 1-in-2^64 event and the
/// rule exists anyway, because "vanishingly unlikely" and "deterministic" are
/// different properties and only the second one is what two facades need.
pub fn winner(domain: u8, item: &[u8], nodes: &[i32]) -> Option<i32> {
    nodes
        .iter()
        .map(|id| (score(domain, item, *id), *id))
        // `max_by_key` on the pair would break ties by the HIGHER id, because
        // the id is the second key. Written out so the rule is the one stated.
        .reduce(|best, next| match next.0.cmp(&best.0) {
            std::cmp::Ordering::Greater => next,
            std::cmp::Ordering::Equal if next.1 < best.1 => next,
            _ => best,
        })
        .map(|(_, id)| id)
}

/// The item bytes of one partition: `<topic>\0<partition decimal>`.
pub fn partition_item(topic: &str, partition: i32) -> Vec<u8> {
    let mut item = Vec::with_capacity(topic.len() + 8);
    item.extend_from_slice(topic.as_bytes());
    item.push(0);
    item.extend_from_slice(partition.to_string().as_bytes());
    item
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// The published FNV-1a-64 test vectors. If this fails, the constants are
    /// wrong and every number below it is meaningless.
    #[test]
    fn fnv1a64_is_fnv1a64() {
        assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a64(b"foobar"), 0x85944171f73967e8);
    }

    /// splitmix64's finalizer, against the reference implementation's own
    /// output for the first two states of the generator.
    #[test]
    fn mix64_is_splitmix64s_finalizer() {
        assert_eq!(mix64(0), 0);
        assert_eq!(mix64(0x9e37_79b9_7f4a_7c15), 0xe220_a839_7b1d_cdaf);
        assert_eq!(mix64(0x3c6e_f372_fe94_f82a), 0x6e78_9e6a_a1b9_65f4);
    }

    /// THE pinned table. A change to the domain tags, the separators, the node
    /// id encoding, the hash or the finalizer fails here — which is the point:
    /// every one of those changes would silently move leadership and group
    /// ownership on a running cluster, and none of them would show up in a
    /// deployment diff.
    #[test]
    fn the_scheme_is_pinned() {
        for (domain, item, node, want) in [
            (
                DOMAIN_PARTITION,
                partition_item("orders", 0),
                1,
                0x0b1a301ad5704724u64,
            ),
            (
                DOMAIN_PARTITION,
                partition_item("orders", 0),
                2,
                0x563e299a5ff86bb6,
            ),
            (
                DOMAIN_PARTITION,
                partition_item("orders", 1),
                1,
                0x25b38eb2f1c21846,
            ),
            (
                DOMAIN_PARTITION,
                partition_item("orders", 7),
                3,
                0x86b98cd37f131fc7,
            ),
            (
                DOMAIN_GROUP,
                b"orders-consumer".to_vec(),
                1,
                0x3437e6ea20050dc2,
            ),
            (
                DOMAIN_GROUP,
                b"orders-consumer".to_vec(),
                2,
                0xff65307c19dd155e,
            ),
            (
                DOMAIN_GROUP,
                b"orders-consumer".to_vec(),
                3,
                0x8f3da208494794ac,
            ),
            (DOMAIN_GROUP, b"".to_vec(), 1, 0xd27a13771d92b307),
            // DOMAIN_TXN, pinned before anything routes on it: the day cluster
            // mode grows a transaction coordinator, this table is what stops
            // the scheme moving under it.
            (DOMAIN_TXN, b"tx-1".to_vec(), 1, 0x56f5e1427f0231ec),
            (DOMAIN_TXN, b"tx-1".to_vec(), 2, 0x7a6079306c934186),
            (
                DOMAIN_GROUP,
                b"a group with spaces".to_vec(),
                64,
                0x302d0d1e07a31b9f,
            ),
        ] {
            assert_eq!(
                score(domain, &item, node),
                want,
                "score(domain={domain:#x}, item={item:?}, node={node}) moved"
            );
        }

        // ...and the answers those scores compose into.
        let three = [1, 2, 3];
        assert_eq!(
            winner(DOMAIN_GROUP, b"orders-consumer", &three),
            Some(2),
            "the owner of orders-consumer moved"
        );
        assert_eq!(
            winner(DOMAIN_PARTITION, &partition_item("orders", 0), &three),
            Some(2)
        );
        assert_eq!(
            winner(DOMAIN_PARTITION, &partition_item("orders", 7), &three),
            Some(1)
        );
    }

    /// The domain tag is not decoration: a group, a topic and a transactional
    /// id that share a name must not share a derivation.
    #[test]
    fn the_domains_are_separate() {
        assert_ne!(
            score(DOMAIN_PARTITION, b"orders", 1),
            score(DOMAIN_GROUP, b"orders", 1)
        );
        assert_ne!(
            score(DOMAIN_TXN, b"orders", 1),
            score(DOMAIN_GROUP, b"orders", 1)
        );
        assert_ne!(
            score(DOMAIN_TXN, b"orders", 1),
            score(DOMAIN_PARTITION, b"orders", 1)
        );
    }

    /// An empty live set has no owner, and that is a real state — a node whose
    /// registry read has never succeeded. It must be `None` and not a panic or
    /// a node 0 that does not exist.
    #[test]
    fn an_empty_set_owns_nothing() {
        assert_eq!(winner(DOMAIN_GROUP, b"g", &[]), None);
        assert_eq!(winner(DOMAIN_GROUP, b"g", &[7]), Some(7));
    }

    /// The order the live set arrives in is not an input: two facades whose
    /// registry read came back in different orders must name the same owner.
    #[test]
    fn the_answer_does_not_depend_on_the_order_of_the_set() {
        for item in ["a", "orders-consumer", "svc.billing", "x"] {
            let up = winner(DOMAIN_GROUP, item.as_bytes(), &[1, 2, 3, 4]);
            let down = winner(DOMAIN_GROUP, item.as_bytes(), &[4, 3, 2, 1]);
            let mixed = winner(DOMAIN_GROUP, item.as_bytes(), &[3, 1, 4, 2]);
            assert_eq!(up, down, "{item}");
            assert_eq!(up, mixed, "{item}");
        }
    }

    /// Spread, over the shape this is actually for: 1024 partitions and three
    /// nodes. Not a statistical proof — a hash that fails this is broken in a
    /// way no cluster would survive, which is what the assertion is for.
    #[test]
    fn the_load_is_even_within_fifteen_percent() {
        let nodes = [1, 2, 3];
        let mut count: HashMap<i32, usize> = HashMap::new();
        for p in 0..1024 {
            let owner = winner(DOMAIN_PARTITION, &partition_item("orders", p), &nodes).unwrap();
            *count.entry(owner).or_default() += 1;
        }
        let even = 1024f64 / 3.0;
        for id in nodes {
            let got = *count.get(&id).unwrap_or(&0) as f64;
            assert!(
                (got - even).abs() / even <= 0.15,
                "node {id} leads {got} of 1024 partitions"
            );
        }
    }

    /// MINIMAL DISRUPTION, which is the whole reason for rendezvous: removing
    /// one node moves exactly the partitions that had it as leader, and not one
    /// other.
    #[test]
    fn losing_a_node_moves_only_what_that_node_had() {
        let before: Vec<i32> = (0..1024)
            .map(|p| winner(DOMAIN_PARTITION, &partition_item("orders", p), &[1, 2, 3]).unwrap())
            .collect();
        let after: Vec<i32> = (0..1024)
            .map(|p| winner(DOMAIN_PARTITION, &partition_item("orders", p), &[1, 3]).unwrap())
            .collect();
        for (p, (was, now)) in before.iter().zip(&after).enumerate() {
            if *was == 2 {
                assert_ne!(*now, 2, "partition {p} still leads on the dead node");
            } else {
                assert_eq!(*was, *now, "partition {p} moved for nothing");
            }
        }
        // ...and the same for a node JOINING, which is the same event seen from
        // the other side: only items that pick the newcomer move.
        let grown: Vec<i32> = (0..1024)
            .map(|p| {
                winner(
                    DOMAIN_PARTITION,
                    &partition_item("orders", p),
                    &[1, 2, 3, 4],
                )
                .unwrap()
            })
            .collect();
        let moved = before
            .iter()
            .zip(&grown)
            .filter(|(was, now)| was != now)
            .count();
        assert!(
            grown
                .iter()
                .zip(&before)
                .all(|(now, was)| now == was || *now == 4),
            "a partition moved between two nodes that both stayed"
        );
        // ~1/4 of them, and the bound is loose because this asserts the SHAPE
        // (only the newcomer's share moves), not the exact draw.
        assert!((150..400).contains(&moved), "{moved} of 1024 moved");
    }
}
