//! checkpoint — the position cache the sink parks in the bucket (plan §4.5(1)).
//!
//! `_queen/<esc queue>/checkpoint/<k>.json.zst` holds, for every partition the
//! engine tracks, the next offset the window *after* `k` would read from. It is
//! a **cache and never the commit truth**: a stale entry costs re-read bytes
//! that the `ts < T_{k-1}` filter throws away (plan §4.5), a missing one costs a
//! probe-seek ([`crate::seek`]) or a read from `logStart`. Nothing here can lose
//! or duplicate a record — only make a restart slower.
//!
//! Two properties the format has to have, and the tests pin both:
//!
//! * **Deterministic bytes.** Positions are sorted by partition name before
//!   serialisation and the zstd level is a constant, so the same position map
//!   encodes to the same object every time. Nothing else in the sink depends on
//!   that, but a checkpoint that changed bytes on every write would make an
//!   object-store diff useless and would hide a real change.
//! * **Small at a million partitions.** The map is the one structure that scales
//!   with cardinality (plan §2), so it is JSON — one `["name",offset]` pair per
//!   partition — compressed. A 1M-entry map lands around 10 MB (plan §4.5); the
//!   `checkpoint_1m_entries_size` test measures it rather than trusting the
//!   estimate.

use std::io::Read;

use crate::types::Checkpoint;

/// The zstd level every checkpoint object is written at.
///
/// Fixed on purpose: the level is part of the bytes, so a level chosen from a
/// config value would make two sinks with the same positions write two different
/// objects. 3 is the repository's default level everywhere else (the segment
/// blobs, [`crate::writer`]).
pub const ZSTD_LEVEL: i32 = 3;

/// The most a single checkpoint object is allowed to inflate to.
///
/// A checkpoint is read from the sink's own bucket, so this is a guard against
/// corruption and against a truncated/again-compressed object, not against an
/// adversary. 1M partitions decode to roughly 30 MB of JSON, so 512 MiB leaves
/// two orders of magnitude of headroom.
pub const MAX_DECODED_BYTES: usize = 512 * 1024 * 1024;

/// Serialise a checkpoint to its object bytes: JSON with the positions sorted by
/// partition name, then a single zstd frame at [`ZSTD_LEVEL`].
///
/// Sorting happens here rather than being demanded of the caller so that the
/// determinism property cannot be broken by a caller that iterates a `HashMap`.
/// Both compress and serialise are infallible for this type — `Checkpoint` has
/// no map keys that can fail and the sink writes into memory — so the signature
/// carries no error.
pub fn encode(cp: &Checkpoint) -> Vec<u8> {
    let mut norm = Checkpoint {
        k: cp.k,
        t_end: cp.t_end,
        positions: cp.positions.clone(),
    };
    // Stable sort: with duplicate names (a caller bug — a position map cannot
    // hold two entries for one partition) the input order still decides, so the
    // encoding stays a function of the input.
    norm.positions.sort_by(|a, b| a.0.cmp(&b.0));
    let json =
        serde_json::to_vec(&norm).expect("Checkpoint is a plain struct and always serialises");
    zstd::stream::encode_all(&json[..], ZSTD_LEVEL)
        .expect("zstd of an in-memory buffer cannot fail")
}

/// Parse the bytes of a checkpoint object.
///
/// Every failure is a `String`, because every failure has the same handling at
/// the call site: log it once, ignore the checkpoint, and fall back to the other
/// two position sources of plan §4.5. A corrupt checkpoint is never fatal.
pub fn decode(bytes: &[u8]) -> Result<Checkpoint, String> {
    let dec = zstd::stream::Decoder::new(bytes)
        .map_err(|e| format!("checkpoint: not a zstd frame: {e}"))?;
    let mut json = Vec::new();
    // `+ 1` so that hitting the cap exactly is distinguishable from a document
    // that merely happens to be MAX_DECODED_BYTES long.
    dec.take(MAX_DECODED_BYTES as u64 + 1)
        .read_to_end(&mut json)
        .map_err(|e| format!("checkpoint: zstd decode: {e}"))?;
    if json.len() > MAX_DECODED_BYTES {
        return Err(format!(
            "checkpoint: decodes to more than {MAX_DECODED_BYTES} bytes; refusing"
        ));
    }
    serde_json::from_slice(&json).map_err(|e| format!("checkpoint: bad JSON: {e}"))
}

/// Whether window `k` is a checkpoint window (plan §4.5: one every
/// `QUEEN_S3_CHECKPOINT_EVERY` windows).
///
/// `every == 0` disables checkpointing entirely, which is a legitimate
/// configuration for a queue small enough that a full re-read is free.
pub fn should_checkpoint(k: u64, every: u64) -> bool {
    every != 0 && k.is_multiple_of(every)
}

/// Build the `positions` vector of a [`Checkpoint`] from any iterator of
/// `(name, next offset)` — a `BTreeMap`, a `HashMap`, a slice — sorted by name
/// so [`encode`] has nothing left to do.
pub fn positions_from<I, S>(entries: I) -> Vec<(String, i64)>
where
    I: IntoIterator<Item = (S, i64)>,
    S: AsRef<str>,
{
    let mut out: Vec<(String, i64)> = entries
        .into_iter()
        .map(|(name, offset)| (name.as_ref().to_string(), offset))
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Micros;

    fn cp(k: u64, positions: Vec<(&str, i64)>) -> Checkpoint {
        Checkpoint {
            k,
            t_end: Micros(1_788_480_000_000_000),
            positions: positions
                .into_iter()
                .map(|(n, o)| (n.to_string(), o))
                .collect(),
        }
    }

    #[test]
    fn round_trip() {
        let c = cp(
            42,
            vec![("cust-0001", 17), ("cust-0002", 0), ("a/b space", -1)],
        );
        let bytes = encode(&c);
        let back = decode(&bytes).unwrap();
        assert_eq!(back.k, 42);
        assert_eq!(back.t_end, c.t_end);
        // decode returns what encode wrote: sorted.
        assert_eq!(
            back.positions,
            vec![
                ("a/b space".to_string(), -1),
                ("cust-0001".to_string(), 17),
                ("cust-0002".to_string(), 0),
            ]
        );
    }

    #[test]
    fn encoding_is_deterministic_and_order_independent() {
        let a = cp(7, vec![("p1", 1), ("p2", 2), ("p3", 3)]);
        let b = cp(7, vec![("p3", 3), ("p1", 1), ("p2", 2)]);
        assert_eq!(encode(&a), encode(&a), "same input twice");
        assert_eq!(encode(&a), encode(&b), "input order must not move a byte");
    }

    #[test]
    fn positions_from_sorts() {
        use std::collections::HashMap;
        let mut m: HashMap<String, i64> = HashMap::new();
        for i in 0..64 {
            m.insert(format!("p{i:03}"), i as i64);
        }
        let v = positions_from(m.iter().map(|(k, v)| (k.as_str(), *v)));
        assert_eq!(v.len(), 64);
        assert!(v.windows(2).all(|w| w[0].0 < w[1].0), "sorted by name");
        assert_eq!(v[0], ("p000".to_string(), 0));
    }

    #[test]
    fn should_checkpoint_every_n() {
        assert!(!should_checkpoint(1, 20));
        assert!(should_checkpoint(20, 20));
        assert!(should_checkpoint(40, 20));
        assert!(!should_checkpoint(41, 20));
        assert!(!should_checkpoint(20, 0), "0 disables checkpointing");
        assert!(should_checkpoint(5, 1), "1 checkpoints every window");
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(decode(b"not zstd at all").is_err());
        let not_json = zstd::stream::encode_all(&b"{{{"[..], ZSTD_LEVEL).unwrap();
        assert!(decode(&not_json).is_err());
        assert!(decode(&[]).is_err());
    }
}
