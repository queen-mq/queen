//! `pack_segment`: phase (b) of the fire (PLAN_KV_TIMERS §1.3), extracted so the sweeper packs a
//! segment with the SAME recipe the fusion flush uses instead of growing a second one.
//!
//! Wired from `src/frames.rs` — see `src/tests_unit/README.md` for the three-line block.
//!
//! What is actually at stake, in the order it would hurt:
//!
//!  1. **The hash blob is the dedup identity SQL compares.** `queen.log_txns.hashes` is a
//!     `16 * msg_count` bytea with a frame-order stride; position k in the blob IS packed frame
//!     k. A misordered or short blob does not fail — it silently makes the dedup probe answer
//!     about a different message, and the fire's own alignment guard (§6.2 point 1,
//!     `octet_length(hash[i]) = count[i]*16`) is the only thing between that and a timer being
//!     deleted after the wrong frame was pushed.
//!  2. **The fire packs OUTSIDE any transaction and then commits all-or-nothing per segment**
//!     (§6.2 point 2). A blob that does not decode is discovered after the claim, with the lease
//!     held, and costs a whole batch.
//!  3. **The extraction must be faithful.** These frames go into the same `log_segments.blob`
//!     column the fusion path writes and are read back by the same pop. "Equivalent" is not
//!     enough: the bytes are compared here against the recipe fusion already ships.

use super::*;

fn f<'a>(mid: [u8; 16], txn: &'a str, payload: &'a [u8]) -> FrameIn<'a> {
    FrameIn { message_id: mid, txn, trace_id: None, producer_sub: None, payload, encrypted: false }
}

/// A segment shaped like one the fire actually builds: a scheduled timer carries a producer sub
/// stamped by the SP argument (§6.2 — never read from the op), and may be already encrypted
/// because encryption happens at schedule time, not at fire time (§13.4).
fn sample<'a>() -> Vec<FrameIn<'a>> {
    vec![
        FrameIn {
            message_id: [1u8; 16],
            txn: "timer:orders:reminder-42",
            trace_id: None,
            producer_sub: Some("svc-scheduler@acme"),
            payload: br#"{"orderId":42,"kind":"reminder"}"#,
            encrypted: false,
        },
        FrameIn {
            message_id: [2u8; 16],
            txn: "timer:orders:reminder-43",
            trace_id: Some([9u8; 16]),
            producer_sub: Some("svc-scheduler@acme"),
            payload: br#"{"orderId":43}"#,
            encrypted: true,
        },
        FrameIn {
            message_id: [3u8; 16],
            txn: "timer:orders:reminder-44",
            trace_id: None,
            producer_sub: None,
            payload: b"",
            encrypted: false,
        },
    ]
}

#[test]
fn blob_and_hashes_are_byte_identical_to_the_fusion_recipe() {
    // THE test of the extraction. `fusion::build_hashes_and_blob` is
    // `zstd_compress(pack_frames(fins), level)` plus the concatenated per-frame xxh3_128, and
    // pack_segment must be that and nothing else — same level, same frame order, same hash
    // serialization. If this ever fails, the timer path and the push path have started writing
    // different segment bytes into the same column.
    let fins = sample();
    let level = 3;

    let seg = pack_segment(&fins, level);

    let expect_blob = zstd_compress(&pack_frames(&fins), level);
    let mut expect_hashes = Vec::with_capacity(fins.len() * 16);
    for fr in &fins {
        expect_hashes.extend_from_slice(&crate::util::txn_hash128(fr.txn));
    }

    assert_eq!(seg.blob, expect_blob, "segment blob diverged from the fusion recipe");
    assert_eq!(seg.hashes, expect_hashes, "hash blob diverged from the fusion recipe");
    assert_eq!(seg.count, fins.len());
}

#[test]
fn the_hash_blob_satisfies_the_fires_alignment_guard() {
    // `octet_length(hash[i]) = count[i] * 16` is checked inside log_timers_fire_v1 and raises.
    // Producing a blob that fails it means the whole batch aborts with the lease held.
    for n in [0usize, 1, 2, 7, 200] {
        let txns: Vec<String> = (0..n).map(|i| format!("timer:q:k-{i}")).collect();
        let fins: Vec<FrameIn> = txns.iter().map(|t| f([0u8; 16], t, b"{}")).collect();
        let seg = pack_segment(&fins, 3);
        assert_eq!(seg.count, n);
        assert_eq!(seg.hashes.len(), seg.count * 16, "stride broken at {n} frames");
    }
}

#[test]
fn hash_k_is_the_hash_of_frame_k() {
    // Position, not identity: the SP echoes `i` for a duplicate and the broker maps that back to
    // a timer row to delete. Duplicate txns inside one segment must therefore keep their
    // positions — a dedup-at-pack-time "optimization" here would delete the wrong timer.
    let fins = vec![
        f([1u8; 16], "dup", b"{}"),
        f([2u8; 16], "other", b"{}"),
        f([3u8; 16], "dup", b"{}"),
    ];
    let seg = pack_segment(&fins, 3);
    assert_eq!(seg.count, 3, "pack_segment must not collapse duplicate txns");
    for (i, fr) in fins.iter().enumerate() {
        assert_eq!(
            &seg.hashes[i * 16..(i + 1) * 16],
            &crate::util::txn_hash128(fr.txn)[..],
            "hash slot {i} does not belong to frame {i}"
        );
    }
    assert_eq!(&seg.hashes[0..16], &seg.hashes[32..48], "equal txns must hash equal");
}

#[test]
fn the_blob_round_trips_every_field_the_fire_carries() {
    let fins = sample();
    let seg = pack_segment(&fins, 3);

    let raw = zstd_decompress(&seg.blob);
    let out = unpack_frames(&raw).expect("the fire's blob must decode");
    assert_eq!(out.len(), fins.len());

    for (o, i) in out.iter().zip(fins.iter()) {
        assert_eq!(o.txn, i.txn, "txn is the dedup identity and the cancel-after-fire answer");
        assert_eq!(o.payload, i.payload);
        assert_eq!(o.producer_sub.as_deref(), i.producer_sub, "§6.2: provenance is SP-supplied");
        assert_eq!(
            o.encrypted, i.encrypted,
            "§13.4: the ciphertext bit is set at SCHEDULE; losing it here delivers ciphertext \
             that the consumer never decrypts"
        );
        assert_eq!(o.message_id, uuid_bytes_to_string(&i.message_id), "the promised messageId");
    }
}

#[test]
fn an_empty_segment_is_representable_and_decodes_to_nothing() {
    // Reachable: every row of a group can turn out stale at claim-verification time. It must
    // produce an empty, VALID blob rather than something the fire has to special-case, and
    // certainly not a panic.
    let seg = pack_segment(&[], 3);
    assert_eq!(seg.count, 0);
    assert!(seg.hashes.is_empty());
    let out = unpack_frames(&zstd_decompress(&seg.blob)).expect("empty blob must still decode");
    assert!(out.is_empty());
}

#[test]
fn levels_the_sweeper_can_be_configured_with_all_round_trip() {
    let fins = sample();
    for level in [1, 3, 9, 19] {
        let seg = pack_segment(&fins, level);
        let out = unpack_frames(&zstd_decompress(&seg.blob))
            .unwrap_or_else(|| panic!("blob at zstd level {level} did not decode"));
        assert_eq!(out.len(), fins.len(), "level {level} lost frames");
        assert_eq!(out[0].txn, fins[0].txn);
    }
}

#[test]
fn a_payload_at_the_timer_ceiling_round_trips() {
    // QUEEN_TIMERS_MAX_PAYLOAD_BYTES is min(1 MiB, plan.max_payload_bytes) (§9.2). The frame
    // codec writes a u32 body length and a u16 txn length; a payload near the ceiling is where a
    // width mistake would show up, and it is exactly the shape a "one big scheduled report"
    // timer produces.
    let big = vec![b'x'; 1024 * 1024];
    let fins = vec![f([7u8; 16], "timer:reports:monthly", &big)];
    let seg = pack_segment(&fins, 3);
    let out = unpack_frames(&zstd_decompress(&seg.blob)).expect("1 MiB payload must decode");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].payload.len(), big.len());
    assert_eq!(seg.hashes.len(), 16);
}

#[test]
fn packing_is_deterministic() {
    // The fire may repack a group after a `duplicate` verdict removes some of its rows (§12).
    // Same input, same bytes — otherwise a retry is not a retry.
    let fins = sample();
    let a = pack_segment(&fins, 3);
    let b = pack_segment(&fins, 3);
    assert_eq!(a.blob, b.blob);
    assert_eq!(a.hashes, b.hashes);
}
