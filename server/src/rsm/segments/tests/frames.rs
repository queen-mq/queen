//! The frame of §11.2 on its own.

use super::super::frame::*;
use super::{blob, hashes};

fn one(pid: u64, base: u64, count: u32, blob_len: usize) -> Vec<u8> {
    let mut out = Vec::new();
    encode_into(
        &mut out,
        pid,
        base,
        count,
        -42,
        &hashes(pid, count),
        &blob(base, blob_len),
    )
    .expect("encode");
    out
}

#[test]
fn a_frame_round_trips_every_field() {
    let bytes = one(0xdead_beef, 1234, 3, 64);
    let f = decode(&bytes).expect("decode");
    assert_eq!(f.header.pid, 0xdead_beef);
    assert_eq!(f.header.base_offset, 1234);
    assert_eq!(f.header.count, 3);
    assert_eq!(f.header.created_at_us, -42);
    assert_eq!(f.header.end_offset(), 1237);
    assert_eq!(f.hashes, &hashes(0xdead_beef, 3)[..]);
    assert_eq!(f.blob, &blob(1234, 64)[..]);
    assert_eq!(f.header.frame_len(), bytes.len());
    assert_eq!(encoded_len(3, 64), bytes.len());
}

#[test]
fn a_frame_with_no_messages_and_no_payload_is_legal() {
    // The smallest thing the format can say. It still carries pid and base, so
    // a scan of a file that holds one can still step over it.
    let bytes = one(7, 0, 0, 0);
    assert_eq!(bytes.len(), HEADER_LEN);
    let f = decode(&bytes).expect("decode");
    assert_eq!(f.header.count, 0);
    assert!(f.hashes.is_empty() && f.blob.is_empty());
}

#[test]
fn len_covers_everything_after_itself_so_a_scan_can_step() {
    // Three frames back to back: stepping by `frame_len` must land exactly on
    // each one. This is the property that makes a file self-describing and a
    // `.qidx` rebuildable (§11.2).
    let mut file = Vec::new();
    let mut at = Vec::new();
    for i in 0..3u64 {
        at.push(file.len());
        encode_into(
            &mut file,
            9,
            i * 10,
            2,
            i as i64,
            &hashes(i, 2),
            &blob(i, 8 + i as usize * 40),
        )
        .expect("encode");
    }
    let mut pos = 0usize;
    for (i, want) in at.iter().enumerate() {
        assert_eq!(pos, *want, "frame {i} does not start where it was written");
        let f = decode(&file[pos..]).expect("decode in place");
        assert_eq!(f.header.base_offset, i as u64 * 10);
        pos += f.header.frame_len();
    }
    assert_eq!(pos, file.len());
}

#[test]
fn every_single_bit_flip_in_a_frame_is_caught() {
    // The checksum covers everything after itself, so a flip in pid,
    // base_offset, count, created_at, a hash or the blob must be caught; a
    // flip in `len` or in the checksum itself is caught as a length or a
    // checksum error. Nothing may decode and be believed.
    let bytes = one(5, 100, 2, 24);
    for byte in 0..bytes.len() {
        for bit in 0..8 {
            let mut b = bytes.clone();
            b[byte] ^= 1 << bit;
            if b == bytes {
                continue;
            }
            match decode(&b) {
                Err(_) => {}
                Ok(f) => panic!(
                    "byte {byte} bit {bit} decoded as pid {} base {} count {}",
                    f.header.pid, f.header.base_offset, f.header.count
                ),
            }
        }
    }
}

#[test]
fn a_truncation_at_any_length_is_reported_never_believed() {
    let bytes = one(5, 100, 2, 24);
    for cut in 0..bytes.len() {
        let e = decode(&bytes[..cut]).expect_err("a short buffer must not decode");
        assert!(
            matches!(e, FrameError::Truncated { .. } | FrameError::BadLength(_)),
            "cut {cut} gave {e:?}"
        );
    }
    assert!(decode(&bytes).is_ok());
}

#[test]
fn a_lying_count_is_refused_before_anything_is_read() {
    // `count` decides how many bytes are hashes; a header claiming more than
    // `len` leaves room for is refused by `parse_header`, which is what a
    // scanner calls BEFORE it allocates the body.
    let mut bytes = one(5, 0, 1, 8);
    bytes[28..32].copy_from_slice(&u32::MAX.to_le_bytes());
    let e = parse_header(&bytes).expect_err("a count past the body must be refused");
    assert!(matches!(e, FrameError::Stride { .. }), "{e:?}");
}

#[test]
fn a_length_below_the_header_or_past_the_cap_is_refused() {
    let mut bytes = one(5, 0, 1, 8);
    bytes[0..4].copy_from_slice(&3u32.to_le_bytes());
    assert!(matches!(
        parse_header(&bytes),
        Err(FrameError::BadLength(3))
    ));
    bytes[0..4].copy_from_slice(&(MAX_FRAME_BODY_LEN + 1).to_le_bytes());
    assert!(matches!(
        parse_header(&bytes),
        Err(FrameError::BadLength(_))
    ));
}

#[test]
fn the_encoder_refuses_a_hash_list_that_is_not_sixteen_per_message() {
    let mut out = Vec::new();
    let e = encode_into(&mut out, 1, 0, 3, 0, &[0u8; 32], &[]).expect_err("stride");
    assert!(
        matches!(
            e,
            FrameError::HashStride {
                count: 3,
                hashes: 32
            }
        ),
        "{e:?}"
    );
}

#[test]
fn hostile_bytes_never_panic() {
    // Not a fuzzer: a deterministic sweep of shapes a damaged or hostile file
    // can present. The only acceptable outcomes are Ok and Err.
    let mut seed = 0x243f_6a88_85a3_08d3u64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let good = one(11, 22, 4, 48);
    for i in 0..20_000u32 {
        let mut b = good.clone();
        match i % 4 {
            0 => {
                let at = (next() as usize) % b.len();
                b[at] = next() as u8;
            }
            1 => b.truncate((next() as usize) % (b.len() + 1)),
            2 => {
                let n = (next() as usize) % 64;
                b = (0..n).map(|_| next() as u8).collect();
            }
            _ => {
                // A believable header over nothing at all: the shape that
                // would make a naive reader allocate `len` bytes.
                b.truncate(HEADER_LEN);
                b[0..4].copy_from_slice(&((next() % (1 << 20)) as u32 + 36).to_le_bytes());
            }
        }
        let _ = decode(&b);
        let _ = parse_header(&b);
    }
}
