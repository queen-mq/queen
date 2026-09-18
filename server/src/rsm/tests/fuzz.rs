//! The decoder under hostile bytes: random buffers, mutations of valid
//! encodings, and hand-crafted headers that lie.
//!
//! The contract is narrow and absolute: **decoding never panics, never
//! aborts and never allocates on a length prefix it has not checked**. It
//! returns a [`CodecError`]. Entries reach this code from a peer's socket and
//! from files a crash truncated, so a panic here is an availability bug and an
//! OOM abort is a remote one.
//!
//! The PRNG is a fixed-seed SplitMix64 written out in full rather than the
//! `rand` crate: the corpus must be identical on every machine and in every
//! run, so a failure is reproducible from the seed printed in the assertion.

use super::samples::*;
use crate::rsm::effect::*;
use crate::rsm::entry::*;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // SplitMix64.
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
    fn byte(&mut self) -> u8 {
        (self.next() >> 24) as u8
    }
}

/// Both decoders, on the same bytes. Neither may panic.
fn feed(bytes: &[u8]) {
    let _ = decode_entry(bytes);
    let _ = decode_entry_at(bytes);
    let _ = decode_effect(bytes);
    let _ = parse_entry_header(bytes);
}

#[test]
fn random_bytes_never_panic() {
    let mut rng = Rng(0x5EED_1111_2222_3333);
    let mut buf = Vec::with_capacity(1024);
    for _ in 0..20_000 {
        let n = rng.below(600);
        buf.clear();
        for _ in 0..n {
            buf.push(rng.byte());
        }
        feed(&buf);
    }
}

#[test]
fn structured_random_bytes_never_panic() {
    // Purely random bytes almost never get past the checksum, so most of the
    // decoder never runs. This corpus keeps the FRAME honest (a correct length
    // and checksum) and randomises the body, which is what walks the field
    // decoders with garbage.
    let mut rng = Rng(0xABCD_4444_5555_6666);
    for _ in 0..20_000 {
        let n = rng.below(256);
        let mut body = Vec::with_capacity(n);
        for _ in 0..n {
            body.push(rng.byte());
        }
        // A plausible header in front of a random body, half the time.
        if rng.next() & 1 == 0 && body.len() >= 34 {
            body[0..2].copy_from_slice(&ENTRY_FORMAT.to_le_bytes());
            body[2..6].copy_from_slice(&1u32.to_le_bytes());
        }
        let mut framed = Vec::with_capacity(ENTRY_HEADER_LEN + body.len());
        framed.extend_from_slice(&(body.len() as u32).to_le_bytes());
        framed.extend_from_slice(&checksum(&body).to_le_bytes());
        framed.extend_from_slice(&body);
        feed(&framed);
    }
}

#[test]
fn mutations_of_valid_encodings_never_panic() {
    let mut corpus: Vec<Vec<u8>> = all_effect_samples()
        .iter()
        .map(|(_, e)| encode_effect(e))
        .collect();
    corpus.push(encode_entry(&entry_sample()).expect("encode"));
    for o in outcome_samples() {
        let mut e = Entry::new(1, 1, 1);
        e.add_command(uuid(9), o, vec![Effect::Noop]).unwrap();
        corpus.push(encode_entry(&e).expect("encode"));
    }

    let mut rng = Rng(0xFACE_7777_8888_9999);
    for round in 0..20_000 {
        let seed = &corpus[rng.below(corpus.len())];
        let mut b = seed.clone();
        match round % 4 {
            // Flip some bytes.
            0 => {
                for _ in 0..1 + rng.below(6) {
                    let i = rng.below(b.len());
                    b[i] ^= 1 << rng.below(8);
                }
            }
            // Replace some bytes outright (length prefixes become absurd).
            1 => {
                for _ in 0..1 + rng.below(4) {
                    let i = rng.below(b.len());
                    b[i] = rng.byte();
                }
            }
            // Cut the tail.
            2 => {
                let keep = rng.below(b.len());
                b.truncate(keep);
            }
            // Append junk.
            _ => {
                for _ in 0..1 + rng.below(16) {
                    b.push(rng.byte());
                }
            }
        }
        feed(&b);
        // Half the time, REPAIR the frame around the mutated body. Without
        // this the entry corpus would stop at the checksum every time and the
        // field decoders below it would never see a mutation at all — a fuzz
        // test that only exercises xxh3.
        if b.len() > ENTRY_HEADER_LEN && rng.next() & 1 == 0 {
            let body_len = b.len() - ENTRY_HEADER_LEN;
            let sum = checksum(&b[ENTRY_HEADER_LEN..]);
            b[0..4].copy_from_slice(&(body_len as u32).to_le_bytes());
            b[4..12].copy_from_slice(&sum.to_le_bytes());
            feed(&b);
        }
    }
}

/// The corpus above is worthless if it never gets past the frame, so this
/// counts how deep it reaches: a repaired mutation of a valid entry must, most
/// of the time, decode into the body and fail (or succeed) THERE.
#[test]
fn the_mutation_corpus_reaches_the_field_decoders() {
    let seed = encode_entry(&entry_sample()).expect("encode");
    let mut rng = Rng(0x1234_5678_9ABC_DEF0);
    let (mut past_the_frame, rounds) = (0usize, 2_000);
    for _ in 0..rounds {
        let mut b = seed.clone();
        // Mutate the BODY only, then repair the frame.
        for _ in 0..1 + rng.below(4) {
            let i = ENTRY_HEADER_LEN + rng.below(b.len() - ENTRY_HEADER_LEN);
            b[i] ^= 1 << rng.below(8);
        }
        let sum = checksum(&b[ENTRY_HEADER_LEN..]);
        b[4..12].copy_from_slice(&sum.to_le_bytes());
        match decode_entry(&b) {
            Err(CodecError::Checksum) | Err(CodecError::Header) => {}
            _ => past_the_frame += 1,
        }
    }
    assert!(
        past_the_frame * 2 > rounds,
        "only {past_the_frame} of {rounds} mutations reached the body decoder"
    );
}

#[test]
fn a_lying_count_does_not_allocate() {
    // The classic way to make a hand-rolled decoder abort: a well-formed frame
    // whose counted vector claims four billion items. The reader reserves what
    // could still fit, capped, so this errors in microseconds instead of
    // asking the allocator for tens of gigabytes.
    let mut body = Vec::new();
    body.extend_from_slice(&ENTRY_FORMAT.to_le_bytes());
    body.extend_from_slice(&1u32.to_le_bytes()); // kinds_version
    body.extend_from_slice(&1i64.to_le_bytes()); // now_us
    body.extend_from_slice(&1u64.to_le_bytes()); // pid_base
    body.extend_from_slice(&1u64.to_le_bytes()); // kv_version_base
    body.extend_from_slice(&u32::MAX.to_le_bytes()); // command_count

    let mut framed = Vec::new();
    framed.extend_from_slice(&(body.len() as u32).to_le_bytes());
    framed.extend_from_slice(&checksum(&body).to_le_bytes());
    framed.extend_from_slice(&body);

    let started = std::time::Instant::now();
    assert!(decode_entry(&framed).is_err());
    assert!(
        started.elapsed().as_secs() < 5,
        "the decoder did real work on a lying count"
    );

    // The same for the effect vector, and for the counted vectors inside an
    // effect body (`pids`, `delivered`, `names`, `hashes`).
    let mut body2 = body[..body.len() - 4].to_vec();
    body2.extend_from_slice(&0u32.to_le_bytes()); // no commands
    body2.extend_from_slice(&u32::MAX.to_le_bytes()); // …and four billion effects
    let mut framed2 = Vec::new();
    framed2.extend_from_slice(&(body2.len() as u32).to_le_bytes());
    framed2.extend_from_slice(&checksum(&body2).to_le_bytes());
    framed2.extend_from_slice(&body2);
    assert!(decode_entry(&framed2).is_err());

    let mut eff = Vec::new();
    eff.extend_from_slice(&(Kind::GarbageAdd as u16).to_le_bytes());
    eff.extend_from_slice(&VERSION_1.to_le_bytes());
    eff.extend_from_slice(&4u32.to_le_bytes());
    eff.extend_from_slice(&u32::MAX.to_le_bytes()); // pids: four billion
    assert!(decode_effect(&eff).is_err());
}

#[test]
fn a_lying_length_prefix_is_refused_before_it_is_believed() {
    // An effect header that claims a body larger than MAX_BODY_LEN is a bad
    // header, not a truncated record: nothing downstream should try to wait
    // for more bytes to arrive.
    let mut eff = encode_effect(&Effect::PartitionDelete { pid: 1 });
    eff[4..8].copy_from_slice(&(MAX_BODY_LEN + 1).to_le_bytes());
    assert_eq!(decode_effect(&eff), Err(CodecError::Header));

    let mut ent = encode_entry(&entry_sample()).expect("encode");
    ent[0..4].copy_from_slice(&(MAX_BODY_LEN + 1).to_le_bytes());
    assert_eq!(decode_entry(&ent), Err(CodecError::Header));
    assert!(parse_entry_header(&ent).is_none());

    // And one that claims MORE than the buffer holds is truncated, so a reader
    // walking a file knows to stop rather than to discard.
    let mut ent = encode_entry(&entry_sample()).expect("encode");
    let len = u32::from_le_bytes(ent[0..4].try_into().unwrap());
    ent[0..4].copy_from_slice(&(len + 1).to_le_bytes());
    assert_eq!(decode_entry(&ent), Err(CodecError::Truncated));
}

#[test]
fn an_empty_buffer_is_an_error_not_a_panic() {
    assert!(decode_entry(&[]).is_err());
    assert!(decode_effect(&[]).is_err());
    assert!(parse_entry_header(&[]).is_none());
    for n in 0..ENTRY_HEADER_LEN {
        let buf = vec![0u8; n];
        assert!(decode_entry(&buf).is_err());
    }
    for n in 0..EFFECT_HEADER_LEN {
        let buf = vec![0u8; n];
        assert_eq!(decode_effect(&buf), Err(CodecError::Truncated));
    }
}
