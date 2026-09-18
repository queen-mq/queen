//! The gates the codec owes beyond its bytes: the catalogue-version gate
//! (§5.3, D20, I16), the checksum boundary (§11.5, I11), and the values the
//! encoder refuses to write because the decoder could never read them.
//!
//! A round trip and a golden fixture both start from the encoder, so neither
//! can see any of this. Every test here starts from BYTES — hand-built, or
//! forged with the unchecked encoder — and asks what the node does with them.

use super::samples::*;
use crate::rsm::effect::*;
use crate::rsm::entry::*;

/// The POP tag (§5.4). Spelled out because the tag table is private: the point
/// of these tests is what a body carrying this number does, not what the
/// constant is called.
const TAG_POP: u16 = 2;
/// The first tag of the reserved placeholder range.
const TAG_PLACEHOLDER_MIN: u16 = 0xF000;

/// One entry, assembled by hand: the header, one command carrying the given
/// `(tag, version, body)` outcome, and one `Noop` effect. Nothing here goes
/// through the encoder under test, so it can express what the encoder cannot.
fn hand_built_entry(kinds_version: u32, tag: u16, version: u16, outcome_body: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&ENTRY_FORMAT.to_le_bytes());
    body.extend_from_slice(&kinds_version.to_le_bytes());
    body.extend_from_slice(&1_768_000_000_000_042i64.to_le_bytes()); // now_us
    body.extend_from_slice(&7u64.to_le_bytes()); // pid_base
    body.extend_from_slice(&91_005u64.to_le_bytes()); // kv_version_base

    body.extend_from_slice(&1u32.to_le_bytes()); // one command
    body.extend_from_slice(&uuid(0xc0)); // request id
    body.extend_from_slice(&0u32.to_le_bytes()); // first_effect
    body.extend_from_slice(&1u32.to_le_bytes()); // effect_count
    body.extend_from_slice(&tag.to_le_bytes());
    body.extend_from_slice(&version.to_le_bytes());
    body.extend_from_slice(&(outcome_body.len() as u32).to_le_bytes());
    body.extend_from_slice(outcome_body);

    body.extend_from_slice(&1u32.to_le_bytes()); // one effect
    body.extend_from_slice(&(Kind::Noop as u16).to_le_bytes());
    body.extend_from_slice(&VERSION_1.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes()); // an empty body

    frame(&body)
}

/// `body_len | xxh3 | body`, the entry frame, computed here so a test can
/// mutate a body and make the bytes honest again.
fn frame(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(ENTRY_HEADER_LEN + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&checksum(body).to_le_bytes());
    out.extend_from_slice(body);
    out
}

// ---------------------------------------------------------------------------
// The catalogue gate covers OUTCOMES, not only effect kinds
// ---------------------------------------------------------------------------

#[test]
fn an_outcome_at_an_unknown_version_stops_the_node() {
    // The hole this test exists for: a placeholder USED to be returned before
    // the version was ever looked at, so a body from a later catalogue was
    // decoded as this one's, silently, and re-encoded at version 1. In a
    // rolling upgrade (D20) that is an old voter answering the retry of a
    // command (D6, I6) from bytes it never understood — a wrong answer to a
    // client, where I16 asks for a node that stops.
    let bytes = hand_built_entry(1, 0xF001, 7, b"future-kv-shape");
    let err = decode_entry(&bytes).unwrap_err();
    assert_eq!(
        err,
        CodecError::UnknownVersion {
            kind: 0xF001,
            version: 7
        },
        "a reserved-range outcome at an unknown version was interpreted"
    );
    assert!(err.fatal(), "{err}");

    // A typed tag at an unknown version was already fatal; it stays so.
    let err = decode_entry(&hand_built_entry(1, TAG_POP, 7, &[])).unwrap_err();
    assert_eq!(
        err,
        CodecError::UnknownVersion {
            kind: TAG_POP,
            version: 7
        }
    );
    assert!(err.fatal(), "{err}");

    // Version 0 is not a catalogue version at all.
    let err = decode_entry(&hand_built_entry(1, 0xF001, 0, b"x")).unwrap_err();
    assert!(err.fatal(), "{err}");
}

#[test]
fn a_placeholder_keeps_its_version_across_the_codec() {
    // The same bug seen from the other side: the decoded value re-encodes to
    // the SAME bytes. A version dropped on the way back would make the entry a
    // different entry — and the chain digest of §12.9 is over entry bytes.
    let mut e = Entry::new(1, 2, 3);
    e.add_command(
        uuid(1),
        Outcome::Placeholder(Placeholder::new(0xF00A, VERSION_1, b"private".to_vec()).unwrap()),
        vec![Effect::Noop],
    )
    .unwrap();
    let bytes = encode_entry(&e).expect("encode");
    let back = decode_entry(&bytes).expect("decode");
    assert_eq!(back, e);
    match &back.commands[0].outcome {
        Outcome::Placeholder(p) => {
            assert_eq!(
                (p.tag(), p.version(), p.body()),
                (0xF00A, VERSION_1, &b"private"[..])
            );
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(encode_entry(&back).expect("re-encode"), bytes);
}

#[test]
fn an_outcomes_version_reaches_the_header_gate() {
    // `kinds_version` used to be the maximum over EFFECTS only, so an entry
    // whose only novelty was an outcome reported version 1 and sailed through
    // D20's gate. It must report what the outcome was minted at.
    let later = Placeholder::at_version_for_tests(0xF001, 2, b"v2-shape".to_vec());
    let mut e = Entry::new(1, 2, 3);
    e.add_command(uuid(1), Outcome::Placeholder(later), vec![Effect::Noop])
        .unwrap();

    assert_eq!(
        kinds_version_of(&e.effects),
        1,
        "the effects alone are version 1: that is the reading that hid this"
    );
    assert_eq!(catalogue_version_of(&e.commands, &e.effects), 2);
    assert_eq!(e.kinds_version, 2, "add_command must fold the outcome in");

    // And this build, which supports catalogue version 1, refuses to write it
    // rather than proposing an entry an old node would misread.
    assert_eq!(encode_entry(&e), Err(CodecError::UnknownCatalogue(2)));
    assert!(CodecError::UnknownCatalogue(2).fatal());

    // A header that lies downwards is refused as well: it would walk the
    // outcome past the gate.
    let mut lying = e.clone();
    lying.kinds_version = 1;
    assert_eq!(
        lying.validate(),
        Err(CodecError::Layout(
            "kinds_version below what the entry carries"
        ))
    );
}

#[test]
fn an_entry_from_a_later_catalogue_is_refused_whole() {
    // Read off the header, before any body: a node on catalogue 1 does not get
    // to decide field by field which parts of a catalogue-2 entry it still
    // understands (§12.8, I16).
    let good = encode_entry(&entry_sample()).expect("encode");
    let mut body = good[ENTRY_HEADER_LEN..].to_vec();
    body[2..6].copy_from_slice(&(SUPPORTED_KINDS_VERSION + 1).to_le_bytes());
    let err = decode_entry(&frame(&body)).unwrap_err();
    assert_eq!(
        err,
        CodecError::UnknownCatalogue(SUPPORTED_KINDS_VERSION + 1)
    );
    assert!(err.fatal(), "{err}");
}

// ---------------------------------------------------------------------------
// The checksum boundary: what a verified body's failure means
// ---------------------------------------------------------------------------

#[test]
fn a_verified_body_that_does_not_decode_is_fatal_not_a_torn_tail() {
    // Everything past the xxh3 check is what the LEADER wrote and what a
    // quorum committed. §11.5's rule for damage — truncate the tail, or
    // discard the state directory — must not fire on it: truncating or
    // skipping a committed entry is the silent divergence I16 forbids.
    //
    // A field error. `Append`'s hash stride is the realistic one: a planner
    // bug, or an encoder that widens a field without bumping its version.
    let mut e = Entry::new(1, 2, 3);
    e.add_command(
        uuid(1),
        Outcome::Empty,
        vec![Effect::Append {
            pid: 7,
            bucket: 1,
            base_offset: 0,
            count: 3,
            created_at_us: 1,
            hashes: vec![0u8; 32], // two hashes for three frames
            blob: vec![],
        }],
    )
    .unwrap();
    let err = decode_entry(&encode_entry_unchecked(&e)).unwrap_err();
    assert_eq!(err, CodecError::Malformed("hashes stride"));
    assert!(
        err.fatal(),
        "a committed entry that does not decode was called a torn tail: {err}"
    );

    // A layout error, on bytes that are otherwise perfect.
    let mut overlapping = entry_sample();
    overlapping.commands[1].first_effect = 0;
    let err = decode_entry(&encode_entry_unchecked(&overlapping)).unwrap_err();
    assert_eq!(err, CodecError::Malformed("command spans overlap"));
    assert!(err.fatal(), "{err}");

    // A body that ends inside a record although the frame said it was whole:
    // the length prefix and the checksum agree, so this is not a torn tail
    // either.
    let good = encode_entry(&entry_sample()).expect("encode");
    let short = &good[ENTRY_HEADER_LEN..good.len() - 40];
    let err = decode_entry(&frame(short)).unwrap_err();
    assert_eq!(err, CodecError::Malformed("the body ends inside a record"));
    assert!(err.fatal(), "{err}");

    // And the other side of the boundary is unchanged: BEFORE the checksum,
    // damage is damage, and recovery may truncate.
    assert_eq!(
        decode_entry(&good[..good.len() - 1]),
        Err(CodecError::Truncated)
    );
    assert!(!CodecError::Truncated.fatal());
    let mut flipped = good.clone();
    flipped[ENTRY_HEADER_LEN + 3] ^= 0x80;
    assert_eq!(decode_entry(&flipped), Err(CodecError::Checksum));
    assert!(!CodecError::Checksum.fatal());
    assert!(!CodecError::Header.fatal());
    // Raised on unverified bytes (a standalone effect, a file tail), these two
    // still mean "damage"; it is the entry decoder that promotes them.
    assert!(!CodecError::Field("x").fatal());
    assert!(!CodecError::Layout("x").fatal());
}

#[test]
fn every_post_checksum_error_is_re_classified() {
    // The promotion in one place, so no future caller has to remember it.
    assert_eq!(
        CodecError::Field("f").after_checksum(),
        CodecError::Malformed("f")
    );
    assert_eq!(
        CodecError::Layout("l").after_checksum(),
        CodecError::Malformed("l")
    );
    assert!(matches!(
        CodecError::Truncated.after_checksum(),
        CodecError::Malformed(_)
    ));
    assert!(matches!(
        CodecError::Header.after_checksum(),
        CodecError::Malformed(_)
    ));
    // The Unknown* family already says something sharper: it is kept as it is.
    for err in [
        CodecError::UnknownKind(9),
        CodecError::UnknownVersion {
            kind: 1,
            version: 9,
        },
        CodecError::UnknownOutcome(9),
        CodecError::UnknownFormat(9),
        CodecError::UnknownCatalogue(9),
    ] {
        assert_eq!(err.clone().after_checksum(), err);
        assert!(err.fatal(), "{err}");
    }
}

// ---------------------------------------------------------------------------
// What the encoder refuses to write
// ---------------------------------------------------------------------------

#[test]
fn a_placeholder_cannot_carry_a_typed_tag() {
    // This was a `debug_assert!`, which is nothing in a release build: the
    // outcome encoded with the POP tag and the decoder then read a private
    // body as a pop claim. Now the value cannot be built at all.
    let err = Placeholder::new(TAG_POP, VERSION_1, b"kv".to_vec()).unwrap_err();
    assert_eq!(
        err,
        CodecError::Layout("a placeholder outcome must carry a tag in the reserved range")
    );
    assert!(Placeholder::new(TAG_PLACEHOLDER_MIN - 1, VERSION_1, vec![]).is_err());
    assert!(Placeholder::new(TAG_PLACEHOLDER_MIN, VERSION_1, vec![]).is_ok());
    assert!(Placeholder::new(u16::MAX, VERSION_1, vec![]).is_ok());

    // Nor at a version this build does not know: it could not read it back.
    assert_eq!(
        Placeholder::new(0xF001, 7, vec![]),
        Err(CodecError::UnknownVersion {
            kind: 0xF001,
            version: 7
        })
    );
}

#[test]
fn the_encoder_refuses_an_entry_it_could_never_read_back() {
    // `encode_entry` runs `validate`, because these bytes are what gets
    // proposed: an entry the cluster commits and every node then refuses is a
    // committed entry nobody can apply.
    let mut bad_effect = Entry::new(1, 2, 3);
    bad_effect
        .add_command(
            uuid(1),
            Outcome::Empty,
            vec![Effect::Append {
                pid: 7,
                bucket: 1,
                base_offset: 0,
                count: 3,
                created_at_us: 1,
                hashes: vec![0u8; 32],
                blob: vec![],
            }],
        )
        .unwrap();
    assert_eq!(
        encode_entry(&bad_effect),
        Err(CodecError::Layout("append hashes stride"))
    );

    let mut orphan = entry_sample();
    orphan.effects.push(Effect::Noop);
    assert_eq!(
        encode_entry(&orphan),
        Err(CodecError::Layout("an effect belongs to no command"))
    );

    // The sample itself still encodes, so the checks above are not a blanket
    // refusal.
    assert!(encode_entry(&entry_sample()).is_ok());
}

// ---------------------------------------------------------------------------
// I18: the counters the planner hands out, against the header's own bases
// ---------------------------------------------------------------------------

/// A `PartitionCreate` at a given pid. Everything else about it is constant:
/// only the pid is under test.
fn partition_create(pid: Pid) -> Effect {
    Effect::PartitionCreate {
        pid,
        uuid: uuid(0x30),
        tenant: T0.into(),
        queue: "orders".into(),
        partition: "Default".into(),
        created_at_us: 1_768_000_000_000_000,
    }
}

/// A `KvPut` at a given version, of the key the Kafka facade fences on.
fn kv_put(version: u64) -> Effect {
    Effect::KvPut {
        tenant: T0.into(),
        ns: "qk".into(),
        key: "fence".into(),
        value: b"1".to_vec(),
        version,
        expires_at_us: None,
        created_at_us: 1_768_000_000_000_000,
        updated_at_us: 1_768_000_000_000_000,
    }
}

#[test]
fn a_pid_assigned_twice_in_one_entry_is_refused() {
    // The hole this test exists for: apply's I18 assertion compares the
    // header's bases with `meta`, which a duplicate assignment leaves EQUAL,
    // and then advances them by the count. So an entry whose two
    // `PartitionCreate`s carry one pid commits, and every node applies it
    // identically and wrongly: `partitions_by_key` maps two (tenant, queue,
    // partition) keys to one pid, and from then on the second partition's
    // appends land in the first's segments and cursors — cross-queue, and
    // across tenants under a `Tenant` scope — with no node stopping.
    let mut twice = Entry::new(1_768_000_000_000_042, 100, 7);
    twice
        .add_command(
            uuid(1),
            Outcome::Empty,
            vec![partition_create(100), partition_create(100)],
        )
        .expect("add_command does not check I18; validate does");
    assert_eq!(
        twice.validate(),
        Err(CodecError::Layout(
            "a created pid is not pid_base + its ordinal"
        ))
    );
    assert_eq!(
        encode_entry(&twice),
        Err(CodecError::Layout(
            "a created pid is not pid_base + its ordinal"
        ))
    );
    // Forged past the encoder, it is a committed entry no node may apply, not
    // a torn tail §11.5 could truncate.
    let err = decode_entry(&encode_entry_unchecked(&twice)).unwrap_err();
    assert_eq!(
        err,
        CodecError::Malformed("a created pid is not pid_base + its ordinal")
    );
    assert!(err.fatal(), "{err}");

    // A pid from somewhere else entirely (a stale overlay after a step-down,
    // D4): equally refused, in both directions.
    let mut stale = Entry::new(1, 100, 7);
    stale
        .add_command(uuid(1), Outcome::Empty, vec![partition_create(57)])
        .unwrap();
    assert!(stale.validate().is_err());
    let mut ahead = Entry::new(1, 100, 7);
    ahead
        .add_command(uuid(1), Outcome::Empty, vec![partition_create(101)])
        .unwrap();
    assert!(ahead.validate().is_err(), "a gap is not allowed either");

    // The honest shape: the ordinal runs over the ENTRY's effects, across
    // commands, in apply order.
    let mut good = Entry::new(1, 100, 7);
    good.add_command(
        uuid(1),
        Outcome::Empty,
        vec![partition_create(100), effect_sample(Kind::Append)],
    )
    .unwrap();
    good.add_command(uuid(2), Outcome::Empty, vec![partition_create(101)])
        .unwrap();
    good.validate().expect("100 then 101 from base 100");
    assert!(encode_entry(&good).is_ok());

    // And the base itself cannot be walked past the end of the number line.
    let mut overflow = Entry::new(1, u64::MAX, 7);
    overflow
        .add_command(
            uuid(1),
            Outcome::Empty,
            vec![partition_create(u64::MAX), partition_create(0)],
        )
        .unwrap();
    assert_eq!(
        overflow.validate(),
        Err(CodecError::Layout("pid_base + ordinal overflows"))
    );
}

#[test]
fn a_kv_version_assigned_twice_in_one_entry_is_refused() {
    // §8, 024: versions are unique, never reused and strictly increasing
    // across the entry, "so two commands writing one key in one entry get
    // different versions and a stale `expect` cannot win (Kafka `qk:fence`,
    // S3 sink lease)". Two writes at one version is exactly that loss.
    let mut e = Entry::new(1, 100, 9_000);
    e.add_command(uuid(1), Outcome::Empty, vec![kv_put(9_000)])
        .unwrap();
    e.add_command(uuid(2), Outcome::Empty, vec![kv_put(9_000)])
        .unwrap();
    assert_eq!(
        e.validate(),
        Err(CodecError::Layout(
            "a KV version is not kv_version_base + its ordinal"
        ))
    );
    let err = decode_entry(&encode_entry_unchecked(&e)).unwrap_err();
    assert!(err.fatal(), "{err}");

    // Consecutive versions from the base, and a `KvDelete` between them, which
    // is not a versioned write and takes no ordinal.
    let mut good = Entry::new(1, 100, 9_000);
    good.add_command(
        uuid(1),
        Outcome::Empty,
        vec![kv_put(9_000), effect_sample(Kind::KvDelete), kv_put(9_001)],
    )
    .unwrap();
    good.validate().expect("9000, a delete, 9001");

    let mut overflow = Entry::new(1, 0, u64::MAX);
    overflow
        .add_command(uuid(1), Outcome::Empty, vec![kv_put(u64::MAX), kv_put(0)])
        .unwrap();
    assert_eq!(
        overflow.validate(),
        Err(CodecError::Layout("kv_version_base + ordinal overflows"))
    );
}

#[test]
fn two_commands_cannot_share_a_request_id() {
    // §5.4: apply inserts `request_id → (now, outcome)` for EVERY logged
    // command, so two commands with one id in one entry collapse to the last
    // one's outcome. D13 makes that reachable without any planner bug: the
    // `propose` deadline fires, the receiver retries with the SAME id (D6),
    // and the retry reaches the planner while the original is still in the
    // drain queue. The client's retry of the first command would then be
    // answered with the second's claim.
    let mut e = Entry::new(1, 2, 3);
    e.add_command(uuid(7), Outcome::Empty, vec![Effect::Noop])
        .unwrap();
    e.add_command(
        uuid(7),
        Outcome::Pop(PopOutcome {
            claims: vec![PopClaim {
                pid: 7,
                start_offset: 0,
                end_offset: 1,
                worker: "worker-7".into(),
                lease_expires_at_us: Some(2),
                delivery_attempt: 1,
                conflated: false,
            }],
        }),
        vec![Effect::Noop],
    )
    .unwrap();
    assert_eq!(
        e.validate(),
        Err(CodecError::Layout("two commands share a request id"))
    );
    assert_eq!(
        encode_entry(&e),
        Err(CodecError::Layout("two commands share a request id"))
    );
    let err = decode_entry(&encode_entry_unchecked(&e)).unwrap_err();
    assert_eq!(
        err,
        CodecError::Malformed("two commands share a request id")
    );
    assert!(err.fatal(), "{err}");

    // The pair is not adjacent: the scan is over a sorted copy, not over
    // neighbours in plan order.
    let mut apart = Entry::new(1, 2, 3);
    for id in [uuid(1), uuid(2), uuid(1)] {
        apart
            .add_command(id, Outcome::Empty, vec![Effect::Noop])
            .unwrap();
    }
    assert_eq!(
        apart.validate(),
        Err(CodecError::Layout("two commands share a request id"))
    );

    // Distinct ids are fine, and the sample entry (three of them) still is.
    let mut distinct = Entry::new(1, 2, 3);
    for id in [uuid(1), uuid(2), uuid(3)] {
        distinct
            .add_command(id, Outcome::Empty, vec![Effect::Noop])
            .unwrap();
    }
    distinct.validate().expect("three different ids");
    entry_sample().validate().expect("the sample entry");
}

// ---------------------------------------------------------------------------
// The catalogue version is a per-kind and per-tag fact, not a constant
// ---------------------------------------------------------------------------

#[test]
fn every_outcome_tag_pins_its_version_and_an_unknown_tag_is_named() {
    // `Outcome::version` and `outcome_version_is_known` used to answer without
    // looking at the variant or the tag, so a phase-2 outcome would report
    // catalogue version 1 by default and walk past D20's gate. Both are
    // matches now; this pins what they answer today.
    for o in outcome_samples() {
        assert_eq!(
            o.version(),
            VERSION_1,
            "every outcome this build mints is at catalogue version 1: {o:?}"
        );
    }

    // A typed tag no decoder exists for (6 is the first reserved for phase 2):
    // fatal, and named as an unknown OUTCOME rather than as a version.
    let err = decode_entry(&hand_built_entry(1, 6, VERSION_1, &[])).unwrap_err();
    assert_eq!(err, CodecError::UnknownOutcome(6));
    assert!(err.fatal(), "{err}");
    let err = decode_entry(&hand_built_entry(
        1,
        TAG_PLACEHOLDER_MIN - 1,
        VERSION_1,
        &[],
    ))
    .unwrap_err();
    assert_eq!(err, CodecError::UnknownOutcome(TAG_PLACEHOLDER_MIN - 1));
    assert!(err.fatal(), "{err}");
}

#[test]
fn the_encoder_refuses_a_body_past_the_codec_limit() {
    // `QUEEN_RAFT_ENTRY_MAX_BYTES` (96 MiB by default) is a TUNABLE; the
    // codec's own limit is not. Raised past it, an entry used to encode
    // happily into bytes whose length prefix no decoder would believe — a
    // value this codec can write and can never read. It costs a few hundred
    // MiB of RAM for a moment, which is why there is exactly one of it.
    assert!(ENTRY_MAX_BYTES_DEFAULT < MAX_BODY_LEN as usize);
    let mut e = Entry::new(1, 2, 3);
    e.add_command(
        uuid(1),
        Outcome::Empty,
        vec![Effect::FlagSet {
            key: "oversize".into(),
            value: vec![0u8; MAX_BODY_LEN as usize],
        }],
    )
    .unwrap();
    assert_eq!(
        encode_entry(&e),
        Err(CodecError::Layout("entry body above the codec's limit"))
    );
}
