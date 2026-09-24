//! Encode → decode → equal, for every effect kind, every outcome and a whole
//! entry; plus the layout rules of §5.1 and the refusals a decoder owes.

use super::samples::*;
use crate::rsm::effect::*;
use crate::rsm::entry::*;

#[test]
fn every_effect_kind_round_trips() {
    for (kind, eff) in all_effect_samples() {
        assert_eq!(
            eff.kind(),
            kind,
            "{} sample is of another kind",
            kind.name()
        );
        let bytes = encode_effect(&eff);
        let (back, used) = decode_effect(&bytes).unwrap_or_else(|e| {
            panic!("{}: {e}", kind.name());
        });
        assert_eq!(used, bytes.len(), "{}: bytes consumed", kind.name());
        assert_eq!(back, eff, "{}: value changed across the codec", kind.name());
        assert_eq!(bytes.len(), EFFECT_HEADER_LEN + eff.encode_body().len());
    }
}

#[test]
fn kind_ids_are_stable_and_bijective() {
    // The ids are permanent (a retired kind's number is never reused), so this
    // test is a pin, not a tautology: changing one breaks every stored entry.
    let expected: &[(u16, Kind)] = &[
        (0, Kind::Noop),
        (1, Kind::QueueUpsert),
        (2, Kind::QueueDelete),
        (3, Kind::GroupUpsert),
        (4, Kind::GroupDelete),
        (5, Kind::PartitionCreate),
        (6, Kind::PartitionDelete),
        (7, Kind::Append),
        (8, Kind::CursorSet),
        (9, Kind::CursorDelete),
        (10, Kind::DlqInsert),
        (11, Kind::DlqDelete),
        (12, Kind::Watermark),
        (13, Kind::KvPut),
        (14, Kind::KvDelete),
        (15, Kind::TimerUpsert),
        (16, Kind::TimerDelete),
        (17, Kind::TimerBackoff),
        (18, Kind::StreamsQueryUpsert),
        (19, Kind::StreamsStatePut),
        (20, Kind::StreamsStateDelete),
        (21, Kind::TraceAppend),
        (22, Kind::TraceExpire),
        (23, Kind::FlagSet),
        (24, Kind::QuotaSet),
        (25, Kind::EphemeralConfigSet),
        (26, Kind::EphemeralConfigDelete),
        (27, Kind::GarbageAdd),
        (28, Kind::DeleteChunk),
        (29, Kind::RequestIdsExpire),
        (30, Kind::ClusterVersionSet),
        (31, Kind::MembershipNote),
        (32, Kind::TenantPurge),
    ];
    assert_eq!(
        expected.len(),
        Kind::ALL.len(),
        "a kind was added without an id pin"
    );
    for (id, kind) in expected {
        assert_eq!(Kind::from_u16(*id), Some(*kind));
        assert_eq!(*kind as u16, *id, "{}", kind.name());
    }
    assert_eq!(
        Kind::from_u16(expected.len() as u16),
        None,
        "the next id must be free"
    );
    // Names are the golden-fixture file names: they must be unique.
    let mut names: Vec<&str> = Kind::ALL.iter().map(|k| k.name()).collect();
    names.sort_unstable();
    let before = names.len();
    names.dedup();
    assert_eq!(names.len(), before, "two kinds share a name");
}

#[test]
fn every_kind_pins_its_catalogue_version() {
    // `Effect::version` used to be `VERSION_1` for every variant, with no
    // match: a kind added in phase 2 would have reported catalogue version 1
    // by default, `catalogue_version_of` with it, and §12.8's gate would have
    // let it be proposed to a voter that cannot decode it (D20, I16). The
    // match is exhaustive now, so the COMPILER refuses a kind that does not
    // state its version; this table is the other half — it refuses a kind
    // added without a pin, and it is what a version bump has to be written
    // into.
    let expected: &[(Kind, u16)] = &[
        (Kind::Noop, VERSION_1),
        (Kind::QueueUpsert, VERSION_1),
        (Kind::QueueDelete, VERSION_1),
        (Kind::GroupUpsert, VERSION_1),
        (Kind::GroupDelete, VERSION_1),
        (Kind::PartitionCreate, VERSION_1),
        (Kind::PartitionDelete, VERSION_1),
        (Kind::Append, VERSION_1),
        (Kind::CursorSet, VERSION_1),
        (Kind::CursorDelete, VERSION_1),
        (Kind::DlqInsert, VERSION_1),
        (Kind::DlqDelete, VERSION_1),
        (Kind::Watermark, VERSION_1),
        (Kind::KvPut, VERSION_1),
        (Kind::KvDelete, VERSION_1),
        (Kind::TimerUpsert, VERSION_1),
        (Kind::TimerDelete, VERSION_1),
        (Kind::TimerBackoff, VERSION_1),
        (Kind::StreamsQueryUpsert, VERSION_1),
        (Kind::StreamsStatePut, VERSION_1),
        (Kind::StreamsStateDelete, VERSION_1),
        (Kind::TraceAppend, VERSION_1),
        (Kind::TraceExpire, VERSION_1),
        (Kind::FlagSet, VERSION_1),
        (Kind::QuotaSet, VERSION_1),
        (Kind::EphemeralConfigSet, VERSION_1),
        (Kind::EphemeralConfigDelete, VERSION_1),
        (Kind::GarbageAdd, VERSION_1),
        (Kind::DeleteChunk, VERSION_1),
        (Kind::RequestIdsExpire, VERSION_1),
        (Kind::ClusterVersionSet, VERSION_1),
        (Kind::MembershipNote, VERSION_1),
        (Kind::TenantPurge, VERSION_1),
    ];
    assert_eq!(
        expected.len(),
        Kind::ALL.len(),
        "a kind was added without a version pin"
    );
    for (kind, want) in expected {
        let eff = effect_sample(*kind);
        assert_eq!(eff.version(), *want, "{}", kind.name());
        assert!(
            *want as u32 <= SUPPORTED_KINDS_VERSION,
            "{} is minted above what this build supports",
            kind.name()
        );
        // The version really reaches the bytes: `kind | version | len`.
        let bytes = encode_effect(&eff);
        assert_eq!(u16::from_le_bytes([bytes[2], bytes[3]]), *want);
    }
    let mut all: Vec<Effect> = all_effect_samples().into_iter().map(|(_, e)| e).collect();
    // Version 2 is one shape of one kind: a cursor row carrying metadata.
    if let Effect::CursorSet {
        pid,
        group,
        mut row,
    } = effect_sample(Kind::CursorSet)
    {
        row.metadata = "m".into();
        let v2 = Effect::CursorSet { pid, group, row };
        assert_eq!(v2.version(), VERSION_2);
        all.push(v2);
    }
    assert_eq!(kinds_version_of(&all), SUPPORTED_KINDS_VERSION);
}

#[test]
fn every_outcome_round_trips_inside_an_entry() {
    for (i, outcome) in outcome_samples().into_iter().enumerate() {
        let mut e = Entry::new(1_768_000_000_000_000, 1, 1);
        e.add_command(uuid(i as u8), outcome.clone(), vec![Effect::Noop])
            .unwrap();
        let bytes = encode_entry(&e).expect("encode");
        let back = decode_entry(&bytes).expect("decode");
        assert_eq!(back.commands[0].outcome, outcome);
        assert_eq!(back, e);
    }
}

#[test]
fn the_whole_entry_round_trips() {
    let e = entry_sample();
    let bytes = encode_entry(&e).expect("encode");
    let back = decode_entry(&bytes).expect("decode");
    assert_eq!(back, e);
    assert_eq!(back.format, ENTRY_FORMAT);
    assert_eq!(back.now_us, 1_768_000_000_000_042);
    assert_eq!(back.pid_base, 7);
    assert_eq!(back.kv_version_base, 91_005);
    assert_eq!(back.commands.len(), 3);
    assert_eq!(back.effects.len(), 9);
    assert_eq!(e.encoded_len().unwrap(), bytes.len());
    // decode_entry_at reports what it consumed, so a reader can walk a file.
    let mut two = bytes.clone();
    two.extend_from_slice(&bytes);
    let (first, used) = decode_entry_at(&two).expect("first");
    assert_eq!(used, bytes.len());
    assert_eq!(first, e);
    let (second, used2) = decode_entry_at(&two[used..]).expect("second");
    assert_eq!(used2, bytes.len());
    assert_eq!(second, e);
    // …and decode_entry refuses the same buffer, because it must hold one.
    assert!(matches!(decode_entry(&two), Err(CodecError::Field(_))));
}

#[test]
fn commands_span_their_effects() {
    let e = entry_sample();
    let spans: Vec<(u32, u32)> = e
        .commands
        .iter()
        .map(|c| (c.first_effect, c.effect_count))
        .collect();
    assert_eq!(spans, vec![(0, 3), (3, 2), (5, 4)]);
    assert_eq!(e.effects_of(&e.commands[0]).len(), 3);
    assert_eq!(e.effects_of(&e.commands[2])[0].kind(), Kind::DlqInsert);
    e.validate().expect("the built entry is valid");
}

#[test]
fn a_command_with_no_effects_is_refused() {
    // §5.4: commands that plan nothing are never logged — empty pops, refusals,
    // lost CAS, replays. Logging them would make cost follow the poll rate.
    let mut e = Entry::new(1, 1, 1);
    assert_eq!(
        e.add_command(uuid(1), Outcome::Empty, vec![]),
        Err(CodecError::Layout("command with no effects"))
    );
    assert!(e.commands.is_empty());
    assert!(e.effects.is_empty());
}

#[test]
fn kinds_version_tracks_what_the_entry_carries() {
    let mut e = Entry::new(1, 1, 1);
    assert_eq!(e.kinds_version, 0, "an empty entry uses no kind");
    e.add_command(uuid(1), Outcome::Empty, vec![Effect::Noop])
        .unwrap();
    assert_eq!(e.kinds_version, VERSION_1 as u32);
    assert_eq!(kinds_version_of(&e.effects), VERSION_1 as u32);
    assert!(SUPPORTED_KINDS_VERSION >= e.kinds_version);

    // A header that understates what it carries would walk an effect past the
    // cluster-version gate (I16, D20).
    let mut lying = entry_sample();
    lying.kinds_version = 0;
    assert_eq!(
        lying.validate(),
        Err(CodecError::Layout(
            "kinds_version below what the entry carries"
        ))
    );
    // One that overstates is a version this build cannot support: fatal, and
    // fatal is NOT "skip the effect".
    let mut ahead = entry_sample();
    ahead.kinds_version = SUPPORTED_KINDS_VERSION + 1;
    let err = ahead.validate().unwrap_err();
    assert!(err.fatal(), "{err}");
}

#[test]
fn a_broken_layout_is_refused_not_trusted() {
    let base = entry_sample();

    let mut past_the_end = base.clone();
    past_the_end.commands[2].effect_count = 99;
    assert_eq!(
        past_the_end.validate(),
        Err(CodecError::Layout("command span past the effects"))
    );

    let mut overlapping = base.clone();
    overlapping.commands[1].first_effect = 0;
    assert_eq!(
        overlapping.validate(),
        Err(CodecError::Layout("command spans overlap"))
    );

    let mut empty_cmd = base.clone();
    empty_cmd.commands[0].effect_count = 0;
    assert_eq!(
        empty_cmd.validate(),
        Err(CodecError::Layout("command with no effects"))
    );

    let mut bad_format = base.clone();
    bad_format.format = ENTRY_FORMAT + 1;
    assert_eq!(
        bad_format.validate(),
        Err(CodecError::UnknownFormat(ENTRY_FORMAT + 1))
    );
    assert!(bad_format.validate().unwrap_err().fatal());

    // An effect belonging to no command: apply may walk the effects or walk
    // the commands' spans, and an orphan makes those two disagree.
    let mut orphan = base.clone();
    orphan.commands.pop();
    assert_eq!(
        orphan.validate(),
        Err(CodecError::Layout("an effect belongs to no command"))
    );

    // The ENCODER runs validate(), so none of these can be proposed…
    assert_eq!(
        encode_entry(&past_the_end),
        Err(CodecError::Layout("command span past the effects"))
    );
    assert_eq!(
        encode_entry(&overlapping),
        Err(CodecError::Layout("command spans overlap"))
    );
    assert_eq!(
        encode_entry(&orphan),
        Err(CodecError::Layout("an effect belongs to no command"))
    );
    assert_eq!(
        encode_entry(&bad_format),
        Err(CodecError::UnknownFormat(ENTRY_FORMAT + 1))
    );

    // …and the decoder runs it as well, so none can be read back in either.
    // The bytes here are FORGED with the unchecked encoder and are perfectly
    // well formed — a correct length and a correct xxh3 — which is the point:
    // they verified, so they are what a leader wrote, and the answer is fatal
    // rather than "a torn tail" (see `tests/gates.rs`).
    let bytes = encode_entry_unchecked(&past_the_end);
    assert_eq!(
        decode_entry(&bytes),
        Err(CodecError::Malformed("command span past the effects"))
    );
}

#[test]
fn corruption_is_caught_by_the_checksum() {
    // EVERY bit of every byte, not bit 0 of every byte: a codec that survived
    // the low bit of each byte would still be a codec nobody had tested on the
    // high ones, and the high bits are where a length prefix or a tag lives.
    let e = entry_sample();
    let good = encode_entry(&e).expect("encode");
    for i in 0..good.len() {
        for bit in 0..8 {
            let mut bad = good.clone();
            bad[i] ^= 1 << bit;
            assert!(
                decode_entry(&bad).is_err(),
                "byte {i} bit {bit} flipped and the entry still decoded"
            );
        }
    }
}

#[test]
fn a_truncated_entry_is_truncated_not_corrupt() {
    let e = entry_sample();
    let good = encode_entry(&e).expect("encode");
    for cut in 0..good.len() {
        let err = decode_entry(&good[..cut]).unwrap_err();
        assert!(
            matches!(err, CodecError::Truncated | CodecError::Header),
            "cut at {cut}: {err}"
        );
    }
}

#[test]
fn an_unknown_kind_or_version_stops_the_node() {
    // I16: decoding an unknown kind stops the Raft instance on that node (no
    // votes, no acks, no apply), never skips it. The codec's part of that
    // contract is to report it as fatal and to refuse to step over the effect
    // even though its length prefix says how long it is.
    let eff = encode_effect(&Effect::Append {
        pid: 1,
        bucket: 0,
        base_offset: 0,
        count: 0,
        created_at_us: 0,
        hashes: vec![],
        blob: vec![],
    });

    let mut unknown_kind = eff.clone();
    unknown_kind[0] = 0xEE;
    unknown_kind[1] = 0x00;
    let err = decode_effect(&unknown_kind).unwrap_err();
    assert_eq!(err, CodecError::UnknownKind(0x00EE));
    assert!(err.fatal());

    let mut unknown_version = eff.clone();
    unknown_version[2] = 0x09;
    let err = decode_effect(&unknown_version).unwrap_err();
    assert_eq!(
        err,
        CodecError::UnknownVersion {
            kind: Kind::Append as u16,
            version: 9
        }
    );
    assert!(err.fatal());

    // Corruption is not fatal in this sense: it is a torn tail, and recovery
    // (§11.5) decides what to do with it.
    assert!(!CodecError::Checksum.fatal());
    assert!(!CodecError::Truncated.fatal());
    assert!(!CodecError::Field("x").fatal());
    assert!(!CodecError::Layout("x").fatal());
}

#[test]
fn an_appends_hashes_must_have_one_per_frame() {
    // 16 bytes per frame, frame order: the dedup index and ack-by-hash both
    // index into this by ordinal, so a stride that does not match the count is
    // a corrupt effect, not a tolerable one.
    let bad = Effect::Append {
        pid: 7,
        bucket: 1,
        base_offset: 0,
        count: 3,
        created_at_us: 1,
        hashes: vec![0u8; 32],
        blob: vec![],
    };
    let bytes = encode_effect(&bad);
    assert_eq!(
        decode_effect(&bytes),
        Err(CodecError::Field("hashes stride"))
    );
}

#[test]
fn trailing_bytes_inside_a_body_are_refused() {
    // A decoder that stops early on a shorter-than-declared body would let two
    // different byte strings mean one effect, which breaks the golden pins and
    // any digest built over encoded bytes.
    let mut bytes = encode_effect(&Effect::PartitionDelete { pid: 7 });
    let len = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    bytes[4..8].copy_from_slice(&(len + 1).to_le_bytes());
    bytes.push(0x00);
    assert_eq!(
        decode_effect(&bytes),
        Err(CodecError::Field("trailing bytes"))
    );
}

#[test]
fn optional_fields_survive_both_arms() {
    // Every Option in the catalogue, in its other arm: the sample set carries
    // one arm, this carries the other, so no `opt_*` pair can be mismatched.
    let mut row = cursor_row();
    row.batch_end = None;
    row.worker = None;
    row.lease_expires_at_us = None;
    row.lease_acquired_at_us = Some(5);
    row.attempt_offset = Some(9);
    row.delivered = vec![];
    let e = Effect::CursorSet {
        pid: 1,
        group: String::new(),
        row,
    };
    let (back, _) = decode_effect(&encode_effect(&e)).unwrap();
    assert_eq!(back, e);

    let mut cfg = queue_config();
    cfg.namespace = None;
    cfg.task = Some("nightly".into());
    cfg.retention_sink_hold = "lake".into();
    let e = Effect::QueueUpsert {
        tenant: T0.into(),
        queue: "q".into(),
        cfg,
    };
    let (back, _) = decode_effect(&encode_effect(&e)).unwrap();
    assert_eq!(back, e);

    let mut row = timer_row();
    row.visible_at_us = None;
    row.last_error = None;
    row.producer_sub = None;
    let e = Effect::TimerUpsert {
        tenant: T0.into(),
        queue: "q".into(),
        key: "k".into(),
        row,
    };
    let (back, _) = decode_effect(&encode_effect(&e)).unwrap();
    assert_eq!(back, e);

    let e = Effect::DlqInsert {
        dlq_id: uuid(1),
        tenant: T0.into(),
        queue: "q".into(),
        group: "__timer__".into(),
        pid: 7,
        // A timer's dead letter: offset -1, and no message id.
        offset: -1,
        message_id: None,
        txn: String::new(),
        payload: vec![],
        error: String::new(),
        retry_count: 0,
        failed_at_us: -1,
    };
    let (back, _) = decode_effect(&encode_effect(&e)).unwrap();
    assert_eq!(back, e);

    let e = Effect::TraceAppend {
        event: TraceEvent {
            trace_id: uuid(2),
            tenant: T0.into(),
            pid: None,
            message_id: None,
            txn: "t".into(),
            consumer_group: None,
            event_type: "published".into(),
            data: vec![],
            worker: Some("w".into()),
            names: vec![],
            created_at_us: 0,
        },
    };
    let (back, _) = decode_effect(&encode_effect(&e)).unwrap();
    assert_eq!(back, e);

    let e = Effect::GarbageAdd {
        pids: vec![],
        scope: GarbageScope::Tenant,
        deleted_at_us: i64::MIN,
    };
    let (back, _) = decode_effect(&encode_effect(&e)).unwrap();
    assert_eq!(back, e);
}

#[test]
fn a_bad_enum_byte_is_a_field_error() {
    // Booleans and the small enums are validated, not cast: a 0x02 in a bool
    // would otherwise make two byte strings mean one value.
    let bytes = encode_effect(&effect_sample(Kind::GroupUpsert));
    let mut guarded = false;
    for i in 0..bytes.len() {
        let mut bad = bytes.clone();
        bad[i] = 0x7F;
        if decode_effect(&bad) == Err(CodecError::Field("mode")) {
            guarded = true;
        }
    }
    assert!(guarded, "no byte position validates the subscription mode");

    let bytes = encode_effect(&Effect::QuotaSet {
        kind: QuotaKind::Streams,
        tenant: T0.into(),
        grant: QuotaGrant::default(),
    });
    let mut bad = bytes.clone();
    bad[EFFECT_HEADER_LEN] = 0x7F; // the quota kind is the first body byte
    assert_eq!(decode_effect(&bad), Err(CodecError::Field("quota kind")));
}

#[test]
fn every_field_has_its_own_slot() {
    // The one defect class the golden fixtures cannot catch: two fields of the
    // SAME type swapped between the encoder and the decoder. Golden bytes are
    // produced by the encoder under test, so a transposition is golden too, and
    // a sample where several columns are 0 (a fresh queue has eight of them)
    // round-trips happily through a swap.
    //
    // So: give every field of every multi-field row a value no other field in
    // that row has, and round-trip it. A swap then changes the value. Every
    // kind and every outcome with more than one field is below — the ones left
    // out the first time (the deletes, PartitionCreate, TimerBackoff, the
    // streams and ephemeral rows, DeleteChunk, GarbageAdd, and the Push,
    // Renew and DlqHead outcomes) are what let an `offset`/`committed` swap
    // hide behind two equal 41s.
    let cfg = QueueConfig {
        id: uuid(0x11),
        namespace: Some("ns-1".into()),
        task: Some("task-2".into()),
        priority: 3,
        lease_time: 4,
        retry_limit: 5,
        retry_delay: 6,
        ttl: 7,
        dead_letter_queue: true,
        dlq_after_max_retries: false,
        delayed_processing: 8,
        window_buffer: 9,
        retention_seconds: 10,
        completed_retention_seconds: 11,
        retention_enabled: false,
        encryption_enabled: true,
        max_wait_time_seconds: 12,
        max_queue_size: 13,
        min_pop_wait_time: 14,
        dedup_window_seconds: 15,
        retention_sink_hold: "sink-16".into(),
        retention_sink_hold_max_seconds: 17,
        created_at_us: 18,
    };
    let e = Effect::QueueUpsert {
        tenant: "t-19".into(),
        queue: "q-20".into(),
        cfg,
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let row = CursorRow {
        committed: 1,
        batch_end: Some(2),
        worker: Some("w-3".into()),
        lease_expires_at_us: Some(4),
        lease_acquired_at_us: Some(5),
        batch_retry_count: 6,
        attempt_offset: Some(7),
        attempt_count: 8,
        total_consumed: 9,
        lease_conflated: true,
        delivered: vec![uuid(10)],
        created_at_us: 11,
        metadata: String::new(),
    };
    let e = Effect::CursorSet {
        pid: 12,
        group: "g-13".into(),
        row,
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let row = TimerRow {
        partition: "p-1".into(),
        deliver_at_us: 2,
        visible_at_us: Some(3),
        frame: vec![4],
        payload_zstd: true,
        encrypted: false,
        txn: "txn-5".into(),
        message_id: uuid(6),
        attempts: 7,
        last_error: Some("err-8".into()),
        producer_sub: Some("sub-9".into()),
        created_at_us: 10,
        updated_at_us: 11,
    };
    let e = Effect::TimerUpsert {
        tenant: "t-12".into(),
        queue: "q-13".into(),
        key: "k-14".into(),
        row,
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::QuotaSet {
        kind: QuotaKind::Ephemeral,
        tenant: "t-1".into(),
        grant: QuotaGrant {
            enabled: true,
            max_rows: Some(2),
            max_bytes: Some(3),
            max_timers: Some(4),
            max_timer_horizon_s: Some(5),
            max_reads_per_sec: Some(6),
            max_writes_per_sec: Some(7),
            max_queues: Some(8),
            max_msgs_per_sec: Some(9),
            max_queries: Some(10),
            updated_at_us: 11,
        },
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::KvPut {
        tenant: "t-1".into(),
        ns: "ns-2".into(),
        key: "k-3".into(),
        value: vec![4],
        version: 5,
        expires_at_us: Some(6),
        created_at_us: 7,
        updated_at_us: 8,
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::DlqInsert {
        dlq_id: uuid(1),
        tenant: "t-2".into(),
        queue: "q-3".into(),
        pid: 4,
        group: "g-5".into(),
        offset: 6,
        message_id: Some(uuid(7)),
        txn: "txn-8".into(),
        payload: vec![9],
        error: "e-10".into(),
        retry_count: 11,
        failed_at_us: 12,
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::TraceAppend {
        event: TraceEvent {
            trace_id: uuid(1),
            tenant: "t-2".into(),
            pid: Some(3),
            message_id: Some(uuid(4)),
            txn: "txn-5".into(),
            consumer_group: Some("g-6".into()),
            event_type: "e-7".into(),
            data: vec![8],
            worker: Some("w-9".into()),
            names: vec!["n-10".into()],
            created_at_us: 11,
        },
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::StreamsQueryUpsert {
        query_id: uuid(1),
        tenant: "t-2".into(),
        row: StreamsQueryRow {
            name: "n-3".into(),
            source_queue: "src-4".into(),
            sink_queue: Some("sink-5".into()),
            config_hash: "h-6".into(),
            created_at_us: 7,
            updated_at_us: 8,
        },
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::GroupUpsert {
        tenant: "t-1".into(),
        queue: "q-2".into(),
        group: "g-3".into(),
        meta: GroupMeta {
            id: uuid(4),
            partition_name: "p-5".into(),
            namespace: "ns-6".into(),
            task: "task-7".into(),
            mode: SubscriptionMode::New,
            subscription_timestamp_us: 8,
            conflation: true,
            seeded: false,
            registered_at_us: 9,
        },
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::MembershipNote {
        node_id: 1,
        generation: 2,
        disk_uuid: uuid(3),
        address: "a-4".into(),
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::Watermark {
        pid: 1,
        log_start: 2,
        txns_start: 3,
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::Append {
        pid: 1,
        bucket: 2,
        base_offset: 3,
        count: 1,
        created_at_us: 4,
        hashes: uuid(5).to_vec(),
        blob: vec![6],
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    // The outcomes, on the same terms.
    let o = Outcome::Pop(PopOutcome {
        claims: vec![PopClaim {
            pid: 1,
            start_offset: 2,
            end_offset: 3,
            worker: "w-4".into(),
            lease_expires_at_us: Some(5),
            delivery_attempt: 6,
            conflated: true,
        }],
    });
    let mut e = Entry::new(1, 2, 3);
    e.add_command(uuid(1), o, vec![Effect::Noop]).unwrap();
    assert_eq!(decode_entry(&encode_entry(&e).unwrap()).unwrap(), e);

    let o = Outcome::Ack(AckOutcome {
        results: vec![AckResult {
            pid: 1,
            committed: 2,
            acked: 3,
            conflated: 4,
            dlq: 5,
            lease_released: true,
            batch_retry_count: 6,
            noop_hashes: vec![uuid(7)],
            stale_hashes: vec![uuid(8), uuid(9)],
        }],
    });
    let mut e = Entry::new(4, 5, 6);
    e.add_command(uuid(2), o, vec![Effect::Noop]).unwrap();
    assert_eq!(decode_entry(&encode_entry(&e).unwrap()).unwrap(), e);

    let o = Outcome::Push(PushOutcome {
        items: vec![
            PushVerdict::Created {
                pid: 1,
                offset: 2,
                created_at_us: 3,
            },
            PushVerdict::Duplicate { pid: 4, offset: 5 },
            PushVerdict::Refused {
                code: "c-6".into(),
                message: "m-7".into(),
            },
        ],
    });
    let mut e = Entry::new(7, 8, 9);
    e.add_command(uuid(3), o, vec![Effect::Noop]).unwrap();
    assert_eq!(decode_entry(&encode_entry(&e).unwrap()).unwrap(), e);

    // `offset` and `committed` are adjacent i64 columns: 1 and 2, never the
    // same number, or their transposition is invisible here AND in the golden
    // bytes (which the encoder under test produced).
    let o = Outcome::DlqHead(DlqHeadOutcome {
        pid: 1,
        dlq_id: uuid(2),
        offset: 3,
        committed: 4,
        lease_released: true,
    });
    let mut e = Entry::new(10, 11, 12);
    e.add_command(uuid(4), o, vec![Effect::Noop]).unwrap();
    assert_eq!(decode_entry(&encode_entry(&e).unwrap()).unwrap(), e);

    let o = Outcome::Renew(RenewOutcome {
        renewed: 1,
        min_expires_at_us: Some(2),
    });
    let mut e = Entry::new(13, 14, 15);
    e.add_command(uuid(5), o, vec![Effect::Noop]).unwrap();
    assert_eq!(decode_entry(&encode_entry(&e).unwrap()).unwrap(), e);

    // And the effect kinds the first round of this test left out.
    let e = Effect::PartitionCreate {
        pid: 1,
        uuid: uuid(2),
        tenant: "t-3".into(),
        queue: "q-4".into(),
        partition: "p-5".into(),
        created_at_us: 6,
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::TimerBackoff {
        tenant: "t-1".into(),
        queue: "q-2".into(),
        key: "k-3".into(),
        visible_at_us: 4,
        attempts: 5,
        last_error: Some("e-6".into()),
        updated_at_us: 7,
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::StreamsStatePut {
        query_id: uuid(1),
        pid: 2,
        key: "k-3".into(),
        value: vec![4],
        updated_at_us: 5,
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::StreamsStateDelete {
        query_id: uuid(1),
        pid: 2,
        key: "k-3".into(),
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::EphemeralConfigSet {
        tenant: "t-1".into(),
        queue: "q-2".into(),
        options: vec![3],
        updated_at_us: 4,
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::DeleteChunk {
        pids: vec![1, 2],
        scope: GarbageScope::Group {
            group: "g-3".into(),
        },
        resume: vec![4],
        limit: 5,
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::GarbageAdd {
        pids: vec![1, 2],
        scope: GarbageScope::Group {
            group: "g-3".into(),
        },
        deleted_at_us: 4,
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::KvDelete {
        tenant: "t-1".into(),
        ns: "ns-2".into(),
        key: "k-3".into(),
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::CursorDelete {
        pid: 1,
        group: "g-2".into(),
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::DlqDelete {
        dlq_id: uuid(1),
        tenant: "t-2".into(),
        queue: "q-3".into(),
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::TimerDelete {
        tenant: "t-1".into(),
        queue: "q-2".into(),
        key: "k-3".into(),
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::FlagSet {
        key: "k-1".into(),
        value: vec![2],
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    // The all-string deletes: `tenant` and `queue` are the same type and next
    // to each other, which is the easiest pair in the catalogue to transpose.
    let e = Effect::QueueDelete {
        tenant: "t-1".into(),
        queue: "q-2".into(),
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::GroupDelete {
        tenant: "t-1".into(),
        queue: "q-2".into(),
        group: "g-3".into(),
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);

    let e = Effect::EphemeralConfigDelete {
        tenant: "t-1".into(),
        queue: "q-2".into(),
    };
    assert_eq!(decode_effect(&encode_effect(&e)).unwrap().0, e);
}

/// A3b (`ALICE_PGLESS_NEWARCH.md` §5): the log-native reference encoding
/// [`encode_entry_payload_free`] drops every `Append`'s PAYLOAD and replaces it
/// with the payload's 4-byte FRAME LENGTH — nothing else changes. It is
/// byte-identical to `encode_entry` of the same entry whose `Append`s were built
/// with that 4-byte length as their blob — so the writer's raft-log bytes and the
/// codec cannot drift — and it decodes back to exactly that length-carrying
/// entry, which is what a replay hands apply (so `RetainedBytes` is computed from
/// the same length live and on replay: replay-stable, I2). The payload itself is
/// gone from the entry (it lives once, in the qlog).
#[test]
fn payload_free_entry_carries_the_frame_length_and_matches_its_length_blob_twin() {
    // Two frames, so the hash list is a non-trivial 32 bytes that MUST survive.
    let hashes = [uuid(5).to_vec(), uuid(6).to_vec()].concat();
    let payload = vec![10u8, 11, 12, 13, 14, 15, 16, 17]; // 8 bytes
    let with_blob = Effect::Append {
        pid: 7,
        bucket: 2,
        base_offset: 3,
        count: 2,
        created_at_us: 4,
        hashes: hashes.clone(),
        blob: payload.clone(),
    };
    // The frame length the payload-free entry carries in place of the payload:
    // frame::encoded_len(count, payload_len) — the segment's own pos.len.
    let frame_len = crate::rsm::segments::frame::encoded_len(2, payload.len()) as u32;
    let length_blob = Effect::Append {
        pid: 7,
        bucket: 2,
        base_offset: 3,
        count: 2,
        created_at_us: 4,
        hashes,
        blob: frame_len.to_le_bytes().to_vec(), // the 4-byte length, not the payload
    };
    // A non-Append effect alongside it: the payload-free path must leave every
    // other effect (and the header, commands and outcomes) untouched.
    let mut with = Entry::new(100, 0, 0);
    with.add_command(uuid(1), Outcome::Empty, vec![Effect::Noop, with_blob])
        .unwrap();
    let mut length = Entry::new(100, 0, 0);
    length
        .add_command(
            uuid(1),
            Outcome::Empty,
            vec![Effect::Noop, length_blob.clone()],
        )
        .unwrap();

    let pf = encode_entry_payload_free(&with).unwrap();
    // 1. byte-identical to encoding the length-blob entry (no drift).
    assert_eq!(
        pf,
        encode_entry(&length).unwrap(),
        "the payload-free encoding drifted from the length-blob entry"
    );
    // 2. it decodes to exactly the length-blob entry (what a replay hands apply).
    assert_eq!(decode_entry(&pf).unwrap(), length);
    // 3. the recovered Append carries the SAME metadata and the 4-byte length,
    //    NOT the payload.
    let Effect::Append {
        pid,
        base_offset,
        count,
        hashes: rec_hashes,
        blob,
        ..
    } = &decode_entry(&pf).unwrap().effects[1]
    else {
        panic!("second effect is not an Append");
    };
    assert_eq!((*pid, *base_offset, *count), (7, 3, 2));
    assert_eq!(rec_hashes.len(), 32, "the hash list survives");
    assert_eq!(
        blob.len(),
        4,
        "the entry carries the 4-byte frame length, not the payload"
    );
    assert_eq!(
        u32::from_le_bytes(blob[..4].try_into().unwrap()),
        frame_len,
        "the carried length is the frame length"
    );
    assert_ne!(*blob, payload, "the payload bytes are NOT in the entry");
    // 4. it is smaller than the with-blob encoding (4-byte length < 8-byte payload).
    assert!(pf.len() < encode_entry(&with).unwrap().len());
}
