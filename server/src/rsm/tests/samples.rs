//! The canonical sample of every effect kind, of every outcome, and of one
//! whole entry.
//!
//! These values ARE the golden fixtures: `golden/<name>.bin` holds the bytes
//! each one encodes to. Changing a sample changes a fixture, which is a format
//! change and needs a kind version bump (§5.3) — that is exactly what the
//! golden test is for.
//!
//! The values are deliberately awkward: negative times and offsets where the
//! SQL allows them (`committed = -1`, a timer's DLQ `offset = -1`), both arms
//! of every `Option`, a multi-frame `Append`, non-ASCII text, an empty string
//! and an empty blob. A codec that only ever sees tidy values is not pinned.

use crate::rsm::effect::*;
use crate::rsm::entry::*;

/// The default tenant every pre-tenancy caller lands in.
pub const T0: &str = "00000000-0000-0000-0000-000000000001";

pub fn uuid(n: u8) -> [u8; 16] {
    let mut b = [0u8; 16];
    for (i, slot) in b.iter_mut().enumerate() {
        *slot = n.wrapping_add(i as u8);
    }
    b
}

pub fn queue_config() -> QueueConfig {
    QueueConfig {
        id: uuid(0x10),
        namespace: Some("orders".into()),
        task: None,
        priority: 0,
        // 60, not 300: what an implicitly push-created queue has always leased.
        lease_time: 60,
        retry_limit: 3,
        retry_delay: 1000,
        ttl: 3600,
        dead_letter_queue: true,
        dlq_after_max_retries: true,
        delayed_processing: 0,
        window_buffer: 0,
        retention_seconds: 0,
        completed_retention_seconds: 0,
        retention_enabled: false,
        encryption_enabled: false,
        max_wait_time_seconds: 0,
        max_queue_size: 0,
        min_pop_wait_time: 0,
        dedup_window_seconds: 3600,
        retention_sink_hold: String::new(),
        retention_sink_hold_max_seconds: 604_800,
        created_at_us: 1_768_000_000_000_000,
    }
}

pub fn cursor_row() -> CursorRow {
    CursorRow {
        committed: -1,
        batch_end: Some(41),
        worker: Some("worker-7".into()),
        lease_expires_at_us: Some(1_768_000_060_000_000),
        lease_acquired_at_us: None,
        batch_retry_count: 2,
        attempt_offset: None,
        attempt_count: 1,
        total_consumed: 3,
        lease_conflated: true,
        delivered: vec![uuid(0x40), uuid(0x50)],
        // Older than every other stamp in this row: the cursor was created at
        // first contact, the lease was taken later. `log_partition_dead_v1`
        // compares exactly this value with the cleanup cutoff (006 ≈623).
        created_at_us: 1_767_999_000_000_000,
        metadata: String::new(),
    }
}

pub fn timer_row() -> TimerRow {
    TimerRow {
        partition: "Default".into(),
        deliver_at_us: 1_768_000_900_000_000,
        visible_at_us: Some(1_768_000_905_000_000),
        frame: vec![0x28, 0xb5, 0x2f, 0xfd, 0x00],
        payload_zstd: true,
        encrypted: false,
        txn: "timer-txn-1".into(),
        message_id: uuid(0x60),
        attempts: 2,
        last_error: Some("connection refused".into()),
        producer_sub: Some("svc-scheduler".into()),
        created_at_us: 1_768_000_000_000_000,
        updated_at_us: 1_768_000_300_000_000,
    }
}

/// The canonical value of one kind. The match is exhaustive, so a kind added
/// to the catalogue without a sample does not compile — and therefore cannot
/// ship without a golden fixture.
pub fn effect_sample(kind: Kind) -> Effect {
    match kind {
        Kind::Noop => Effect::Noop,

        Kind::QueueUpsert => Effect::QueueUpsert {
            tenant: T0.into(),
            queue: "orders".into(),
            cfg: queue_config(),
        },
        Kind::QueueDelete => Effect::QueueDelete {
            tenant: T0.into(),
            queue: "orders".into(),
        },

        Kind::GroupUpsert => Effect::GroupUpsert {
            tenant: T0.into(),
            queue: "orders".into(),
            group: "billing".into(),
            meta: GroupMeta {
                id: uuid(0x20),
                partition_name: String::new(),
                namespace: String::new(),
                task: String::new(),
                mode: SubscriptionMode::Timestamp,
                subscription_timestamp_us: 1_767_999_000_000_000,
                conflation: false,
                seeded: true,
                registered_at_us: 1_768_000_000_000_000,
            },
        },
        Kind::GroupDelete => Effect::GroupDelete {
            tenant: T0.into(),
            queue: "orders".into(),
            group: "__QUEUE_MODE__".into(),
        },

        Kind::PartitionCreate => Effect::PartitionCreate {
            pid: 7,
            uuid: uuid(0x30),
            tenant: T0.into(),
            queue: "orders".into(),
            partition: "Default".into(),
            created_at_us: 1_768_000_000_000_000,
        },
        Kind::PartitionDelete => Effect::PartitionDelete { pid: 7 },

        Kind::Append => Effect::Append {
            pid: 7,
            bucket: 129,
            base_offset: 42,
            count: 2,
            created_at_us: 1_768_000_000_000_001,
            hashes: uuid(0x40)
                .iter()
                .chain(uuid(0x50).iter())
                .copied()
                .collect(),
            blob: vec![0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x07, 0x39, 0x00, 0x00],
        },

        Kind::CursorSet => Effect::CursorSet {
            pid: 7,
            group: "billing".into(),
            row: cursor_row(),
        },
        Kind::CursorDelete => Effect::CursorDelete {
            pid: 7,
            group: "__QUEUE_MODE__".into(),
        },

        Kind::DlqInsert => Effect::DlqInsert {
            dlq_id: uuid(0x70),
            tenant: T0.into(),
            queue: "orders".into(),
            group: "billing".into(),
            pid: 7,
            offset: 41,
            message_id: Some(uuid(0x80)),
            txn: "txn-41".into(),
            // UTF-8 inside a blob field: \xc3\xa8 is "è". A payload is bytes,
            // not a string, and must survive without validation.
            payload: b"{\"order\":\"\xc3\xa8\"}".to_vec(),
            error: "Retries exhausted".into(),
            retry_count: 3,
            failed_at_us: 1_768_000_120_000_000,
        },
        Kind::DlqDelete => Effect::DlqDelete {
            dlq_id: uuid(0x70),
            tenant: T0.into(),
            queue: "orders".into(),
        },

        Kind::Watermark => Effect::Watermark {
            pid: 7,
            log_start: 40,
            txns_start: 16,
        },

        Kind::KvPut => Effect::KvPut {
            tenant: T0.into(),
            ns: "qk".into(),
            key: "node:kafka-0".into(),
            value: b"null".to_vec(),
            version: 91_005,
            expires_at_us: Some(1_768_000_030_000_000),
            created_at_us: 1_768_000_000_000_000,
            updated_at_us: 1_768_000_000_000_000,
        },
        Kind::KvDelete => Effect::KvDelete {
            tenant: T0.into(),
            ns: "qk".into(),
            key: "node:kafka-0".into(),
        },

        Kind::TimerUpsert => Effect::TimerUpsert {
            tenant: T0.into(),
            queue: "reminders".into(),
            key: "user/42/nudge".into(),
            row: timer_row(),
        },
        Kind::TimerDelete => Effect::TimerDelete {
            tenant: T0.into(),
            queue: "reminders".into(),
            key: "user/42/nudge".into(),
        },
        Kind::TimerBackoff => Effect::TimerBackoff {
            tenant: T0.into(),
            queue: "reminders".into(),
            key: "user/42/nudge".into(),
            visible_at_us: 1_768_000_930_000_000,
            attempts: 3,
            last_error: None,
            updated_at_us: 1_768_000_910_000_000,
        },

        Kind::StreamsQueryUpsert => Effect::StreamsQueryUpsert {
            query_id: uuid(0x90),
            tenant: T0.into(),
            row: StreamsQueryRow {
                name: "orders-per-minute".into(),
                source_queue: "orders".into(),
                sink_queue: Some("orders-agg".into()),
                config_hash: "b1946ac92492d234".into(),
                created_at_us: 1_768_000_000_000_000,
                updated_at_us: 1_768_000_060_000_000,
            },
        },
        Kind::StreamsStatePut => Effect::StreamsStatePut {
            query_id: uuid(0x90),
            pid: 7,
            key: "2026-09-18T07:00".into(),
            value: br#"{"count":12}"#.to_vec(),
            updated_at_us: 1_768_000_060_000_000,
        },
        Kind::StreamsStateDelete => Effect::StreamsStateDelete {
            query_id: uuid(0x90),
            pid: 7,
            key: "2026-09-18T07:00".into(),
        },

        Kind::TraceAppend => Effect::TraceAppend {
            event: TraceEvent {
                trace_id: uuid(0xa0),
                tenant: T0.into(),
                pid: Some(7),
                message_id: Some(uuid(0x80)),
                txn: "txn-41".into(),
                consumer_group: Some("billing".into()),
                event_type: "consumed".into(),
                data: br#"{"latencyMs":18}"#.to_vec(),
                worker: None,
                names: vec!["checkout".into(), "città".into()],
                created_at_us: 1_768_000_120_000_000,
            },
        },
        Kind::TraceExpire => Effect::TraceExpire {
            cutoff_us: 1_767_395_200_000_000,
        },

        Kind::FlagSet => Effect::FlagSet {
            key: "maintenance_mode".into(),
            value: br#"{"level":2}"#.to_vec(),
        },

        Kind::QuotaSet => Effect::QuotaSet {
            kind: QuotaKind::Kv,
            tenant: T0.into(),
            grant: QuotaGrant {
                enabled: true,
                max_rows: Some(1_000_000),
                max_bytes: Some(1_073_741_824),
                max_timers: Some(1000),
                max_timer_horizon_s: None,
                max_reads_per_sec: Some(5000),
                max_writes_per_sec: None,
                max_queues: None,
                max_msgs_per_sec: None,
                max_queries: None,
                updated_at_us: 1_768_000_000_000_000,
            },
        },

        Kind::EphemeralConfigSet => Effect::EphemeralConfigSet {
            tenant: T0.into(),
            queue: "presence".into(),
            options: br#"{"maxBytes":1048576}"#.to_vec(),
            updated_at_us: 1_768_000_000_000_000,
        },
        Kind::EphemeralConfigDelete => Effect::EphemeralConfigDelete {
            tenant: T0.into(),
            queue: "presence".into(),
        },

        Kind::GarbageAdd => Effect::GarbageAdd {
            pids: vec![7, 8, 9],
            scope: GarbageScope::Queue,
            deleted_at_us: 1_768_000_200_000_000,
        },
        Kind::DeleteChunk => Effect::DeleteChunk {
            pids: vec![7, 8, 9],
            scope: GarbageScope::Group {
                group: "billing".into(),
            },
            resume: vec![0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            limit: 1000,
        },

        Kind::RequestIdsExpire => Effect::RequestIdsExpire {
            cutoff_us: 1_767_999_400_000_000,
        },
        Kind::ClusterVersionSet => Effect::ClusterVersionSet { version: 1 },
        Kind::MembershipNote => Effect::MembershipNote {
            node_id: 2,
            generation: 4,
            disk_uuid: uuid(0xb0),
            address: "queen-mq-2.queen-mq:6634".into(),
        },
        Kind::TenantPurge => Effect::TenantPurge { tenant: T0.into() },
    }
}

/// Every kind's sample, in id order.
pub fn all_effect_samples() -> Vec<(Kind, Effect)> {
    Kind::ALL.iter().map(|k| (*k, effect_sample(*k))).collect()
}

/// The canonical outcome of every variant, for the round-trip test.
pub fn outcome_samples() -> Vec<Outcome> {
    vec![
        Outcome::Empty,
        Outcome::Push(PushOutcome {
            items: vec![
                PushVerdict::Created {
                    pid: 7,
                    offset: 42,
                    created_at_us: 1_768_000_000_000_001,
                },
                PushVerdict::Duplicate { pid: 7, offset: 11 },
                PushVerdict::Refused {
                    code: "queue_full".into(),
                    message: "max_queue_size reached".into(),
                },
            ],
        }),
        Outcome::Pop(PopOutcome {
            claims: vec![
                PopClaim {
                    pid: 7,
                    start_offset: 42,
                    end_offset: 43,
                    worker: "worker-7".into(),
                    lease_expires_at_us: Some(1_768_000_060_000_000),
                    delivery_attempt: 1,
                    conflated: false,
                },
                PopClaim {
                    pid: 8,
                    start_offset: 0,
                    end_offset: 0,
                    worker: String::new(),
                    lease_expires_at_us: None,
                    delivery_attempt: 2,
                    conflated: true,
                },
            ],
        }),
        Outcome::Ack(AckOutcome {
            results: vec![AckResult {
                pid: 7,
                committed: 43,
                acked: 2,
                conflated: 0,
                dlq: 1,
                lease_released: true,
                batch_retry_count: 0,
                noop_hashes: vec![uuid(0x40)],
                stale_hashes: vec![],
            }],
        }),
        Outcome::Renew(RenewOutcome {
            renewed: 3,
            min_expires_at_us: Some(1_768_000_090_000_000),
        }),
        Outcome::DlqHead(DlqHeadOutcome {
            pid: 7,
            dlq_id: uuid(0x70),
            // `offset` and `committed` are adjacent i64 columns: give them
            // DIFFERENT values, or a swap of exactly those two between the
            // encoder and the decoder is invisible to the round trip and to
            // the golden bytes alike (the fixture is produced by the encoder
            // under test, so a transposition is golden too). The cursor
            // advances PAST the filed frame, so 42 is also the true shape.
            offset: 41,
            committed: 42,
            lease_released: true,
        }),
        Outcome::Placeholder(
            Placeholder::new(0xF001, VERSION_1, b"kv:v=91005".to_vec())
                .expect("a reserved tag at a version this build knows"),
        ),
    ]
}

/// One whole entry of the message path: a push that created a queue, a
/// partition and a segment; a pop that claimed it; an ack that filed one dead
/// letter. Three commands, nine effects, every header field non-trivial.
pub fn entry_sample() -> Entry {
    let mut e = Entry::new(1_768_000_000_000_042, 7, 91_005);

    e.add_command(
        uuid(0xc0),
        Outcome::Push(PushOutcome {
            items: vec![
                PushVerdict::Created {
                    pid: 7,
                    offset: 42,
                    created_at_us: 1_768_000_000_000_001,
                },
                PushVerdict::Duplicate { pid: 7, offset: 11 },
            ],
        }),
        vec![
            effect_sample(Kind::QueueUpsert),
            effect_sample(Kind::PartitionCreate),
            effect_sample(Kind::Append),
        ],
    )
    .expect("push plans effects");

    e.add_command(
        uuid(0xc1),
        Outcome::Pop(PopOutcome {
            claims: vec![PopClaim {
                pid: 7,
                start_offset: 42,
                end_offset: 43,
                worker: "worker-7".into(),
                lease_expires_at_us: Some(1_768_000_060_000_000),
                delivery_attempt: 1,
                conflated: false,
            }],
        }),
        vec![
            effect_sample(Kind::GroupUpsert),
            effect_sample(Kind::CursorSet),
        ],
    )
    .expect("pop plans effects");

    e.add_command(
        uuid(0xc2),
        Outcome::Ack(AckOutcome {
            results: vec![AckResult {
                pid: 7,
                committed: 43,
                acked: 2,
                conflated: 0,
                dlq: 1,
                lease_released: true,
                batch_retry_count: 0,
                noop_hashes: vec![],
                stale_hashes: vec![uuid(0x50)],
            }],
        }),
        vec![
            effect_sample(Kind::DlqInsert),
            effect_sample(Kind::CursorSet),
            effect_sample(Kind::Watermark),
            effect_sample(Kind::RequestIdsExpire),
        ],
    )
    .expect("ack plans effects");

    e
}
