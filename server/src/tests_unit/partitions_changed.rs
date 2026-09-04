//! `POST /api/v1/partitions/changed` — the database-free half (PLAN_S3_SINK.md §5.1).
//!
//! Wired from `src/handlers/partitions.rs`; see `src/tests_unit/README.md` for
//! the mechanism. Everything here runs without a database: what is under test
//! is the edge — which requests the broker refuses, which numbers it clamps
//! instead of refusing, and whether the JSON the SQL composes still means to a
//! client what the broker meant by it.
//!
//! What is at stake, in the order it would hurt:
//!
//!  1. **The ceilings.** They are the only thing between one call and 64 000
//!     rows, and the entry count is the one that REJECTS — a silently dropped
//!     entry leaves a sink waiting for a queue the broker never looked at.
//!  2. **The clamp direction.** `limit` is clamped, never rejected, and a
//!     clamp to 0 would be an endpoint that answers nothing for ever.
//!  3. **The safeTime floor.** It is a duration in milliseconds in the
//!     environment and whole seconds on the wire, and rounding it DOWN would
//!     shorten the one bound that makes a window safe to commit.
//!  4. **The conformance.** The body is composed by SQL and passed through
//!     untouched, so nothing in Rust would notice a field renamed on one side.
//!     The types in `queen-protocol` are the other side, and this is where the
//!     two are made to meet.

use super::*;

// ------------------------------------------------------------------ ceilings

fn entry(queue: &str) -> ChangedEntry {
    ChangedEntry {
        queue: queue.to_string(),
        since: None,
        after: None,
        limit: None,
    }
}

#[test]
fn a_batch_over_the_entry_ceiling_is_refused_whole() {
    // 64 exactly is fine...
    let ok: Vec<ChangedEntry> = (0..MAX_ENTRIES).map(|i| entry(&format!("q{i}"))).collect();
    assert!(bind(&ok).is_ok());

    // ...65 is not, and the message names both numbers so a caller can fix it
    // without reading this file.
    let over: Vec<ChangedEntry> = (0..MAX_ENTRIES + 1)
        .map(|i| entry(&format!("q{i}")))
        .collect();
    let err = bind(&over).expect_err("65 entries must be refused");
    assert!(err.contains("65"), "{err}");
    assert!(err.contains("64"), "{err}");
}

#[test]
fn an_empty_batch_is_legal_because_the_watermark_is_the_answer() {
    // The watermark-only request: a reader whose queues are all idle still
    // needs `safeTime` to close the window it is holding. Refusing it would
    // force a second route that answers only that.
    let b = bind(&[]).expect("an empty batch is a request for safeTime alone");
    assert!(b.queues.is_empty());
    assert!(b.since.is_empty());
    assert!(b.after.is_empty());
    assert!(b.limits.is_empty());
}

#[test]
fn an_empty_queue_name_is_refused_rather_than_answered_unknown() {
    // It could never resolve, so passing it through would come back
    // UNKNOWN_TOPIC_OR_PARTITION and read as "your queue was deleted" instead
    // of "you sent an empty string".
    let err = bind(&[entry("orders"), entry("")]).expect_err("empty queue name");
    assert!(err.contains("entry 1"), "{err}");
}

#[test]
fn the_limit_is_clamped_at_both_ends_and_never_rejected() {
    let mut e = entry("orders");
    for (asked, want) in [
        (None, DEFAULT_LIMIT),
        (Some(0), 1),
        (Some(-5), 1),
        (Some(1), 1),
        (Some(999), 999),
        (Some(1000), 1000),
        (Some(1001), MAX_LIMIT),
        // A caller that sends something absurd gets the ceiling, not a 400:
        // the answer's `next` is what teaches it the real bound.
        (Some(i64::MAX), MAX_LIMIT),
        (Some(i64::MIN), 1),
    ] {
        e.limit = asked;
        let b = bind(std::slice::from_ref(&e)).expect("a limit is never a 400");
        assert_eq!(b.limits[0], want, "limit {asked:?}");
    }
}

#[test]
fn the_four_arrays_stay_index_aligned_with_the_request() {
    // Misalignment is what the SQL raises QPARTS on, and the reason it can is
    // that this function is the only thing that builds the arrays. A `since`
    // or `after` that is absent must occupy its slot as a NULL, not vanish.
    let e = vec![
        entry("a"),
        ChangedEntry {
            queue: "b".into(),
            since: Some("2026-09-04T10:00:00.000000Z".into()),
            after: None,
            limit: Some(10),
        },
        ChangedEntry {
            queue: "c".into(),
            since: None,
            after: Some("n|cust-0002".into()),
            limit: None,
        },
    ];
    let b = bind(&e).unwrap();
    assert_eq!(b.queues, vec!["a", "b", "c"]);
    assert_eq!(b.since.len(), 3);
    assert_eq!(b.after.len(), 3);
    assert_eq!(b.limits.len(), 3);
    assert_eq!(b.since[0], None);
    assert_eq!(b.since[1].as_deref(), Some("2026-09-04T10:00:00.000000Z"));
    assert_eq!(b.since[2], None);
    assert_eq!(b.after[2].as_deref(), Some("n|cust-0002"));
    assert_eq!(b.limits, vec![DEFAULT_LIMIT, 10, DEFAULT_LIMIT]);
}

#[test]
fn a_since_is_handed_to_postgres_verbatim() {
    // This handler deliberately does not parse timestamps: PostgreSQL is the
    // one parser for every timestamp on this wire (see `db::log_partitions_changed`),
    // and a second one here could accept or reject a spelling the rest of the
    // API does not. So even a string that is obviously not a timestamp travels
    // — and comes back as the SQLSTATE 22007 the handler answers 400 for.
    let e = ChangedEntry {
        queue: "orders".into(),
        since: Some("not a timestamp".into()),
        after: None,
        limit: None,
    };
    let b = bind(std::slice::from_ref(&e)).expect("parsing is PostgreSQL's job");
    assert_eq!(b.since[0].as_deref(), Some("not a timestamp"));
}

// -------------------------------------------------------------- body parsing

#[test]
fn the_optional_keys_are_optional_in_every_spelling() {
    // Absent, and explicitly null, must mean the same thing — a client
    // serializing a struct with `Option` fields writes one or the other and
    // neither is a 400.
    for wire in [
        r#"{"entries":[{"queue":"orders"}]}"#,
        r#"{"entries":[{"queue":"orders","since":null,"after":null,"limit":null}]}"#,
    ] {
        let body: ChangedBody =
            serde_json::from_str(wire).unwrap_or_else(|e| panic!("{wire}: {e}"));
        let b = bind(&body.entries).unwrap();
        assert_eq!(b.since[0], None, "{wire}");
        assert_eq!(b.after[0], None, "{wire}");
        assert_eq!(b.limits[0], DEFAULT_LIMIT, "{wire}");
    }
    // An absent `entries` is the empty batch, i.e. the watermark-only request.
    let body: ChangedBody = serde_json::from_str("{}").unwrap();
    assert!(body.entries.is_empty());
}

#[test]
fn a_body_that_is_not_the_shape_is_a_parse_failure_not_a_silent_empty() {
    // Each of these reaches the `400` arm of the handler rather than being read
    // as "no entries", which would answer a watermark to a caller that asked
    // for queues.
    for wire in [
        r#"{"entries":{}}"#,
        r#"{"entries":[{"since":"2026-09-04T10:00:00Z"}]}"#, // no queue
        r#"{"entries":[{"queue":7}]}"#,
        r#"{"entries":[{"queue":"orders","limit":"1000"}]}"#,
        "null",
        "not json",
    ] {
        assert!(
            serde_json::from_str::<ChangedBody>(wire).is_err(),
            "{wire} must not parse"
        );
    }

    // ONE SHAPE THAT DOES PARSE AND IS NOT AN OBJECT, recorded rather than
    // asserted away: serde reads a struct from a JSON SEQUENCE positionally, so
    // a bare `[]` is `{"entries":[]}` — the watermark-only request. `FetchBody`
    // has had exactly the same property since C2 and nothing depends on it
    // either way; it is written down here so a reader who meets it in a log
    // knows it is serde's rule and not this route's.
    let body: ChangedBody = serde_json::from_str("[]").expect("serde reads a struct from a seq");
    assert!(body.entries.is_empty());
}

// ------------------------------------------------------------ the safe floor

#[test]
fn the_degraded_floor_rounds_up_from_milliseconds() {
    // Milliseconds in the environment, whole seconds to the SQL. Rounding DOWN
    // would hand back a floor SHORTER than the operator configured, and the
    // floor's whole job is to exceed any write statement's timeout — the one
    // direction in which a wrong answer is a lost record rather than a slow one.
    assert_eq!(secs_ceil(30_000), 30);
    assert_eq!(secs_ceil(30_001), 31);
    assert_eq!(secs_ceil(1), 1);
    assert_eq!(secs_ceil(999), 1);
    assert_eq!(secs_ceil(1_000), 1);
    assert_eq!(secs_ceil(1_001), 2);
    // ...and it saturates rather than wrapping into a negative interval.
    assert_eq!(secs_ceil(u64::MAX), i32::MAX);
}

#[test]
fn the_guard_is_a_constant_and_positive() {
    // A zero or negative guard would make safeTime `now()`, i.e. would claim a
    // window is settled while a transaction started microseconds ago has yet to
    // appear in this backend's stats snapshot.
    //
    // A `const` assertion, so clippy calls it constant — which it is, and that
    // is the point: this is a compile-time-known value nothing may edit to zero
    // without a red test. Written as a static assertion for exactly that
    // reason, rather than silenced.
    const _: () = assert!(GUARD_SECS > 0);
    // The resolved floor must be longer than the guard, or the degraded arm
    // would be LESS conservative than the healthy one. NOT const: it is read
    // from the environment.
    assert!(safe_floor_secs() > GUARD_SECS);
}

// ------------------------------------------------ queen-protocol conformance
//
// The body is composed by `033_log_partitions_changed.sql` and returned by the
// handler untouched, so no Rust type on the broker side describes it. The
// canonical types in `crates/queen-protocol` are the ONLY written-down
// definition of this wire, and this is where the two are made to agree — the
// same role `fetch_render.rs` plays for `POST /api/v1/fetch`.

/// A response transcribed from a real call against a seeded database, byte for
/// byte, including PostgreSQL's own `jsonb` rendering: keys ordered by (length,
/// then bytewise) and `", "` / `": "` separators. Key ORDER is not contract —
/// no caller may depend on it — but the key NAMES, the value types and the
/// timestamp shape all are.
const A_REAL_ANSWER: &str = concat!(
    r#"{"entries": [{"next": "n|cust-0002", "queue": "orders", "partitions": ["#,
    r#"{"name": "cust-0001", "logStart": 1, "lastOffset": 10, "#,
    r#""lastWriteAt": "2026-09-04T10:00:01.000000Z"}, "#,
    r#"{"name": "cust-0002", "logStart": 2, "lastOffset": 20, "#,
    r#""lastWriteAt": "2026-09-04T10:00:02.000000Z"}]}, "#,
    r#"{"error": "UNKNOWN_TOPIC_OR_PARTITION", "queue": "ghost"}], "#,
    r#""safeTime": "2026-09-04T07:11:44.965167Z", "safeTimeDegraded": false}"#,
);

#[test]
fn the_error_markers_agree_with_the_client_types() {
    // Three files spell these strings: the SQL that WRITES them, this handler
    // that names them, and the client type that COMPARES against them. Nothing
    // on the hot path reads the constants here, which is why they are
    // `allow(dead_code)` — this is the pin that gives them a reader.
    assert_eq!(
        ERR_UNKNOWN,
        queen_protocol::partitions::ERR_UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(ERR_BAD_CURSOR, queen_protocol::ERR_BAD_CURSOR);
    // ...and the unknown-queue marker is the SAME string the fetch path uses,
    // because it is the same condition: no such queue for this tenant.
    assert_eq!(ERR_UNKNOWN, queen_protocol::ERR_UNKNOWN_TOPIC_OR_PARTITION);
}

#[test]
fn a_real_answer_round_trips_through_the_client_types() {
    let got: queen_protocol::ChangedResponse = serde_json::from_str(A_REAL_ANSWER)
        .expect("the body this route returns must deserialize as the canonical type");
    assert_eq!(got.safe_time, "2026-09-04T07:11:44.965167Z");
    assert!(!got.safe_time_degraded);
    assert_eq!(got.entries.len(), 2);

    let orders = &got.entries[0];
    assert!(orders.is_ok());
    assert_eq!(orders.queue, "orders");
    assert_eq!(orders.next.as_deref(), Some("n|cust-0002"));
    assert_eq!(orders.partitions.len(), 2);
    assert_eq!(orders.partitions[1].name, "cust-0002");
    assert_eq!(orders.partitions[1].last_offset, 20);
    assert_eq!(orders.partitions[1].log_start, 2);

    // The error entry omits `partitions` and `next` entirely — the SQL builds
    // exactly two keys for it — so both must default on the client side rather
    // than failing the decode of the whole page.
    let ghost = &got.entries[1];
    assert!(ghost.is_unknown_queue());
    assert!(ghost.partitions.is_empty());
    assert!(!ghost.has_more());
}

#[test]
fn every_rendered_timestamp_is_six_digits_and_a_z() {
    // `lastWriteAt` and `safeTime` are what a reader compares a fetched
    // record's `ts` against, and `ts` is rendered by the SAME format string in
    // `032_log_fetch.sql`. A precision or suffix that drifted on one side would
    // make every comparison between them subtly wrong, so the shape is pinned
    // here rather than assumed.
    let got: queen_protocol::ChangedResponse = serde_json::from_str(A_REAL_ANSWER).unwrap();
    let mut stamps = vec![got.safe_time.clone()];
    stamps.extend(
        got.entries
            .iter()
            .flat_map(|e| e.partitions.iter().map(|p| p.last_write_at.clone())),
    );
    assert_eq!(stamps.len(), 3);
    for s in stamps {
        assert!(s.ends_with('Z'), "{s} must end in Z");
        assert_eq!(s.len(), 27, "{s} must be YYYY-MM-DDTHH:MM:SS.ffffffZ");
        let frac = s.rsplit_once('.').expect("a fractional part").1;
        assert_eq!(
            frac.len(),
            7,
            "{s} must carry exactly six fractional digits"
        );
        assert!(
            frac[..6].chars().all(|c| c.is_ascii_digit()),
            "{s} fractional digits"
        );
    }
}

#[test]
fn a_request_this_broker_accepts_is_one_the_client_type_writes() {
    // The other direction: what `queen-protocol` serializes must be what this
    // handler's own parser reads, field name for field name.
    let req = queen_protocol::ChangedRequest::new(vec![
        queen_protocol::ChangedEntry::new("orders")
            .since("2026-09-04T10:00:00.000000Z")
            .after("t|1788516004000000|cust-0004")
            .limit(500),
        queen_protocol::ChangedEntry::new("events"),
    ]);
    let wire = serde_json::to_string(&req).expect("serialize");
    let parsed: ChangedBody =
        serde_json::from_str(&wire).expect("the broker must read what the client writes");
    let b = bind(&parsed.entries).expect("and accept it");
    assert_eq!(b.queues, vec!["orders", "events"]);
    assert_eq!(b.since[0].as_deref(), Some("2026-09-04T10:00:00.000000Z"));
    assert_eq!(b.after[0].as_deref(), Some("t|1788516004000000|cust-0004"));
    assert_eq!(b.limits, vec![500, DEFAULT_LIMIT]);
    assert_eq!(b.since[1], None);
    assert_eq!(b.after[1], None);

    // ...and the empty (watermark-only) request survives the round trip too.
    let wire = serde_json::to_string(&queen_protocol::ChangedRequest::safe_time_only()).unwrap();
    let parsed: ChangedBody = serde_json::from_str(&wire).unwrap();
    assert!(bind(&parsed.entries).unwrap().queues.is_empty());
}
