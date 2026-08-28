//! `POST /api/v1/fetch` — the pure half of C2 (PLAN_QUEEN_KAFKA.md).
//!
//! Wired from `src/handlers/fetch.rs`; see `src/tests_unit/README.md` for the
//! mechanism. Everything here runs without a database: the SP meta is a literal
//! transcribed from `032_log_fetch.sql`'s `jsonb_build_object` calls, and the
//! blobs are packed with the same recipe the push path writes into
//! `queen.log_segments.blob`, so the renderer is exercised against real segment
//! bytes rather than against a mock of them.
//!
//! What is at stake, in the order it would hurt:
//!
//!  1. **The absolute offset.** `base + frame index` is the entire addressing
//!     scheme of this endpoint and the only thing a caller can commit. Off by
//!     one and a consumer silently skips or re-reads a message for ever.
//!  2. **Blob/meta alignment.** Blobs arrive flattened in the meta's traversal
//!     order and are consumed by a running index; a mid-segment slice
//!     (`startIdx > 0`) is where an alignment bug first shows.
//!  3. **The long-poll decision.** `bytes` and `any_error` are what park or
//!     release the caller, so they are asserted, not incidental.

use super::*;

use crate::frames::{pack_frames, zstd_compress, FrameIn};

fn enc() -> std::sync::Arc<crate::encryption::Encryption> {
    // No QUEEN_ENCRYPTION_KEY in the test environment ⇒ disabled, and a
    // disabled Encryption early-returns None from the envelope sniff, so
    // payloads are spliced verbatim.
    crate::encryption::Encryption::from_env()
}

/// A segment blob exactly as the push path stores one: frames packed in order,
/// zstd'd at the fusion default level.
fn seg(txns_and_payloads: &[(&str, &[u8])]) -> Vec<u8> {
    let frames: Vec<FrameIn> = txns_and_payloads
        .iter()
        .enumerate()
        .map(|(i, (txn, payload))| FrameIn {
            message_id: [i as u8 + 1; 16],
            txn,
            trace_id: None,
            producer_sub: None,
            payload,
            encrypted: false,
        })
        .collect();
    zstd_compress(&pack_frames(&frames), 3)
}

// ---------------------------------------------------------------------- render

#[test]
fn a_record_offset_is_the_segments_base_plus_its_frame_index() {
    // One segment based at 100 holding three frames, fetched from 101: the
    // records delivered are the LAST TWO, at 101 and 102 — not at 0 and 1, and
    // not at 100 and 101.
    let blob = seg(&[
        ("t-100", br#"{"n":100}"#),
        ("t-101", br#"{"n":101}"#),
        ("t-102", br#"{"n":102}"#),
    ]);
    let meta = r#"{"entries":[{"high":103,"logStart":0,"segments":[
        {"base":100,"startIdx":1,"take":2,"createdAt":"2026-08-28T10:00:00.000000Z"}]}]}"#;

    let p = render_fetch(
        meta,
        &[blob],
        &["orders".to_string()],
        &["eu".to_string()],
        &enc(),
    )
    .expect("the meta the SP builds must render");

    let v: serde_json::Value = serde_json::from_str(&p.body).unwrap();
    let recs = v["entries"][0]["records"].as_array().unwrap();
    assert_eq!(recs.len(), 2, "startIdx=1 take=2 of a 3-frame segment");
    assert_eq!(recs[0]["offset"], 101);
    assert_eq!(recs[1]["offset"], 102);
    assert_eq!(recs[0]["transactionId"], "t-101");
    assert_eq!(recs[0]["payload"], serde_json::json!({"n": 101}));
    assert_eq!(recs[0]["ts"], "2026-08-28T10:00:00.000000Z");
    // The bounds ride along whether or not records did.
    assert_eq!(v["entries"][0]["highWatermark"], 103);
    assert_eq!(v["entries"][0]["logStartOffset"], 0);
    assert_eq!(v["entries"][0]["queue"], "orders");
    assert_eq!(v["entries"][0]["partition"], "eu");
    assert!(!p.any_error);
}

#[test]
fn offsets_stay_correct_across_segments_and_entries() {
    // Two entries, the first spanning two segments with a retention-shaped gap
    // between them (10..11 then 40..41): the blobs are flattened in traversal
    // order, so a renderer that lost the running index would attribute the
    // second segment's frames to the first segment's base.
    let a = seg(&[("a0", b"1"), ("a1", b"2")]);
    let b = seg(&[("b0", b"3"), ("b1", b"4")]);
    let c = seg(&[("c0", b"5")]);
    let meta = r#"{"entries":[
        {"high":42,"logStart":10,"segments":[
            {"base":10,"startIdx":0,"take":2,"createdAt":"2026-08-28T10:00:00.000000Z"},
            {"base":40,"startIdx":0,"take":2,"createdAt":"2026-08-28T10:00:01.000000Z"}]},
        {"high":8,"logStart":7,"segments":[
            {"base":7,"startIdx":0,"take":1,"createdAt":"2026-08-28T10:00:02.000000Z"}]}]}"#;

    let p = render_fetch(
        meta,
        &[a, b, c],
        &["orders".to_string(), "orders".to_string()],
        &["eu".to_string(), "us".to_string()],
        &enc(),
    )
    .unwrap();
    let v: serde_json::Value = serde_json::from_str(&p.body).unwrap();

    let e0 = v["entries"][0]["records"].as_array().unwrap();
    assert_eq!(
        e0.iter().map(|r| r["offset"].as_i64().unwrap()).collect::<Vec<_>>(),
        vec![10, 11, 40, 41]
    );
    let e1 = v["entries"][1]["records"].as_array().unwrap();
    assert_eq!(e1.len(), 1);
    assert_eq!(e1[0]["offset"], 7);
    assert_eq!(e1[0]["transactionId"], "c0");
    assert_eq!(v["entries"][1]["partition"], "us");
}

#[test]
fn an_empty_entry_still_carries_its_bounds() {
    // The caught-up consumer, and the shape a ListOffsets probe reads: no
    // segments, both watermarks present, no error. This is also the entry the
    // long poll parks on, so `bytes` must be zero.
    let meta = r#"{"entries":[{"high":57,"logStart":12,"segments":[]}]}"#;
    let p = render_fetch(meta, &[], &["orders".to_string()], &["eu".to_string()], &enc()).unwrap();
    let v: serde_json::Value = serde_json::from_str(&p.body).unwrap();
    assert!(v["entries"][0]["records"].as_array().unwrap().is_empty());
    assert_eq!(v["entries"][0]["highWatermark"], 57);
    assert_eq!(v["entries"][0]["logStartOffset"], 12);
    assert!(v["entries"][0].get("error").is_none(), "no error key when there is no error");
    assert_eq!(p.bytes, 0);
    assert!(!p.any_error);
}

#[test]
fn the_two_error_markers_survive_to_the_wire_and_release_the_poll() {
    // Both markers in one response, mixed with a healthy entry: `any_error`
    // must be set (the poll returns AT ONCE — the client's offset is wrong now
    // and will be just as wrong in maxWaitMs), and the marker text must reach
    // the client, since a facade maps it straight to a Kafka error code.
    let meta = r#"{"entries":[
        {"error":"OFFSET_OUT_OF_RANGE","high":900,"logStart":800,"segments":[]},
        {"high":5,"logStart":0,"segments":[]},
        {"error":"UNKNOWN_TOPIC_OR_PARTITION","high":0,"logStart":0,"segments":[]}]}"#;
    let qs = vec!["orders".to_string(), "orders".to_string(), "ghost".to_string()];
    let ps = vec!["eu".to_string(), "us".to_string(), "Default".to_string()];
    let p = render_fetch(meta, &[], &qs, &ps, &enc()).unwrap();
    let v: serde_json::Value = serde_json::from_str(&p.body).unwrap();

    assert!(p.any_error);
    assert_eq!(v["entries"][0]["error"], "OFFSET_OUT_OF_RANGE");
    // The bounds come back WITH the out-of-range marker: they are how a
    // consumer resets (to logStartOffset for earliest, highWatermark for
    // latest) without a second round trip.
    assert_eq!(v["entries"][0]["logStartOffset"], 800);
    assert_eq!(v["entries"][0]["highWatermark"], 900);
    assert!(v["entries"][1].get("error").is_none());
    assert_eq!(v["entries"][2]["error"], "UNKNOWN_TOPIC_OR_PARTITION");
}

#[test]
fn an_empty_payload_still_counts_one_byte_towards_min_bytes() {
    // `minBytes: 1` means "wake me on any record" to every Kafka client. A
    // record whose stored payload is empty renders as `null`, and counting its
    // zero bytes literally would park a caller that HAS data to process.
    let blob = seg(&[("t-0", b"")]);
    let meta = r#"{"entries":[{"high":1,"logStart":0,"segments":[
        {"base":0,"startIdx":0,"take":1,"createdAt":"2026-08-28T10:00:00.000000Z"}]}]}"#;
    let p = render_fetch(meta, &[blob], &["q".to_string()], &["Default".to_string()], &enc())
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&p.body).unwrap();
    assert_eq!(v["entries"][0]["records"][0]["payload"], serde_json::Value::Null);
    assert_eq!(p.bytes, 1, "an empty payload is one byte for the minBytes test");
}

#[test]
fn a_misaligned_sp_answer_is_refused_rather_than_served() {
    // Fewer entries than were asked for would shift every echoed queue/partition
    // by one, handing a caller ANOTHER partition's watermarks to commit. That is
    // a broker bug and it must surface as a 500, never as a plausible body.
    let meta = r#"{"entries":[{"high":1,"logStart":0,"segments":[]}]}"#;
    assert!(render_fetch(
        meta,
        &[],
        &["a".to_string(), "b".to_string()],
        &["Default".to_string(), "Default".to_string()],
        &enc()
    )
    .is_none());
    // A segment the meta announces but whose blob is missing is the same class
    // of bug (the running index would then read another entry's segment).
    let meta2 = r#"{"entries":[{"high":9,"logStart":0,"segments":[
        {"base":0,"startIdx":0,"take":1,"createdAt":"2026-08-28T10:00:00.000000Z"}]}]}"#;
    assert!(
        render_fetch(meta2, &[], &["a".to_string()], &["Default".to_string()], &enc()).is_none()
    );
    // And a body that is not the SP's JSON at all.
    assert!(render_fetch("not json", &[], &["a".to_string()], &["p".to_string()], &enc()).is_none());
}

#[test]
fn the_rendered_body_is_valid_json_for_every_name_a_queue_can_have() {
    // Queue and partition names are echoed from the REQUEST, so whatever the
    // parser accepted has to survive the hand-rolled renderer intact.
    let meta = r#"{"entries":[{"high":0,"logStart":0,"segments":[]}]}"#;
    let p = render_fetch(
        meta,
        &[],
        &[r#"we"ird\q"#.to_string()],
        &["p\ta\nrt".to_string()],
        &enc(),
    )
    .unwrap();
    let v: serde_json::Value = serde_json::from_str(&p.body).expect("valid JSON");
    assert_eq!(v["entries"][0]["queue"], r#"we"ird\q"#);
    assert_eq!(v["entries"][0]["partition"], "p\ta\nrt");
}

// ------------------------------------------------------------------- markers

/// The two per-entry error markers are written by SQL, carried untouched
/// through the meta, and read by `queen-protocol`'s `FetchEntryResult`
/// helpers — three files that must spell them identically and none of which
/// would fail if one drifted. A consumer whose `is_offset_out_of_range()`
/// silently stopped matching would loop for ever on an offset it can never
/// read, which is exactly the failure mode M3 of the plan calls out.
#[test]
fn the_error_markers_agree_across_sql_broker_and_client() {
    const FETCH_SQL: &str = include_str!("../../sql/procedures/032_log_fetch.sql");
    for marker in [ERR_OUT_OF_RANGE, ERR_UNKNOWN] {
        assert!(
            FETCH_SQL.contains(&format!("'{marker}'")),
            "032_log_fetch.sql must be the thing that emits {marker}"
        );
    }
    assert_eq!(ERR_OUT_OF_RANGE, queen_protocol::ERR_OFFSET_OUT_OF_RANGE);
    assert_eq!(ERR_UNKNOWN, queen_protocol::ERR_UNKNOWN_TOPIC_OR_PARTITION);
}

/// The rendered body is what the client type has to read, so parse it with the
/// client type rather than with `serde_json::Value` — the same conformance
/// discipline `handlers::data::protocol_conformance` applies to push and pop.
#[test]
fn the_rendered_body_parses_into_the_client_type() {
    let blob = seg(&[("t-41", br#"{"total":19.99}"#), ("t-42", b"")]);
    let meta = r#"{"entries":[
        {"high":43,"logStart":12,"segments":[
            {"base":41,"startIdx":0,"take":2,"createdAt":"2026-08-28T09:15:00.123456Z"}]},
        {"error":"OFFSET_OUT_OF_RANGE","high":900,"logStart":800,"segments":[]}]}"#;
    let p = render_fetch(
        meta,
        &[blob],
        &["orders".to_string(), "orders".to_string()],
        &["eu".to_string(), "ap".to_string()],
        &enc(),
    )
    .unwrap();

    let got: queen_protocol::FetchResponse =
        serde_json::from_str(&p.body).expect("the client type must read what the broker renders");
    assert_eq!(got.record_count(), 2);
    let eu = &got.entries[0];
    assert!(eu.is_ok());
    assert_eq!(eu.records[0].offset, 41);
    assert_eq!(eu.records[0].transaction_id, "t-41");
    assert_eq!(eu.records[0].payload, serde_json::json!({"total": 19.99}));
    assert_eq!(eu.records[1].payload, serde_json::Value::Null);
    assert_eq!(eu.next_offset(), Some(43));
    assert_eq!(eu.high_watermark, 43);
    assert_eq!(eu.log_start_offset, 12);
    assert!(got.entries[1].is_offset_out_of_range());
}

/// The request the client type SERIALIZES must be the request this handler
/// PARSES — the other direction of the same conformance rule.
#[test]
fn the_client_request_parses_into_the_handlers_body() {
    let req = queen_protocol::FetchRequest::new(vec![
        queen_protocol::FetchEntry::new("orders", 41)
            .partition("eu")
            .max_bytes(1_048_576),
        queen_protocol::FetchEntry::new("orders", 0),
    ])
    .long_poll(5_000)
    .min_bytes(1);
    let wire = serde_json::to_vec(&req).unwrap();

    let body: FetchBody = serde_json::from_slice(&wire).expect("the broker must read it back");
    assert_eq!(body.entries.len(), 2);
    assert_eq!(body.entries[0].queue, "orders");
    assert_eq!(body.entries[0].partition.as_ref().unwrap().0, "eu");
    assert_eq!(body.entries[0].offset, 41);
    assert_eq!(body.entries[0].max_bytes, Some(1_048_576));
    assert!(body.entries[1].partition.is_none());
    assert_eq!(body.max_wait_ms, Some(5_000));
    assert_eq!(body.min_bytes, Some(1));
}

// --------------------------------------------------------------------- request

#[test]
fn a_partition_parses_from_a_name_or_from_a_kafka_partition_number() {
    // The mapping the plan settles on is "Kafka partition n = Queen partition
    // n", and a facade holding the number would otherwise 400 the WHOLE batch
    // on a type error.
    let body: FetchBody = serde_json::from_str(
        r#"{"entries":[
            {"queue":"q","partition":"eu","offset":0},
            {"queue":"q","partition":3,"offset":7},
            {"queue":"q","offset":9}]}"#,
    )
    .unwrap();
    assert_eq!(body.entries[0].partition.as_ref().unwrap().0, "eu");
    assert_eq!(body.entries[1].partition.as_ref().unwrap().0, "3");
    assert!(
        body.entries[2].partition.is_none(),
        "an omitted partition stays None so the handler applies the push path's default"
    );
}

#[test]
fn absent_poll_knobs_mean_answer_now() {
    // A request that carries neither knob must not park: `maxWaitMs` absent is
    // 0, and `minBytes` absent is 1 (any record), which is what a client that
    // predates the long poll would expect.
    let body: FetchBody =
        serde_json::from_str(r#"{"entries":[{"queue":"q","offset":0}]}"#).unwrap();
    assert_eq!(body.max_wait_ms.unwrap_or(0), 0);
    assert_eq!(body.min_bytes.unwrap_or(1), 1);
}

#[test]
fn the_clamps_are_the_ceilings_the_module_documents() {
    // Pinned as arithmetic rather than as prose: these are the values passed to
    // SQL, and the SQL is where an unbounded read would actually happen.
    assert_eq!(2_000_000i64.clamp(1, MAX_BYTES_PER_ENTRY), MAX_BYTES_PER_ENTRY.min(2_000_000));
    assert_eq!((-5i64).clamp(1, MAX_BYTES_PER_ENTRY), 1);
    assert_eq!(
        (MAX_BYTES_PER_ENTRY * 4).clamp(1, MAX_BYTES_PER_ENTRY),
        MAX_BYTES_PER_ENTRY
    );
    assert_eq!(60_000u64.min(MAX_WAIT_MS), MAX_WAIT_MS);
    assert_eq!(250u64.min(MAX_WAIT_MS), 250);
    // The whole-response budget has to bound the worst legal request, or the
    // per-entry clamp is decoration.
    assert!(
        MAX_TOTAL_BYTES < MAX_BYTES_PER_ENTRY * MAX_ENTRIES as i64,
        "the total budget must be the binding constraint at full width"
    );
    // The recheck ceiling is the plan's 100-250ms band.
    assert!((100..=250).contains(&RECHECK_MS));
}
