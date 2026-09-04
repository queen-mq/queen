//! [`FakeQueen`] against the semantics it claims to have.
//!
//! This double is load-bearing: the window engine, the driver, the seek and the
//! lease are all proved against it, so a semantic it gets wrong is a semantic
//! the whole crate is proved against wrongly. Everything asserted here is a
//! stated rule of a stored procedure, and each test names it:
//!
//! * `032_log_fetch.sql` — the offset arms, the two kinds of NULL, the ceilings;
//! * `033_log_partitions_changed.sql` — the two modes, the mode-tagged cursor,
//!   the quantized `last_write_at` that makes paging sound;
//! * `024_kv.sql` — `expect`, `putIfAbsent`, `required` rollback, `getPrefix`,
//!   TTLs, and versions that never come from `version + 1`.

use queen_s3::queen::{FakeQueen, KvOp, QueenApi};
use queen_s3::types::{ChangedRequestEntry, FetchError, FetchRequestEntry, Micros, SinkError};

fn ts(s: &str) -> Micros {
    Micros::parse_iso(s).unwrap()
}

fn entry(queue: &str, partition: &str, offset: i64) -> FetchRequestEntry {
    FetchRequestEntry {
        queue: queue.to_string(),
        partition: partition.into(),
        offset,
        max_bytes: None,
    }
}

async fn fetch_one(q: &FakeQueen, e: FetchRequestEntry) -> queen_s3::types::FetchedEntry {
    q.fetch(vec![e], 0, 1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}

// ---------------------------------------------------------------------------
// fetch — 032_log_fetch.sql
// ---------------------------------------------------------------------------

#[tokio::test]
async fn records_come_back_contiguously_from_the_offset_with_their_segment_timestamp() {
    let q = FakeQueen::new();
    q.push(
        "orders",
        "cust-1",
        ts("2026-09-04T10:00:00Z"),
        &["1", "2", "3"],
    );
    q.push("orders", "cust-1", ts("2026-09-04T10:00:01Z"), &["4", "5"]);

    let got = fetch_one(&q, entry("orders", "cust-1", 0)).await;
    assert_eq!(got.error, None);
    assert_eq!(got.high_watermark, 5, "last + 1");
    assert_eq!(got.log_start_offset, 0);
    let offsets: Vec<i64> = got.records.iter().map(|r| r.offset).collect();
    assert_eq!(offsets, vec![0, 1, 2, 3, 4]);
    // Every record of ONE push shares its segment's timestamp: it is a commit
    // time, not a per-message one, and the window engine depends on that.
    assert_eq!(got.records[0].ts, ts("2026-09-04T10:00:00Z"));
    assert_eq!(got.records[2].ts, ts("2026-09-04T10:00:00Z"));
    assert_eq!(got.records[3].ts, ts("2026-09-04T10:00:01Z"));
    // …and offsets and timestamps are co-monotone within the partition.
    for pair in got.records.windows(2) {
        assert!(pair[0].offset < pair[1].offset);
        assert!(pair[0].ts <= pair[1].ts);
    }
    assert_eq!(got.records[0].transaction_id, "txn-cust-1-0");
    assert_eq!(got.records[0].payload.as_ref().unwrap().get(), "1");

    // A mid-log offset gets the tail.
    let got = fetch_one(&q, entry("orders", "cust-1", 3)).await;
    assert_eq!(
        got.records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![3, 4]
    );
}

#[tokio::test]
async fn the_offset_arms_are_kafkas_and_the_broker_s() {
    let q = FakeQueen::new();
    q.push(
        "orders",
        "cust-1",
        ts("2026-09-04T10:00:00Z"),
        &["1", "2", "3"],
    );
    q.retention_delete_below("orders", "cust-1", 1);

    // offset = high is VALID AND EMPTY — the caught-up caller, the entry the
    // long poll parks on.
    let got = fetch_one(&q, entry("orders", "cust-1", 3)).await;
    assert_eq!(got.error, None);
    assert!(got.records.is_empty());
    assert_eq!(got.high_watermark, 3);
    assert_eq!(got.log_start_offset, 1);

    // offset > high is out of range: a corrupted cursor must be told, not
    // served an empty answer for ever.
    let got = fetch_one(&q, entry("orders", "cust-1", 4)).await;
    assert_eq!(got.error, Some(FetchError::OffsetOutOfRange));
    assert_eq!(got.high_watermark, 3, "the bounds are still populated");
    assert_eq!(got.log_start_offset, 1, "so the reset target is in hand");

    // offset < logStart is out of range: retention passed the caller by, which
    // is the one failure of plan §4.6 and is never silent.
    let got = fetch_one(&q, entry("orders", "cust-1", 0)).await;
    assert_eq!(got.error, Some(FetchError::OffsetOutOfRange));
}

#[tokio::test]
async fn an_unwritten_lane_is_empty_and_an_unknown_queue_is_unknown() {
    let q = FakeQueen::new();
    q.create_queue("orders");

    // Queue yes, no partition row: bounds 0/0 and NO error. Lanes are
    // materialised lazily by the first push, so on a queue produced to on one
    // lane every other lane looks like this — answering UNKNOWN would be false
    // and would defeat the long poll for the whole batch.
    let got = fetch_one(&q, entry("orders", "never-written", 0)).await;
    assert_eq!(got.error, None);
    assert_eq!((got.log_start_offset, got.high_watermark), (0, 0));
    assert!(got.records.is_empty());
    // Offset 0 is valid and empty; anything above it is out of range, exactly
    // as it will be after the first push.
    let got = fetch_one(&q, entry("orders", "never-written", 1)).await;
    assert_eq!(got.error, Some(FetchError::OffsetOutOfRange));

    // No queue row at all: the tenancy arm, byte-identical to a queue another
    // tenant owns.
    let got = fetch_one(&q, entry("someone-elses", "0", 0)).await;
    assert_eq!(got.error, Some(FetchError::UnknownTopicOrPartition));
    assert_eq!((got.log_start_offset, got.high_watermark), (0, 0));
}

#[tokio::test]
async fn a_null_payload_is_none_and_still_weighs_something() {
    let q = FakeQueen::new();
    q.push(
        "orders",
        "cust-1",
        ts("2026-09-04T10:00:00Z"),
        &["null", "{\"a\":1}"],
    );
    let got = fetch_one(&q, entry("orders", "cust-1", 0)).await;
    assert!(got.records[0].payload.is_none());
    assert_eq!(got.records[1].payload.as_ref().unwrap().get(), "{\"a\":1}");
    assert!(
        got.records[0].weight() > 0,
        "a run of nulls still moves a budget"
    );
}

#[tokio::test]
async fn the_per_entry_ceiling_truncates_and_the_caller_comes_back_for_the_rest() {
    let q = FakeQueen::new();
    let payloads: Vec<&str> = vec!["1"; 50];
    q.push("orders", "cust-1", ts("2026-09-04T10:00:00Z"), &payloads);

    let mut e = entry("orders", "cust-1", 0);
    e.max_bytes = Some(3 * 1024); // three records' worth, at the double's rate
    let got = fetch_one(&q, e.clone()).await;
    assert_eq!(got.records.len(), 3);
    assert_eq!(got.high_watermark, 50, "the bounds are the WHOLE lane's");

    e.offset = got.records.last().unwrap().offset + 1;
    let got = fetch_one(&q, e).await;
    assert_eq!(got.records[0].offset, 3);
}

#[tokio::test]
async fn the_whole_call_budget_is_spent_in_entry_order() {
    let q = FakeQueen::new();
    for lane in ["a", "b", "c"] {
        q.push("orders", lane, ts("2026-09-04T10:00:00Z"), &["1", "2", "3"]);
    }
    q.set_records_per_call(4);
    let out = q
        .fetch(
            vec![
                entry("orders", "a", 0),
                entry("orders", "b", 0),
                entry("orders", "c", 0),
            ],
            0,
            1,
        )
        .await
        .unwrap();
    assert_eq!(out[0].records.len(), 3, "the first entry is served first");
    assert_eq!(out[1].records.len(), 1, "the second gets what is left");
    assert_eq!(out[2].records.len(), 0, "the third gets nothing");
    // …and every entry still reports its bounds, which is what makes an empty
    // answer usable.
    assert_eq!(out[2].high_watermark, 3);
}

#[tokio::test]
async fn retention_moves_log_start_and_drops_what_is_below_it() {
    let q = FakeQueen::new();
    q.push("orders", "cust-1", ts("2026-09-04T10:00:00Z"), &["1", "2"]);
    q.push("orders", "cust-1", ts("2026-09-04T10:00:01Z"), &["3", "4"]);
    q.retention_delete_below("orders", "cust-1", 3);
    assert_eq!(q.bounds("orders", "cust-1"), Some((3, 4)));
    let got = fetch_one(&q, entry("orders", "cust-1", 3)).await;
    assert_eq!(got.records.len(), 1);
    assert_eq!(got.records[0].offset, 3);
    assert_eq!(got.records[0].ts, ts("2026-09-04T10:00:01Z"));
}

// ---------------------------------------------------------------------------
// partitions/changed — 033_log_partitions_changed.sql
// ---------------------------------------------------------------------------

fn changed(
    queue: &str,
    since: Option<Micros>,
    after: Option<String>,
    limit: u32,
) -> ChangedRequestEntry {
    ChangedRequestEntry {
        queue: queue.to_string(),
        since,
        after,
        limit,
    }
}

#[tokio::test]
async fn full_enumeration_walks_by_name_and_pages_with_its_own_cursor() {
    let q = FakeQueen::new();
    for lane in ["c", "a", "d", "b"] {
        q.push("orders", lane, ts("2026-09-04T10:00:00Z"), &["1"]);
    }
    let page = q
        .partitions_changed(vec![changed("orders", None, None, 2)])
        .await
        .unwrap();
    let names: Vec<String> = page.entries[0]
        .partitions
        .iter()
        .map(|p| p.name.to_string())
        .collect();
    assert_eq!(names, vec!["a", "b"]);
    let cursor = page.entries[0].next.clone().unwrap();
    assert!(
        cursor.starts_with("n|"),
        "the cursor is mode-tagged: {cursor}"
    );

    let page = q
        .partitions_changed(vec![changed("orders", None, Some(cursor), 2)])
        .await
        .unwrap();
    let names: Vec<String> = page.entries[0]
        .partitions
        .iter()
        .map(|p| p.name.to_string())
        .collect();
    assert_eq!(names, vec!["c", "d"]);
    assert_eq!(page.entries[0].next, None, "the last page says so");
}

#[tokio::test]
async fn since_mode_returns_only_what_moved_and_pages_by_time_then_name() {
    let q = FakeQueen::new();
    q.push("orders", "old", ts("2026-09-04T09:00:00Z"), &["1"]);
    q.push("orders", "mid", ts("2026-09-04T10:00:00Z"), &["1"]);
    q.push("orders", "new", ts("2026-09-04T10:05:00Z"), &["1"]);

    let out = q
        .partitions_changed(vec![changed(
            "orders",
            Some(ts("2026-09-04T09:30:00Z")),
            None,
            1000,
        )])
        .await
        .unwrap();
    let names: Vec<String> = out.entries[0]
        .partitions
        .iter()
        .map(|p| p.name.to_string())
        .collect();
    assert_eq!(names, vec!["mid", "new"], "ordered by (lastWriteAt, name)");
    assert_eq!(
        out.entries[0].partitions[0].last_offset, 0,
        "last, not next"
    );
    assert_eq!(out.entries[0].partitions[0].log_start, 0);

    let page = q
        .partitions_changed(vec![changed(
            "orders",
            Some(ts("2026-09-04T09:30:00Z")),
            None,
            1,
        )])
        .await
        .unwrap();
    let cursor = page.entries[0].next.clone().unwrap();
    assert!(cursor.starts_with("t|"), "{cursor}");
    let page = q
        .partitions_changed(vec![changed(
            "orders",
            Some(ts("2026-09-04T09:30:00Z")),
            Some(cursor),
            10,
        )])
        .await
        .unwrap();
    assert_eq!(page.entries[0].partitions[0].name.to_string(), "new");
}

#[tokio::test]
async fn a_cursor_from_the_other_mode_is_refused_rather_than_quietly_restarted() {
    let q = FakeQueen::new();
    q.push("orders", "a", ts("2026-09-04T10:00:00Z"), &["1"]);

    let out = q
        .partitions_changed(vec![changed("orders", None, Some("t|1|a".into()), 10)])
        .await
        .unwrap();
    assert_eq!(out.entries[0].error.as_deref(), Some("BAD_CURSOR"));

    let out = q
        .partitions_changed(vec![changed(
            "orders",
            Some(ts("2026-09-04T09:00:00Z")),
            Some("n|a".into()),
            10,
        )])
        .await
        .unwrap();
    assert_eq!(
        out.entries[0].error.as_deref(),
        Some("BAD_CURSOR"),
        "restarting quietly would loop a paging client on the first page for ever"
    );
}

#[tokio::test]
async fn last_write_at_is_quantized_after_the_first_push_and_only_ever_moves_up() {
    let q = FakeQueen::new();

    async fn seen(q: &FakeQueen) -> Micros {
        q.partitions_changed(vec![changed("orders", None, None, 10)])
            .await
            .unwrap()
            .entries[0]
            .partitions[0]
            .last_write_at
            .unwrap()
    }

    // The FIRST push stamps the exact timestamp — which is why the cursor
    // carries MICROSECONDS and not seconds: a second-resolution cursor would
    // round two distinct rows onto one key and page-skip between them.
    q.push("orders", "a", ts("2026-09-04T10:00:00.918204Z"), &["1"]);
    assert_eq!(seen(&q).await, ts("2026-09-04T10:00:00.918204Z"));

    // A later push in the SAME second floors — and must not walk it backwards.
    // "It only ever moves UP" is the whole argument for why a row can be seen
    // twice across pages but never missed.
    q.push("orders", "a", ts("2026-09-04T10:00:00.999999Z"), &["2"]);
    assert_eq!(seen(&q).await, ts("2026-09-04T10:00:00.918204Z"));

    // A push in a later second bumps it, quantized to the second so the
    // allocator UPDATE stays HOT.
    q.push("orders", "a", ts("2026-09-04T10:00:03.500000Z"), &["3"]);
    assert_eq!(seen(&q).await, ts("2026-09-04T10:00:03.000000Z"));
}

#[tokio::test]
async fn an_unknown_queue_takes_the_tenancy_arm_and_a_never_written_lane_is_not_enumerated() {
    let q = FakeQueen::new();
    q.create_queue("orders");
    let out = q
        .partitions_changed(vec![
            changed("orders", None, None, 10),
            changed("someone-elses", None, None, 10),
        ])
        .await
        .unwrap();
    assert!(out.entries[0].partitions.is_empty(), "no rows, no error");
    assert_eq!(out.entries[0].error, None);
    assert_eq!(
        out.entries[1].error.as_deref(),
        Some("UNKNOWN_TOPIC_OR_PARTITION")
    );
    assert!(out.entries[1].partitions.is_empty());
}

#[tokio::test]
async fn safe_time_is_derived_until_it_is_pinned() {
    let q = FakeQueen::new();
    assert_eq!(q.safe_time(), Micros(0), "nothing pushed, nothing safe");
    q.push("orders", "a", ts("2026-09-04T10:00:00Z"), &["1"]);
    assert_eq!(
        q.safe_time(),
        ts("2026-09-04T10:00:00Z").saturating_add(Micros(1)),
        "everything that exists is safe"
    );
    q.set_safe_time(ts("2026-09-04T09:00:00Z"));
    let out = q
        .partitions_changed(vec![changed("orders", None, None, 10)])
        .await
        .unwrap();
    assert_eq!(out.safe_time, ts("2026-09-04T09:00:00Z"));
    assert!(!out.safe_time_degraded);
    q.set_safe_time_degraded(true);
    let out = q
        .partitions_changed(vec![changed("orders", None, None, 10)])
        .await
        .unwrap();
    assert!(out.safe_time_degraded);
    q.clear_safe_time();
    assert!(q.safe_time() > ts("2026-09-04T09:00:00Z"));
}

// ---------------------------------------------------------------------------
// kv — 024_kv.sql
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_reports_found_separately_from_value() {
    let q = FakeQueen::new();
    let out = q.kv(vec![KvOp::get("missing")]).await.unwrap();
    assert_eq!(out[0].found, Some(false));
    assert_eq!(out[0].value, serde_json::Value::Null);
    assert_eq!(out[0].version, 0, "0 is `not there`");

    // `'null'::jsonb` is a legal stored value: {found:true,value:null} is NOT
    // {found:false}, and no caller may collapse the two.
    q.kv(vec![KvOp::put("k", serde_json::Value::Null)])
        .await
        .unwrap();
    let out = q.kv(vec![KvOp::get("k")]).await.unwrap();
    assert_eq!(out[0].found, Some(true));
    assert_eq!(out[0].value, serde_json::Value::Null);
    assert!(out[0].version > 0);
}

#[tokio::test]
async fn versions_are_opaque_and_never_version_plus_one() {
    let q = FakeQueen::new();
    let mut seen = Vec::new();
    for i in 0..4 {
        let out = q
            .kv(vec![KvOp::put("k", serde_json::json!({ "i": i }))])
            .await
            .unwrap();
        seen.push(out[0].version);
    }
    for pair in seen.windows(2) {
        assert_ne!(pair[1], pair[0], "a version must change on every write");
    }
    // Another key's write consumes from the same sequence, which is what makes
    // `version + 1` unguessable and the ABA impossible.
    q.kv(vec![KvOp::put("other", serde_json::json!(1))])
        .await
        .unwrap();
    let out = q
        .kv(vec![KvOp::put("k", serde_json::json!(2))])
        .await
        .unwrap();
    assert!(out[0].version > seen[3] + 1);
}

#[tokio::test]
async fn expect_zero_is_must_not_exist_and_expect_n_is_a_pure_update() {
    let q = FakeQueen::new();

    // expect: 0 on an absent key applies.
    let out = q
        .kv(vec![KvOp::put_expecting("k", serde_json::json!(1), 0)])
        .await
        .unwrap();
    assert!(out[0].did_apply());
    let version = out[0].version;

    // expect: 0 on a key that is there loses, with reason `exists` and the
    // WINNER's version and value — no second round trip.
    let out = q
        .kv(vec![KvOp::put_expecting("k", serde_json::json!(2), 0)])
        .await
        .unwrap();
    assert_eq!(out[0].applied, Some(false));
    assert_eq!(out[0].reason.as_deref(), Some("exists"));
    assert_eq!(out[0].version, version);
    assert_eq!(out[0].value, serde_json::json!(1));

    // expect: N with the right version applies.
    let out = q
        .kv(vec![KvOp::put_expecting(
            "k",
            serde_json::json!(3),
            version,
        )])
        .await
        .unwrap();
    assert!(out[0].did_apply());

    // expect: N with the wrong version loses, reason `version`.
    let out = q
        .kv(vec![KvOp::put_expecting(
            "k",
            serde_json::json!(4),
            version,
        )])
        .await
        .unwrap();
    assert_eq!(out[0].reason.as_deref(), Some("version"));

    // expect: N on an ABSENT key creates NOTHING, reason `absent`.
    let out = q
        .kv(vec![KvOp::put_expecting("gone", serde_json::json!(1), 42)])
        .await
        .unwrap();
    assert_eq!(out[0].applied, Some(false));
    assert_eq!(out[0].reason.as_deref(), Some("absent"));
    assert_eq!(q.kv_get("gone"), None, "a pure update creates nothing");
}

#[tokio::test]
async fn put_if_absent_is_labelled_put_and_claims_exactly_once() {
    let q = FakeQueen::new();
    let out = q
        .kv(vec![KvOp::put_if_absent_ttl(
            "s3:default:orders:lease",
            serde_json::json!({"instance": "a"}),
            30,
        )])
        .await
        .unwrap();
    assert!(out[0].did_apply());
    assert_eq!(out[0].op, "put", "the SP desugars it and labels the answer");

    let out = q
        .kv(vec![KvOp::put_if_absent_ttl(
            "s3:default:orders:lease",
            serde_json::json!({"instance": "b"}),
            30,
        )])
        .await
        .unwrap();
    assert_eq!(out[0].applied, Some(false));
    assert_eq!(out[0].reason.as_deref(), Some("exists"));
    assert_eq!(
        out[0].value,
        serde_json::json!({"instance": "a"}),
        "the loser is told who holds it"
    );
}

#[tokio::test]
async fn a_required_precondition_rolls_the_whole_batch_back() {
    let q = FakeQueen::new();
    q.kv(vec![KvOp::put(
        "lease",
        serde_json::json!({"instance": "a"}),
    )])
    .await
    .unwrap();
    let held = q.kv_version("lease");

    // The fence loses (a stale version), so NOTHING in the batch lands: the
    // commit pointer must not move under a lease this instance no longer holds.
    let err = q
        .kv(vec![
            KvOp::fence("lease", serde_json::json!({"instance": "b"}), held + 99),
            KvOp::put("committed", serde_json::json!({"k": 7})),
        ])
        .await
        .unwrap_err();
    match err {
        SinkError::Precondition {
            failed_index,
            reason,
            version,
            value,
        } => {
            assert_eq!(failed_index, 0);
            assert_eq!(reason, "version");
            assert_eq!(version, held);
            assert_eq!(value, serde_json::json!({"instance": "a"}));
        }
        other => panic!("expected a precondition, got {other:?}"),
    }
    assert_eq!(q.kv_get("committed"), None, "the whole batch rolled back");
    assert_eq!(q.kv_version("lease"), held, "and the lease is untouched");

    // The same batch with the right version lands whole.
    let out = q
        .kv(vec![
            KvOp::fence("lease", serde_json::json!({"instance": "a"}), held),
            KvOp::put("committed", serde_json::json!({"k": 7})),
        ])
        .await
        .unwrap();
    assert!(out[0].did_apply() && out[1].did_apply());
    assert_eq!(q.kv_get("committed"), Some(serde_json::json!({"k": 7})));
}

#[tokio::test]
async fn get_many_answers_rows_and_missing_and_get_prefix_pages() {
    let q = FakeQueen::new();
    for key in ["s3:default:a", "s3:default:b", "s3:default:c", "s3:other:d"] {
        q.kv_seed(key, serde_json::json!(key));
    }
    let out = q
        .kv(vec![KvOp::get_many(vec![
            "s3:default:a".into(),
            "s3:default:zzz".into(),
        ])])
        .await
        .unwrap();
    assert_eq!(out[0].rows.len(), 1);
    assert_eq!(out[0].rows[0].key, "s3:default:a");
    assert_eq!(out[0].missing, vec!["s3:default:zzz"]);

    let out = q
        .kv(vec![KvOp::get_prefix("s3:default:", 2, None)])
        .await
        .unwrap();
    let keys: Vec<String> = out[0].rows.iter().map(|r| r.key.clone()).collect();
    assert_eq!(keys, vec!["s3:default:a", "s3:default:b"]);
    assert!(out[0].truncated);
    assert_eq!(out[0].next_after.as_deref(), Some("s3:default:b"));

    let out = q
        .kv(vec![KvOp::get_prefix(
            "s3:default:",
            2,
            out[0].next_after.clone(),
        )])
        .await
        .unwrap();
    assert_eq!(
        out[0]
            .rows
            .iter()
            .map(|r| r.key.clone())
            .collect::<Vec<_>>(),
        vec!["s3:default:c"]
    );
    assert!(!out[0].truncated);
    assert_eq!(out[0].next_after, None);
}

#[tokio::test]
async fn a_multi_key_read_in_the_same_batch_sees_the_batch_s_own_writes() {
    // 024_kv.sql:939 orders getMany/getPrefix into a SECOND phase, after every
    // write. A caller that relies on it against the real broker is right, so
    // the double must have it too.
    let q = FakeQueen::new();
    let out = q
        .kv(vec![
            KvOp::get_prefix("s3:", 10, None),
            KvOp::put("s3:written-now", serde_json::json!(1)),
        ])
        .await
        .unwrap();
    assert_eq!(out[0].op, "getPrefix");
    assert_eq!(out[0].rows.len(), 1, "the read ran after the write");
    assert_eq!(out[1].op, "put");
}

#[tokio::test]
async fn a_ttl_expires_and_a_forever_write_does_not() {
    let q = FakeQueen::new();
    q.set_now_ms(1_000_000);
    q.kv(vec![
        KvOp::put_ttl("lease", serde_json::json!({"instance": "a"}), 30),
        KvOp::put("committed", serde_json::json!({"k": 1})),
    ])
    .await
    .unwrap();
    assert!(q.kv_get("lease").is_some());

    q.advance_ms(29_000);
    assert!(q.kv_get("lease").is_some(), "still inside the TTL");

    q.advance_ms(2_000);
    assert_eq!(q.kv_get("lease"), None, "expired");
    assert_eq!(
        q.kv_version("lease"),
        0,
        "an expired row reads as version 0"
    );
    assert!(q.kv_get("committed").is_some(), "forever means forever");

    // And the claim wins against the expired-but-unpruned row, which is what
    // lets an instance reclaim its own lease after a restart.
    let out = q
        .kv(vec![KvOp::put_if_absent_ttl(
            "lease",
            serde_json::json!({"instance": "a"}),
            30,
        )])
        .await
        .unwrap();
    assert!(out[0].did_apply());
}

#[tokio::test]
async fn delete_is_conditional_too() {
    let q = FakeQueen::new();
    q.kv_seed("k", serde_json::json!(1));
    let version = q.kv_version("k");
    let out = q
        .kv(vec![KvOp::delete("k", Some(version + 5))])
        .await
        .unwrap();
    assert_eq!(out[0].applied, Some(false));
    assert_eq!(out[0].reason.as_deref(), Some("version"));
    assert!(q.kv_get("k").is_some());

    let out = q.kv(vec![KvOp::delete("k", Some(version))]).await.unwrap();
    assert!(out[0].did_apply());
    assert_eq!(q.kv_get("k"), None);
    // Deleting what is not there, unconditionally, is a success.
    assert!(q.kv(vec![KvOp::delete("k", None)]).await.unwrap()[0].did_apply());
}

// ---------------------------------------------------------------------------
// Fault injection and inspection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fail_next_and_fail_kv_next_are_separate_and_exact() {
    let q = FakeQueen::new();
    q.push("orders", "a", ts("2026-09-04T10:00:00Z"), &["1"]);

    q.fail_next(2);
    assert!(q.fetch(vec![entry("orders", "a", 0)], 0, 1).await.is_err());
    assert!(q
        .partitions_changed(vec![changed("orders", None, None, 10)])
        .await
        .is_err());
    assert!(q.fetch(vec![entry("orders", "a", 0)], 0, 1).await.is_ok());
    // KV is on its own counter: a failing read must not imply a failing commit.
    assert!(q.kv(vec![KvOp::get("k")]).await.is_ok());

    q.fail_kv_next(1);
    assert!(q.kv(vec![KvOp::get("k")]).await.is_err());
    assert!(q.kv(vec![KvOp::get("k")]).await.is_ok());
}

#[tokio::test]
async fn the_double_records_what_it_was_asked() {
    let q = FakeQueen::new();
    q.push("orders", "a", ts("2026-09-04T10:00:00Z"), &["1"]);
    q.fetch(vec![entry("orders", "a", 0)], 0, 1).await.unwrap();
    q.partitions_changed(vec![changed("orders", None, None, 10)])
        .await
        .unwrap();
    q.kv(vec![KvOp::fence("lease", serde_json::json!(1), 0)])
        .await
        .unwrap();
    assert_eq!(q.fetch_calls(), 1);
    assert_eq!(q.changed_calls(), 1);
    assert_eq!(q.kv_calls(), 1);
    let batches = q.kv_batches();
    assert_eq!(batches.len(), 1);
    assert!(batches[0][0].is_required(), "the fence was at index 0");
    assert_eq!(batches[0][0].key(), Some("lease"));
    assert_eq!(q.kv_keys(), vec!["lease"]);
    assert_eq!(q.list_queues().await.unwrap(), vec!["orders"]);
}
