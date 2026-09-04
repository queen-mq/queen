//! THE RETENTION SINK HOLD — PLAN_S3_SINK.md §5.3, decision D5 — end to end
//! against a live Postgres.
//!
//! Kafka's tiered storage never deletes a local segment before it is uploaded.
//! Queen's equivalent is one extra term in the per-queue cutoffs the retention
//! cycle already computes caller-side (fact F13): a queue whose
//! `retentionSinkHold` names an S3 sink has its two segment-deleting cutoffs
//! floored at what that sink has already committed to the lake, so §4.6's
//! "retention overrun — the one failure that is data loss" stops being possible
//! by construction rather than by an operator watching a lag metric.
//!
//! The floor, in one line, and every clause of it is asserted below:
//!
//!     floor = GREATEST(committed.tEnd - 60 s, now() - retentionSinkHoldMaxSeconds)
//!     all_cutoff       = LEAST(all_cutoff,       floor)
//!     completed_cutoff = LEAST(completed_cutoff, floor)
//!
//!   1. **A committed pointer holds.** Segments stamped at or after
//!      `tEnd - 60 s` survive a full cycle; older ones go. (§4.2: `tEnd` is an
//!      EXCLUSIVE bound, so the slack is what covers a segment written a hair
//!      under it.)
//!   2. **A sink that has never committed still protects**, up to the cap —
//!      which is what makes the option safe to switch on BEFORE the sink runs,
//!      and is the reason the missing-pointer case is `-∞` and not `now()`.
//!   3. **The CAP always wins over a stale pointer**, which is the whole reason
//!      D5 could ship: a stopped sink is otherwise unbounded retention, and the
//!      first symptom of unbounded retention is a full disk (§12).
//!   4. **A queue with no hold is byte-identical** to a pre-feature one.
//!   5. **The key is the sink's own**, escaping included: a queue named
//!      `a b/c` finds `s3:default:a%20b%2Fc:committed` and NOT the unescaped
//!      spelling — the property that stops queue `a:b` from composing another
//!      pair's key (connectors/queen-s3/src/layout.rs).
//!   6. **The two options round-trip through `/configure`**, and an illegal one
//!      is REFUSED rather than clamped: both govern deletion, so a silently
//!      corrected typo is either a hole in the lake or retention parked for
//!      years.
//!
//! DETERMINISM, and it is the same mechanism as tests/retention_work_list.rs:
//! the broker's REAL retention loop runs the assertions — nothing here calls a
//! step function by hand — but it is not raced. Cycles are gated through the
//! durable schedule (queen.maintenance_leases, 029): `enabled = false` parks
//! the loop, and one cycle is released by setting `next_due_at = now()` and
//! waiting for `runs` to tick.
//!
//! Its OWN scratch DATABASE, unlike retention_work_list.rs: this suite parks
//! the same lease rows, so sharing `postgres` with it would make either one's
//! result depend on which ran first.
//!
//! ```bash
//! docker run --rm -d --name queen-s3-hold-pg -e POSTGRES_PASSWORD=postgres \
//!   -p 5471:5432 postgres:16
//! QUEEN_EMBEDDED_TEST_PG=localhost:5471 \
//!   cargo test --test retention_sink_hold -- --ignored --nocapture
//! ```

use serde_json::{json, Value};

use queen::{Broker, BrokerConfig};

/// Cycle cadence for the test broker. Long enough that a released cycle is
/// followed by a quiet window, short enough that `poll_interval` (period/10,
/// floored at 1 s) releases it within ~2 s.
const INTERVAL_MS: &str = "20000";

/// The default tenant every fixture here writes under — the same one a
/// tenancy-off broker resolves to, so the KV probe's `k.tenant_id =
/// q.tenant_id` join is exercised with the value production uses.
const TENANT: &str = "00000000-0000-0000-0000-000000000001";

/// The sink name the fixtures use. Legal under the option's own character set,
/// and the middle segment of every key below.
const SINK: &str = "default";

fn nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", nanos())
}

async fn connect(host: &str, port: u16, db: &str) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres password=postgres dbname={db}"),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap_or_else(|e| panic!("connect to {db}: {e}"));
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

// ---------------------------------------------------------------------------
// Schedule control (029_maintenance_leases) — verbatim from
// tests/retention_work_list.rs, which is the precedent for driving the real
// loop one cycle at a time.
// ---------------------------------------------------------------------------

async fn park(c: &tokio_postgres::Client, task: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let n = c
            .execute(
                "UPDATE queen.maintenance_leases SET enabled = false WHERE task = $1",
                &[&task],
            )
            .await
            .expect("park");
        if n == 1 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "task {task} never registered a lease row"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn runs(c: &tokio_postgres::Client, task: &str) -> i64 {
    c.query_one(
        "SELECT runs FROM queen.maintenance_leases WHERE task = $1",
        &[&task],
    )
    .await
    .expect("runs")
    .get(0)
}

/// Release EXACTLY one cycle of a parked task and wait for it to complete.
async fn one_cycle(c: &tokio_postgres::Client, task: &str) {
    let before = runs(c, task).await;
    c.execute(
        "UPDATE queen.maintenance_leases SET enabled = true, next_due_at = now(), \
         lease_until = NULL WHERE task = $1",
        &[&task],
    )
    .await
    .expect("release");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while runs(c, task).await == before {
        assert!(
            std::time::Instant::now() < deadline,
            "task {task} never ran its released cycle"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    c.execute(
        "UPDATE queen.maintenance_leases SET enabled = false WHERE task = $1",
        &[&task],
    )
    .await
    .expect("re-park");
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// `/configure` through the SP the HTTP handler calls, so every assertion about
/// the two options exercises the real write path and the real echo.
async fn configure(c: &tokio_postgres::Client, queue: &str, options: Value) -> Value {
    let txt: String = c
        .query_one(
            "SELECT (queen.configure_queue_v1($1, $2::text::jsonb, $3::text::uuid))::text",
            &[&queue, &options.to_string(), &TENANT],
        )
        .await
        .expect("configure_queue_v1")
        .get(0);
    serde_json::from_str(&txt).expect("configure json")
}

/// The option bag a held queue uses: retention on with a 1-second window, so
/// WITHOUT the floor every segment in these fixtures is deletable and the only
/// thing that can save one is the sink hold.
fn held(sink: &str, max_seconds: i64) -> Value {
    json!({
        "retentionEnabled": true,
        "retentionSeconds": 1,
        "retentionSinkHold": sink,
        "retentionSinkHoldMaxSeconds": max_seconds,
    })
}

async fn queue_id(c: &tokio_postgres::Client, queue: &str) -> String {
    c.query_one(
        "SELECT id::text FROM queen.queues WHERE name = $1 AND tenant_id = $2::text::uuid",
        &[&queue, &TENANT],
    )
    .await
    .expect("queue id")
    .get(0)
}

/// Push one segment through the REAL allocator, the only writer of
/// log_segments. `nonce` keeps the hash blob unique so the dedup probe does not
/// answer "duplicate" (which writes nothing at all).
async fn push(c: &tokio_postgres::Client, queue: &str, partition: &str, nonce: i32) {
    let r: String = c
        .query_one(
            "SELECT (queen.log_push_one_v1($1, $2, 1, \
             decode(lpad(to_hex($3::int), 32, '0'), 'hex'), -1, ''::bytea))::text",
            &[&queue, &partition, &nonce],
        )
        .await
        .expect("push")
        .get(0);
    assert!(r.contains("queued"), "push must land: {r}");
}

async fn pid_of(c: &tokio_postgres::Client, queue_id: &str, partition: &str) -> String {
    c.query_one(
        "SELECT p.id::text FROM queen.log_partitions p \
         WHERE p.queue_id = $1::text::uuid AND p.name = $2",
        &[&queue_id, &partition],
    )
    .await
    .expect("pid")
    .get(0)
}

/// Stamp the partition's segments at the given ages, oldest first, in
/// base_offset order — the shape a real log has — and carry the work-list
/// watermark with them, exactly as tests/retention_work_list.rs's `age` does.
/// Ages are in minutes so a fixture can sit either side of a 60-second slack.
async fn age_segments(c: &tokio_postgres::Client, pid: &str, minutes_ago: &[i64]) {
    let offsets: Vec<i64> = c
        .query(
            "SELECT base_offset FROM queen.log_segments WHERE partition_id = $1::text::uuid \
             ORDER BY base_offset",
            &[&pid],
        )
        .await
        .expect("segments")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        offsets.len(),
        minutes_ago.len(),
        "the fixture must name one age per segment"
    );
    for (base, mins) in offsets.iter().zip(minutes_ago) {
        c.execute(
            "UPDATE queen.log_segments SET created_at = now() - make_interval(mins => $3::int) \
             WHERE partition_id = $1::text::uuid AND base_offset = $2",
            &[&pid, base, &(*mins as i32)],
        )
        .await
        .expect("age segment");
    }
    c.execute(
        "UPDATE queen.log_partitions p SET
             oldest_live_at = (SELECT s.created_at FROM queen.log_segments s
                               WHERE s.partition_id = p.id AND s.base_offset >= p.log_start
                               ORDER BY s.base_offset LIMIT 1)
         WHERE p.id = $1::text::uuid",
        &[&pid],
    )
    .await
    .expect("age watermark");
}

async fn segment_count(c: &tokio_postgres::Client, pid: &str) -> i64 {
    c.query_one(
        "SELECT count(*) FROM queen.log_segments WHERE partition_id = $1::text::uuid",
        &[&pid],
    )
    .await
    .expect("count")
    .get(0)
}

/// Write the sink's commit pointer through the REAL KV write path
/// (`queen.kv_apply_v1`) with the connector's own op — `{"op":"put",
/// "ns":"queen-s3", "forever":true}` — so what retention reads is what a sink
/// would actually have written, not a hand-made row.
///
/// `t_end_minutes_ago` becomes the `tEnd` ISO-microsecond UTC string the sink
/// emits; rendered by POSTGRES, never by this process's clock, which is the
/// same rule §12 puts on the sink itself.
async fn commit_pointer(c: &tokio_postgres::Client, key: &str, t_end_minutes_ago: i64) {
    let t_end: String = c
        .query_one(
            // The parentheses are load-bearing: AT TIME ZONE binds TIGHTER
            // than `-`, so without them PostgreSQL tries to convert the
            // INTERVAL and answers 42883 (the to_char/UTC sweep's own trap).
            "SELECT to_char((now() - make_interval(mins => $1::int)) AT TIME ZONE 'UTC', \
             'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')",
            &[&(t_end_minutes_ago as i32)],
        )
        .await
        .expect("t_end")
        .get(0);
    let ops = json!([{
        "op": "put",
        "ns": "queen-s3",
        "key": key,
        "forever": true,
        "value": {
            "k": 42,
            "tEnd": t_end,
            "manifest": "_queen/q/windows/0000000042.json",
            "records": 1000,
            "bytes": 4096,
            "committedAt": 0
        }
    }]);
    let out: String = c
        .query_one(
            "SELECT (queen.kv_apply_v1($1::text::jsonb, $2::text::uuid, now(), false))::text",
            &[&ops.to_string(), &TENANT],
        )
        .await
        .expect("kv_apply_v1")
        .get(0);
    let res: Value = serde_json::from_str(&out).expect("kv_apply json");
    assert_eq!(
        res[0]["applied"],
        json!(true),
        "the pointer must be written: {out}"
    );
}

/// Set a raw pointer value (a string that is not a timestamp, an object, …)
/// without going through the sink's own shape — for the totality assertions.
async fn commit_raw(c: &tokio_postgres::Client, key: &str, value: Value) {
    let ops = json!([{ "op": "put", "ns": "queen-s3", "key": key,
                       "forever": true, "value": value }]);
    let out: String = c
        .query_one(
            "SELECT (queen.kv_apply_v1($1::text::jsonb, $2::text::uuid, now(), false))::text",
            &[&ops.to_string(), &TENANT],
        )
        .await
        .expect("kv_apply_v1")
        .get(0);
    let res: Value = serde_json::from_str(&out).expect("kv_apply json");
    assert_eq!(
        res[0]["applied"],
        json!(true),
        "the pointer must be written: {out}"
    );
}

/// A queue with `n` segments aged as given, ready for one cycle. Returns the
/// partition id.
async fn seeded_queue(
    c: &tokio_postgres::Client,
    queue: &str,
    options: Value,
    minutes_ago: &[i64],
) -> String {
    let cfg = configure(c, queue, options).await;
    assert_eq!(cfg["configured"], json!(true), "configure failed: {cfg}");
    for n in 0..minutes_ago.len() {
        push(c, queue, "p", n as i32 + 1).await;
    }
    let qid = queue_id(c, queue).await;
    let pid = pid_of(c, &qid, "p").await;
    age_segments(c, &pid, minutes_ago).await;
    assert_eq!(
        segment_count(c, &pid).await,
        minutes_ago.len() as i64,
        "the fixture must start with every segment present"
    );
    pid
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn retention_holds_at_what_the_sink_has_committed() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // Own scratch database: this suite parks the same lease rows as
    // tests/retention_work_list.rs.
    let admin = connect(&host, port, "postgres").await;
    for row in admin
        .query(
            "SELECT datname FROM pg_database WHERE datname LIKE 'qsink\\_hold\\_%'",
            &[],
        )
        .await
        .expect("list leftovers")
    {
        let old: String = row.get(0);
        let _ = admin
            .execute(
                &format!("DROP DATABASE IF EXISTS \"{old}\" WITH (FORCE)"),
                &[],
            )
            .await;
    }
    let db = format!("qsink_hold_{}", nanos());
    admin
        .execute(&format!("CREATE DATABASE \"{db}\""), &[])
        .await
        .expect("create database");

    // Set BEFORE the boot: Config::load reads the environment once.
    std::env::set_var("RETENTION_INTERVAL", INTERVAL_MS);
    std::env::set_var("QUEEN_RETENTION_SAFETY_WALK_MS", INTERVAL_MS);

    let broker = Broker::start(
        BrokerConfig::new()
            .pg(host.clone(), port, "postgres", "postgres", db.clone())
            .pool_size(8),
    )
    .await
    .expect("broker start");

    let c = connect(&host, port, &db).await;
    park(&c, "retention").await;
    park(&c, "retention_watermark").await;

    // =============================================================== 6
    // CONFIGURE — round trip and refusal. First, because everything below
    // depends on these two options actually reaching the row.

    {
        let q = unique("cfg");
        let echo = configure(&c, &q, held("lake-1", 120)).await;
        assert_eq!(echo["options"]["retentionSinkHold"], json!("lake-1"));
        assert_eq!(echo["options"]["retentionSinkHoldMaxSeconds"], json!(120));
        let row = c
            .query_one(
                "SELECT retention_sink_hold, retention_sink_hold_max_seconds \
                 FROM queen.queues WHERE name = $1 AND tenant_id = $2::text::uuid",
                &[&q, &TENANT],
            )
            .await
            .expect("stored options");
        assert_eq!(row.get::<_, String>(0), "lake-1");
        assert_eq!(row.get::<_, i32>(1), 120);

        // OFF is the default, and it is what a queue that never mentions the
        // options gets — the byte-identical pre-feature answer.
        let bare = configure(&c, &unique("cfg-bare"), json!({})).await;
        assert_eq!(bare["options"]["retentionSinkHold"], json!(""));
        assert_eq!(
            bare["options"]["retentionSinkHoldMaxSeconds"],
            json!(604_800)
        );

        // REFUSED, not clamped — and the queue keeps what it had. A sink name
        // carrying ':' would compose another (sink, queue) pair's key; a
        // max-seconds outside the band is either a hole in the lake or
        // retention parked for years.
        for bad in [
            json!({"retentionSinkHold": "bad:name"}),
            json!({"retentionSinkHold": "a/b"}),
            json!({"retentionSinkHold": "x".repeat(65)}),
            json!({"retentionSinkHoldMaxSeconds": 59}),
            json!({"retentionSinkHoldMaxSeconds": 0}),
            json!({"retentionSinkHoldMaxSeconds": 31_536_001}),
        ] {
            let r = configure(&c, &q, bad.clone()).await;
            assert!(r.get("error").is_some(), "{bad} must be refused, got {r}");
            assert!(
                r.get("configured").is_none(),
                "a refusal configures nothing: {r}"
            );
        }
        let row = c
            .query_one(
                "SELECT retention_sink_hold, retention_sink_hold_max_seconds \
                 FROM queen.queues WHERE name = $1 AND tenant_id = $2::text::uuid",
                &[&q, &TENANT],
            )
            .await
            .expect("stored options after refusals");
        assert_eq!(
            (row.get::<_, String>(0), row.get::<_, i32>(1)),
            ("lake-1".to_string(), 120),
            "a refused /configure must leave the queue exactly as it was"
        );

        // The BOUNDS themselves are legal, on both ends.
        for ok in [60, 31_536_000] {
            let r = configure(
                &c,
                &unique("cfg-edge"),
                json!({"retentionSinkHoldMaxSeconds": ok}),
            )
            .await;
            assert_eq!(
                r["options"]["retentionSinkHoldMaxSeconds"],
                json!(ok),
                "{r}"
            );
        }
        // '' is legal and is how a full-replace /configure turns the hold OFF.
        let off = configure(&c, &q, json!({"retentionSinkHold": ""})).await;
        assert_eq!(off["options"]["retentionSinkHold"], json!(""));
    }

    // =============================================================== 1..5
    // Every queue below is seeded BEFORE the single cycle that judges them all:
    // one cycle, one work list, one now(), which is also the honest way to see
    // that the floors are per-queue and do not leak between them.

    // 1 — a committed pointer holds. tEnd is 120 minutes ago and the slack is
    // 60 s, so the floor is t-121m: the segments at 300 and 180 minutes are
    // below it and go, the one at 121 minutes is BELOW it too (121m > 121m is
    // false — it sits exactly on the boundary, so it is kept), and the one at
    // 30 minutes is well above.
    let q_held = unique("held");
    let p_held = seeded_queue(
        &c,
        &q_held,
        // Cap of a year: it must never be the term that wins here, or the test
        // would pass without the pointer being read at all.
        held(SINK, 31_536_000),
        &[300, 180, 119, 30],
    )
    .await;
    commit_pointer(&c, &format!("s3:{SINK}:{q_held}:committed"), 120).await;

    // 2 — a sink that has NEVER committed keeps everything younger than the
    // cap. No KV row at all for this queue.
    let q_virgin = unique("virgin");
    let p_virgin = seeded_queue(&c, &q_virgin, held(SINK, 7_200), &[300, 30]).await;

    // 3 — the CAP wins over a stale pointer. tEnd is 10 hours behind, the cap
    // is 2 hours: the floor is now-2h, so the 300-minute segment goes even
    // though the sink says it never copied it. That is D5's decision made
    // visible — a stuck sink cannot park retention.
    let q_stale = unique("stale");
    let p_stale = seeded_queue(&c, &q_stale, held(SINK, 7_200), &[300, 30]).await;
    commit_pointer(&c, &format!("s3:{SINK}:{q_stale}:committed"), 600).await;

    // 4 — NO hold: byte-identical to a pre-feature queue. Same retention
    // window as every queue above, so the only difference is the option.
    let q_plain = unique("plain");
    let p_plain = seeded_queue(
        &c,
        &q_plain,
        json!({"retentionEnabled": true, "retentionSeconds": 1}),
        &[300, 30],
    )
    .await;

    // 5 — ESCAPING. A queue named `a b/c` must find
    // `s3:default:a%20b%2Fc:committed` — the literal key of the plan, spelled
    // out here rather than computed, so this assertion cannot agree with a
    // broken escape by sharing its bug. (Fixed names are safe: the database is
    // this test's own.)
    //
    // Discriminating on purpose: with a year-long cap, a key that does NOT
    // match leaves the floor at now-1y and NOTHING is deleted, so a miss and a
    // hit differ by the whole fixture.
    let q_esc = "a b/c";
    let p_esc = seeded_queue(&c, q_esc, held(SINK, 31_536_000), &[300, 30]).await;
    commit_pointer(&c, "s3:default:a%20b%2Fc:committed", 120).await;

    // 5b — the same shape with the pointer written at the UNESCAPED spelling.
    // It must NOT be found: the floor falls back to the cap (a year), and
    // nothing is deleted. Without this, an escape that did nothing at all would
    // still pass 5 on a name whose bytes happen to survive.
    let q_raw = "x y/z";
    let p_raw = seeded_queue(&c, q_raw, held(SINK, 31_536_000), &[300, 30]).await;
    commit_pointer(&c, &format!("s3:{SINK}:{q_raw}:committed"), 120).await;

    // 5c — TOTALITY. A pointer a tenant can write but the broker cannot read
    // must degrade to "never committed" (cap only) and must not stop the cycle
    // — for this queue or for any of the others in the same work list.
    let q_junk = unique("junk");
    let p_junk = seeded_queue(&c, &q_junk, held(SINK, 7_200), &[300, 30]).await;
    commit_raw(
        &c,
        &format!("s3:{SINK}:{q_junk}:committed"),
        json!({"k": 1, "tEnd": "not a timestamp"}),
    )
    .await;

    // 7 — A FLOOR MUST NEVER CREATE A CUTOFF. Retention is OFF on this queue
    // and it names a sink with a live pointer. `LEAST` in PostgreSQL IGNORES
    // NULLs, so a floor applied AROUND the CASE arm instead of inside it turns
    // `LEAST(NULL, floor)` into `floor` and starts deleting on a queue whose
    // operator switched retention off — the sharpest way this feature could
    // lose data, and the one a reader of the SQL is most likely to introduce.
    let q_off = unique("off");
    let p_off = seeded_queue(
        &c,
        &q_off,
        json!({
            "retentionEnabled": false,
            "retentionSeconds": 1,
            "retentionSinkHold": SINK,
            "retentionSinkHoldMaxSeconds": 7_200,
        }),
        &[300, 30],
    )
    .await;
    commit_pointer(&c, &format!("s3:{SINK}:{q_off}:committed"), 120).await;

    // ------------------------------------------------------------- ONE cycle
    one_cycle(&c, "retention").await;

    assert_eq!(
        segment_count(&c, &p_held).await,
        2,
        "1 — everything at or after tEnd-60s must survive, everything older must go"
    );
    let ages: Vec<f64> = c
        .query(
            "SELECT (EXTRACT(EPOCH FROM (now() - created_at)) / 60)::float8 \
             FROM queen.log_segments WHERE partition_id = $1::text::uuid ORDER BY base_offset",
            &[&p_held],
        )
        .await
        .expect("survivor ages")
        .iter()
        .map(|r| r.get::<_, f64>(0))
        .collect();
    assert!(
        ages.iter().all(|m| *m < 121.0),
        "1 — a survivor older than the floor: {ages:?}"
    );
    assert!(
        ages.iter().any(|m| *m > 100.0),
        "1 — the segment sitting just inside the floor was deleted: {ages:?}"
    );

    assert_eq!(
        segment_count(&c, &p_virgin).await,
        1,
        "2 — a sink that never committed must still keep everything younger than the cap"
    );
    assert_eq!(
        segment_count(&c, &p_stale).await,
        1,
        "3 — the cap must win over a stale pointer"
    );
    assert_eq!(
        segment_count(&c, &p_plain).await,
        0,
        "4 — a queue with no hold must sweep exactly as it did before the feature"
    );
    assert_eq!(
        segment_count(&c, &p_esc).await,
        1,
        "5 — the escaped key must be the one the cycle probes"
    );
    assert_eq!(
        segment_count(&c, &p_raw).await,
        2,
        "5b — an UNESCAPED key must not match, leaving the cap (a year) as the floor"
    );
    assert_eq!(
        segment_count(&c, &p_junk).await,
        1,
        "5c — an unreadable pointer must read as 'never committed', not stop the cycle"
    );
    assert_eq!(
        segment_count(&c, &p_off).await,
        2,
        "7 — a sink floor must never CREATE a cutoff on a queue whose retention is off"
    );

    // The cycle really ran to completion for everyone, which is the other half
    // of 5c: one tenant's malformed pointer must not cost anybody else a sweep.
    assert!(
        runs(&c, "retention").await > 0,
        "the released cycle must have completed"
    );

    // ------------------------------------------------- the floor MOVES
    // The floor is a fact re-read every cycle, never a cached decision: advance
    // the pointer and the next cycle deletes what the previous one held.
    // 60 minutes back = a floor at 61 minutes: the 119-minute survivor of the
    // first cycle is now below it and goes, the 30-minute one is still above.
    commit_pointer(&c, &format!("s3:{SINK}:{q_held}:committed"), 60).await;
    one_cycle(&c, "retention").await;
    assert_eq!(
        segment_count(&c, &p_held).await,
        1,
        "the floor must follow the pointer forward on the very next cycle"
    );

    // ...and turning the hold OFF returns the queue to plain retention.
    let cfg = configure(
        &c,
        &q_virgin,
        json!({"retentionEnabled": true, "retentionSeconds": 1}),
    )
    .await;
    assert_eq!(cfg["options"]["retentionSinkHold"], json!(""));
    one_cycle(&c, "retention").await;
    assert_eq!(
        segment_count(&c, &p_virgin).await,
        0,
        "clearing retentionSinkHold must restore the unfloored cutoff"
    );

    drop(c);
    drop(broker);
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)"),
            &[],
        )
        .await
        .expect("drop scratch database");
}
