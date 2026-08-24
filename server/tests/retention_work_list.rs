//! The 2026-08-24 retention WORK-LIST redesign, end to end against a live
//! Postgres (companion to tests/retention_lock_scope.rs, which covers the belt
//! lock and the windowed boundary walks of the same incident).
//!
//! What broke: the cycle materialized ONE ROW PER PARTITION — `queen.queues
//! JOIN queen.log_partitions ORDER BY p.id`, 827k rows on the soak cell — and
//! then made a 2-5 ms step call for every one of them, 99.9% of which found
//! nothing eligible. Measured delete rate ~20 segments/s against ~500/s
//! arriving: 25x behind, permanently. The fix gives every partition an INDEXED
//! scalar for the age of its own oldest live data (`log_partitions
//! .oldest_live_at` / `oldest_txn_at`, 001_log_schema) so the cycle asks each
//! queue's index for the partitions that can actually yield something.
//!
//! The four things that must hold, and are asserted below:
//!
//!   1. MAINTENANCE — the columns track reality on every path that moves a
//!      watermark: the retention step (advance, and NULL when the partition
//!      empties), the sidecar purge, and the push allocator's COALESCE, whose
//!      whole point is that a partition which already holds data gets a
//!      BYTE-IDENTICAL write (the HOT discipline it inherits from the
//!      last_write_at quantization it sits beside). Both push branches — dedup
//!      on and dedup off — are exercised, because they are two separate UPDATE
//!      statements.
//!   2. SELECTION — a cycle sweeps EXACTLY the due partitions, oldest first,
//!      capped at QUEEN_RETENTION_DUE_CAP, and the rest are the next cycle's
//!      head rather than lost.
//!   3. POLICY IS NOT CACHED — the column holds the age of the data, never
//!      eligibility, so editing retention_seconds changes what gets swept on
//!      the NEXT cycle with no invalidation step anywhere.
//!   4. SELF-HEALING — a watermark that drifts from reality (the failure mode a
//!      cached fact introduces: a future writer that forgets the column) hides
//!      a partition from retention, and the safety walk repairs it.
//!
//! Plus the shape pin: the Θ(#partitions) probe is gone from the cycle path.
//!
//! DETERMINISM. The broker's real retention loop runs the assertions, but it is
//! not raced: cycles are gated through the durable schedule
//! (queen.maintenance_leases, 029) — `enabled = false` parks the loop, and one
//! cycle is released by setting `next_due_at = now()` and waiting for `runs` to
//! tick. With RETENTION_INTERVAL at 20 s the released cycle is followed by 20 s
//! of quiet, which is the observation window.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`:
//!
//! ```bash
//! docker run --rm -d --name queen-retwork-pg -e POSTGRES_PASSWORD=postgres -p 5477:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5477 cargo test --test retention_work_list -- --ignored --nocapture
//! ```

use queen::{Broker, BrokerConfig};

/// Cycle cadence for the test broker. Long enough that a released cycle is
/// followed by a quiet window, short enough that `poll_interval` (period/10,
/// floored at 1 s) releases it within ~2 s.
const INTERVAL_MS: &str = "20000";

/// Due cap for the test broker: two partitions per queue per rule per cycle, so
/// the cap is observable with a handful of rows instead of thousands.
const DUE_CAP: usize = 2;

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

async fn raw_connect(host: &str, port: u16) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres password=postgres dbname=postgres"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

// ---------------------------------------------------------------------------
// Schedule control (029_maintenance_leases)
// ---------------------------------------------------------------------------

/// Park a maintenance loop. Waits for the row first: the loops create it at
/// boot, so a test that disabled it too early would be disabling nothing.
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
/// `runs` is bumped by the release, and the release also pushes `next_due_at`
/// one full period out, so nothing follows it inside the observation window.
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

async fn make_queue(
    c: &tokio_postgres::Client,
    name: &str,
    retention_seconds: i32,
    dedup_window_seconds: i32,
) -> String {
    c.query_one(
        "INSERT INTO queen.queues(name, retention_enabled, retention_seconds, dedup_window_seconds)
         VALUES ($1, $2, $3, $4) RETURNING id::text",
        &[
            &name,
            &(retention_seconds > 0),
            &retention_seconds,
            &dedup_window_seconds,
        ],
    )
    .await
    .expect("queue")
    .get(0)
}

/// Push one segment through the REAL allocator (queen.log_push_one_v1), which
/// is the only writer of log_segments and therefore the only place the
/// empty -> non-empty watermark transition can happen. `nonce` makes the hash
/// blob unique so the dedup-ON branch does not answer "duplicate" (which writes
/// nothing at all, by design).
async fn push(c: &tokio_postgres::Client, queue: &str, partition: &str, nonce: i32) -> String {
    c.query_one(
        "SELECT (queen.log_push_one_v1($1, $2, 1, decode(lpad(to_hex($3::int), 32, '0'), 'hex'), \
         -1, ''::bytea))::text",
        &[&queue, &partition, &nonce],
    )
    .await
    .expect("push")
    .get(0)
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

/// `(oldest_live_at, oldest_txn_at)` as text, NULL preserved.
async fn watermarks(c: &tokio_postgres::Client, pid: &str) -> (Option<String>, Option<String>) {
    let r = c
        .query_one(
            "SELECT oldest_live_at::text, oldest_txn_at::text FROM queen.log_partitions \
             WHERE id = $1::text::uuid",
            &[&pid],
        )
        .await
        .expect("watermarks");
    (r.get(0), r.get(1))
}

/// created_at of the oldest LIVE segment, straight from log_segments — the
/// truth the column is supposed to mirror.
async fn true_oldest_live(c: &tokio_postgres::Client, pid: &str) -> Option<String> {
    c.query_one(
        "SELECT (SELECT s.created_at::text FROM queen.log_segments s \
                 WHERE s.partition_id = p.id AND s.base_offset >= p.log_start \
                 ORDER BY s.base_offset LIMIT 1) \
         FROM queen.log_partitions p WHERE p.id = $1::text::uuid",
        &[&pid],
    )
    .await
    .expect("truth")
    .get(0)
}

/// `oldest_live_at - (created_at of the oldest live segment)`, in milliseconds.
/// Positive = the watermark is LATER than the data it describes.
async fn live_skew_ms(c: &tokio_postgres::Client, pid: &str) -> f64 {
    // ::float8 in SQL, not a numeric decode: EXTRACT returns `numeric`, which
    // tokio_postgres only maps behind an optional feature this crate does not
    // enable.
    c.query_one(
        "SELECT (EXTRACT(EPOCH FROM (p.oldest_live_at - \
             (SELECT s.created_at FROM queen.log_segments s \
              WHERE s.partition_id = p.id AND s.base_offset >= p.log_start \
              ORDER BY s.base_offset LIMIT 1))) * 1000)::float8 \
         FROM queen.log_partitions p WHERE p.id = $1::text::uuid",
        &[&pid],
    )
    .await
    .expect("skew")
    .get(0)
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

/// Age every segment of a partition by `hours`, and carry the watermark with
/// them — the same edit a real clock would make, without waiting for it.
async fn age(c: &tokio_postgres::Client, pid: &str, hours: i32) {
    c.execute(
        "UPDATE queen.log_segments SET created_at = created_at - make_interval(hours => $2::int) \
         WHERE partition_id = $1::text::uuid",
        &[&pid, &hours],
    )
    .await
    .expect("age segments");
    c.execute(
        "UPDATE queen.log_txns SET created_at = created_at - make_interval(hours => $2::int) \
         WHERE partition_id = $1::text::uuid",
        &[&pid, &hours],
    )
    .await
    .expect("age txns");
    c.execute(
        "UPDATE queen.log_partitions p SET
             oldest_live_at = (SELECT s.created_at FROM queen.log_segments s
                               WHERE s.partition_id = p.id AND s.base_offset >= p.log_start
                               ORDER BY s.base_offset LIMIT 1),
             oldest_txn_at  = (SELECT t.created_at FROM queen.log_txns t
                               WHERE t.partition_id = p.id AND t.base_offset >= p.txns_start
                               ORDER BY t.base_offset LIMIT 1)
         WHERE p.id = $1::text::uuid",
        &[&pid],
    )
    .await
    .expect("age watermarks");
}

const RETENTION_STEP: &str = "SELECT (queen.log_retention_step_v1($1::text::uuid, \
     $2::text::timestamptz, $3::text::timestamptz, $4::int))::text";
const TXNS_STEP: &str =
    "SELECT (queen.log_txns_purge_step_v1($1::text::uuid, $2::text::timestamptz, $3::int))::text";

async fn step(
    c: &tokio_postgres::Client,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> serde_json::Value {
    let txt: String = c.query_one(sql, params).await.expect("step").get(0);
    serde_json::from_str(&txt).expect("step json")
}

/// Sum every "Buffers: shared hit=… read=…" line of a TEXT-format
/// EXPLAIN ANALYZE output (same helper as retention_lock_scope.rs §2f).
fn buffers_of(plan: &str) -> i64 {
    let mut total = 0i64;
    for line in plan.lines() {
        for key in ["hit=", "read="] {
            if let Some(pos) = line.find(key) {
                let digits: String = line[pos + key.len()..]
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect();
                total += digits.parse::<i64>().unwrap_or(0);
            }
        }
    }
    total
}

async fn explain(c: &tokio_postgres::Client, sql: &str) -> String {
    c.query(&format!("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) {sql}"), &[])
        .await
        .expect("explain")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn the_work_list_is_bounded_by_deletable_work() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // Set BEFORE the boot: Config::load reads the environment once.
    std::env::set_var("RETENTION_INTERVAL", INTERVAL_MS);
    std::env::set_var("QUEEN_RETENTION_DUE_CAP", DUE_CAP.to_string());
    // The walk is driven explicitly through its lease row below, never on its
    // own clock: determinism comes from `enabled`, not from a long period. The
    // period is short here purely so `lease::poll_interval` (period/10, capped
    // at 60 s) releases a hand-scheduled walk in ~2 s instead of a minute.
    std::env::set_var("QUEEN_RETENTION_SAFETY_WALK_MS", INTERVAL_MS);

    let _broker = Broker::start(
        BrokerConfig::new()
            .pg(host.clone(), port, "postgres", "postgres", "postgres")
            .pool_size(8),
    )
    .await
    .expect("broker start");

    let c = raw_connect(&host, port).await;
    // Park both maintenance loops: every cycle in this test is released by hand.
    park(&c, "retention").await;
    park(&c, "retention_watermark").await;

    // ================================================================ 1
    // MAINTENANCE — the columns track reality on every writing path.

    // 1a — the push allocator seeds the watermark on the EMPTY -> NON-EMPTY
    // transition and then leaves it BYTE-IDENTICAL. That identity is the whole
    // reason the COALESCE is safe to put in the hot allocator UPDATE: the two
    // columns are indexed, so a changing value would make every push non-HOT.
    // Both branches, because they are two separate UPDATE statements: dedup ON
    // (window > 0, probe-then-allocate) and dedup OFF (single UPDATE fast path).
    for (label, dedup) in [("dedup-on", 3600), ("dedup-off", 0)] {
        let qname = unique(&format!("wm-{label}"));
        let qid = make_queue(&c, &qname, 3600, dedup).await;
        assert!(push(&c, &qname, "p", 1).await.contains("queued"));
        let pid = pid_of(&c, &qid, "p").await;
        let (live1, txn1) = watermarks(&c, &pid).await;
        assert!(
            live1.is_some(),
            "{label}: first push must seed oldest_live_at"
        );
        assert!(
            txn1.is_some(),
            "{label}: first push must seed oldest_txn_at"
        );
        // The seeded value is the allocator UPDATE's own clock_timestamp(),
        // which is a hair off the segment's created_at because the two branches
        // stamp on opposite sides of that UPDATE (003_log_push): dedup-ON takes
        // v_now BEFORE it, dedup-OFF after. Both directions are harmless — a
        // value a hair LATE hides the partition for at most one cycle, a hair
        // EARLY costs one no-op step call — and the sign here is exactly that
        // mechanism, pinned so a reordering of the stamp is noticed.
        let skew = live_skew_ms(&c, &pid).await;
        assert!(
            skew.abs() < 50.0,
            "{label}: watermark/segment skew is {skew} ms, not sub-ms"
        );
        if dedup > 0 {
            assert!(
                skew >= 0.0,
                "{label}: dedup-ON stamps the segment BEFORE the allocator"
            );
        } else {
            assert!(
                skew <= 0.0,
                "{label}: dedup-OFF stamps the segment AFTER the allocator"
            );
        }
        for n in 2..=5 {
            assert!(
                push(&c, &qname, "p", n).await.contains("queued"),
                "{label}: push {n}"
            );
        }
        let (live2, txn2) = watermarks(&c, &pid).await;
        assert_eq!(
            live2, live1,
            "{label}: a non-empty partition's watermark must not move"
        );
        assert_eq!(txn2, txn1, "{label}: same for the sidecar watermark");
        assert_eq!(
            segment_count(&c, &pid).await,
            5,
            "{label}: five distinct segments"
        );
    }

    // 1b — the retention step advances the watermark to the NEW oldest live
    // segment, exactly, in the same transaction as log_start; empties it to
    // NULL; and a later push re-seeds it.
    {
        let qname = unique("wm-step");
        let qid = make_queue(&c, &qname, 3600, 0).await;
        for n in 1..=3 {
            push(&c, &qname, "p", n).await;
        }
        let pid = pid_of(&c, &qid, "p").await;
        age(&c, &pid, 2).await;
        let cutoff: String = c
            .query_one("SELECT now()::text", &[])
            .await
            .expect("now")
            .get(0);
        let none: Option<String> = None;

        for expect_left in [2i64, 1, 0] {
            let r = step(&c, RETENTION_STEP, &[&pid, &cutoff, &none, &1i32]).await;
            assert_eq!(r["deleted"].as_i64(), Some(1));
            assert_eq!(segment_count(&c, &pid).await, expect_left);
            let (live, _) = watermarks(&c, &pid).await;
            assert_eq!(
                live,
                true_oldest_live(&c, &pid).await,
                "watermark must equal the new oldest live segment (or NULL at {expect_left} left)"
            );
            if expect_left == 0 {
                assert!(
                    live.is_none(),
                    "an emptied partition must go NULL, not stale"
                );
            }
        }
        // Refill: the allocator's COALESCE puts the partition back in the index.
        push(&c, &qname, "p", 99).await;
        let (live, _) = watermarks(&c, &pid).await;
        assert!(
            live.is_some(),
            "a push into an emptied partition must re-seed the watermark"
        );
    }

    // 1c — the sidecar purge maintains its own watermark the same way. This is
    // the column that cannot be derived from the segment one: the txns cutoff
    // (dedup window, 900 s floor) is far SHORTER than a typical retention
    // window, so a segment-derived due test nominates every partition holding
    // more than an hour of data — 827k of them on the measured cell.
    {
        let qname = unique("wm-txns");
        let qid = make_queue(&c, &qname, 0, 0).await;
        for n in 1..=3 {
            push(&c, &qname, "p", n).await;
        }
        let pid = pid_of(&c, &qid, "p").await;
        age(&c, &pid, 2).await;
        let cutoff: String = c
            .query_one("SELECT now()::text", &[])
            .await
            .expect("now")
            .get(0);
        let before = watermarks(&c, &pid).await;
        assert!(before.1.is_some());
        let r = step(&c, TXNS_STEP, &[&pid, &cutoff, &2i32]).await;
        assert_eq!(r["deleted"].as_i64(), Some(2));
        let after = watermarks(&c, &pid).await;
        assert_ne!(
            after.1, before.1,
            "the sidecar watermark must advance with txns_start"
        );
        assert_eq!(
            after.0, before.0,
            "the SEGMENT watermark must not move on a sidecar purge"
        );
        step(&c, TXNS_STEP, &[&pid, &cutoff, &2i32]).await;
        assert!(
            watermarks(&c, &pid).await.1.is_none(),
            "a fully purged sidecar must go NULL, or the partition stays in the phase-2 list"
        );
    }

    // ================================================================ 2
    // SELECTION — one cycle sweeps exactly the due partitions, oldest first,
    // capped, and the remainder is the next cycle's head.
    let sel_queue = unique("sel");
    let sel_qid = make_queue(&c, &sel_queue, 3600, 0).await;
    {
        // Five partitions with data 5,4,3,2 hours old (due under a 1 h window)
        // and one 1 minute old (NOT due). Staggered ages make the ordering
        // observable: the cap must take the OLDEST first.
        for (name, hours) in [("p5", 5), ("p4", 4), ("p3", 3), ("p2", 2)] {
            push(&c, &sel_queue, name, hours).await;
            let pid = pid_of(&c, &sel_qid, name).await;
            age(&c, &pid, hours).await;
        }
        push(&c, &sel_queue, "fresh", 100).await;

        one_cycle(&c, "retention").await;
        let gone = surviving(&c, &sel_qid).await;
        assert_eq!(
            gone,
            vec!["fresh".to_string(), "p2".to_string(), "p3".to_string()],
            "the cap must take the two OLDEST due partitions (p5, p4) and no others"
        );

        one_cycle(&c, "retention").await;
        assert_eq!(
            surviving(&c, &sel_qid).await,
            vec!["fresh".to_string()],
            "the remainder must be the next cycle's head, not lost"
        );

        // The fresh partition was never even nominated, so nothing about it
        // changed — including its watermark.
        let fresh_pid = pid_of(&c, &sel_qid, "fresh").await;
        assert_eq!(segment_count(&c, &fresh_pid).await, 1);
        assert!(watermarks(&c, &fresh_pid).await.0.is_some());
    }

    // ================================================================ 3
    // POLICY IS NOT CACHED — the column is the AGE OF THE DATA, never
    // eligibility, so a retention_seconds edit lands on the next cycle with no
    // invalidation, no backfill, no restart.
    {
        let qname = unique("policy");
        let qid = make_queue(&c, &qname, 86_400, 0).await; // 24 h: nothing is due
        push(&c, &qname, "p", 7).await;
        let pid = pid_of(&c, &qid, "p").await;
        age(&c, &pid, 2).await;

        one_cycle(&c, "retention").await;
        assert_eq!(
            segment_count(&c, &pid).await,
            1,
            "2 h of data under a 24 h window survives"
        );
        let before = watermarks(&c, &pid).await;

        c.execute(
            "UPDATE queen.queues SET retention_seconds = 3600 WHERE id = $1::text::uuid",
            &[&qid],
        )
        .await
        .expect("retune");
        assert_eq!(
            watermarks(&c, &pid).await,
            before,
            "a config edit must touch NO partition state"
        );

        one_cycle(&c, "retention").await;
        assert_eq!(
            segment_count(&c, &pid).await,
            0,
            "the 1 h window applies on the next cycle"
        );
    }

    // ================================================================ 4
    // SELF-HEALING — the cost of trusting a cached fact is that a writer which
    // forgets it strands a partition. Prove both halves: drift HIDES the
    // partition from retention, and the safety walk repairs it.
    {
        let qname = unique("drift");
        let qid = make_queue(&c, &qname, 3600, 0).await;
        push(&c, &qname, "p", 11).await;
        let pid = pid_of(&c, &qid, "p").await;
        age(&c, &pid, 2).await;

        // The exact damage a forgetful writer does: the data is 2 h old, the
        // column claims it is new.
        c.execute(
            "UPDATE queen.log_partitions SET oldest_live_at = now() WHERE id = $1::text::uuid",
            &[&pid],
        )
        .await
        .expect("corrupt");
        one_cycle(&c, "retention").await;
        assert_eq!(
            segment_count(&c, &pid).await,
            1,
            "drift must genuinely hide the partition — otherwise this test proves nothing"
        );

        one_cycle(&c, "retention_watermark").await;
        assert_eq!(
            watermarks(&c, &pid).await.0,
            true_oldest_live(&c, &pid).await,
            "the safety walk must re-derive the watermark from the segments"
        );
        one_cycle(&c, "retention").await;
        assert_eq!(
            segment_count(&c, &pid).await,
            0,
            "and retention must then sweep it"
        );

        // The NULL half of the same failure: a partition whose column was
        // wrongly cleared is invisible until the walk puts it back.
        push(&c, &qname, "p", 12).await;
        age(&c, &pid, 2).await;
        c.execute(
            "UPDATE queen.log_partitions SET oldest_live_at = NULL WHERE id = $1::text::uuid",
            &[&pid],
        )
        .await
        .expect("clear");
        one_cycle(&c, "retention").await;
        assert_eq!(
            segment_count(&c, &pid).await,
            1,
            "a NULLed watermark hides the partition"
        );
        one_cycle(&c, "retention_watermark").await;
        assert!(
            watermarks(&c, &pid).await.0.is_some(),
            "the walk must restore it"
        );
        one_cycle(&c, "retention").await;
        assert_eq!(segment_count(&c, &pid).await, 0);
    }

    // ================================================================ 5
    // THE MONSTER QUERY CANNOT RECUR. The old cycle path's first statement was
    // `queen.queues JOIN queen.log_partitions ORDER BY p.id` — 827k rows on the
    // cell, and the statement whose timeout left the belt lock held for 28
    // minutes (tests/retention_lock_scope.rs). Differential buffer proof on a
    // 20k-partition queue where exactly one partition is due: the old shape
    // touches the whole partition set, the new probe touches an index range.
    {
        let qname = unique("scale");
        let qid = make_queue(&c, &qname, 3600, 0).await;
        c.execute(
            "INSERT INTO queen.log_partitions
                 (queue_id, name, last_offset, log_start, oldest_live_at)
             SELECT $1::text::uuid, 'bulk'||g, 9, 0,
                    CASE WHEN g = 1 THEN now() - interval '5 hours' ELSE now() END
             FROM generate_series(1, 20000) g",
            &[&qid],
        )
        .await
        .expect("bulk partitions");
        c.execute("ANALYZE queen.log_partitions", &[]).await.ok();

        let legacy = explain(
            &c,
            &format!(
                "SELECT p.id FROM queen.queues qq \
                 JOIN queen.log_partitions p ON p.queue_id = qq.id \
                 WHERE qq.id = '{qid}'::uuid ORDER BY p.id"
            ),
        )
        .await;
        let probe = explain(
            &c,
            &format!(
                "SELECT p.id FROM queen.log_partitions p \
                 WHERE p.queue_id = '{qid}'::uuid \
                   AND p.oldest_live_at < now() - interval '1 hour' \
                 ORDER BY p.oldest_live_at LIMIT {DUE_CAP}"
            ),
        )
        .await;
        assert!(
            probe.contains("idx_log_partitions_queue_oldest"),
            "the due probe must ride the watermark index, not a scan:\n{probe}"
        );
        let (l, p) = (buffers_of(&legacy), buffers_of(&probe));
        assert!(
            p * 20 < l,
            "the due probe must touch a small fraction of the full-partition walk's buffers \
             (probe={p}, legacy={l})\n--- probe:\n{probe}\n--- legacy:\n{legacy}"
        );

        // And behaviourally: one cycle over those 20k partitions sweeps the ONE
        // that is due and leaves the other 19 999 untouched.
        one_cycle(&c, "retention").await;
        let due_pid = pid_of(&c, &qid, "bulk1").await;
        assert!(
            watermarks(&c, &due_pid).await.0.is_some(),
            "the seeded row has no segments, so the step no-ops and the watermark stands"
        );
        let swept: i64 = c
            .query_one(
                "SELECT count(*) FROM queen.log_partitions \
                 WHERE queue_id = $1::text::uuid AND oldest_live_at IS NULL",
                &[&qid],
            )
            .await
            .expect("swept")
            .get(0);
        assert_eq!(swept, 0, "a no-op step must not invent a watermark change");
    }
}

/// Partition names still holding segments under `queue_id`, sorted.
async fn surviving(c: &tokio_postgres::Client, queue_id: &str) -> Vec<String> {
    let mut names: Vec<String> = c
        .query(
            "SELECT p.name FROM queen.log_partitions p \
             WHERE p.queue_id = $1::text::uuid \
               AND EXISTS (SELECT 1 FROM queen.log_segments s WHERE s.partition_id = p.id)",
            &[&queue_id],
        )
        .await
        .expect("surviving")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    names.sort();
    names
}
