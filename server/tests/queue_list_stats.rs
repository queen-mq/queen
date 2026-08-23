//! Contract test for the queue LIST's segment count (2026-08-23).
//!
//! `queen.log_queue_stats_all_v1` backs `GET /api/v1/resources/queues` and used to
//! COUNT a queue's segments on every call. Counting is the wrong shape for a list:
//! first it was Θ(every segment in the cell) (a CTE with no tenant predicate),
//! then Θ(the tenant's segments) (a LATERAL count through the PK), and on the
//! soak the latter went 635 -> 2,204 ms as segments grew 2M -> 9M at a constant
//! 827k partitions, because retention only starts deleting after the plan's
//! window. The count now lives where segments are scanned anyway — the
//! retained-bytes slow lane (`log_refresh_retained_bytes_v1`, 028) — and lands
//! in `queen.stats.segment_count`, which the list reads. This pins the three
//! things that make that sound:
//!
//!   * the lane writes the count and the list reports it — and nothing else
//!     writes it: the counters refresh (011) must self-assign the column or
//!     it would zero every queue's count each cycle;
//!   * the list's plan never touches `log_segments` at all, so its cost is
//!     Θ(the tenant's partitions) however many segments accumulate;
//!   * the lag semantics: a queue the lane has not passed over reads 0, and a
//!     segment added after the lane's pass is invisible until the next one —
//!     partitions and retained frames stay live.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`:
//!
//! ```bash
//! docker run --rm -d --name queen-liststats-pg -e POSTGRES_PASSWORD=postgres -p 5465:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5465 cargo test --test queue_list_stats -- --ignored --nocapture
//! ```
//!
//! Its own test binary (and so its own process) because the admission arbiter is
//! process-global and `embedded_smoke` drives a Broker of its own.

use queen::{Broker, BrokerConfig};

const DEFAULT_TENANT: &str = "00000000-0000-0000-0000-000000000001";

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

/// A queue of `parts` partitions, each holding data (last_offset = 9, i.e. ten
/// retained frames), with `segs_per_part` segments apiece.
async fn seed_queue(c: &tokio_postgres::Client, queue: &str, parts: i32, segs_per_part: i64) {
    c.execute("INSERT INTO queen.queues(name) VALUES ($1) ON CONFLICT DO NOTHING", &[&queue])
        .await
        .expect("queue");
    c.execute(
        "INSERT INTO queen.log_partitions(queue_id,name,last_offset,log_start,last_write_at)
         SELECT (SELECT id FROM queen.queues WHERE name=$1), 'p'||g, 9, 0, now()
         FROM generate_series(1,$2) g",
        &[&queue, &parts],
    )
    .await
    .expect("partitions");
    add_segments(c, queue, segs_per_part).await;
}

/// `n` more segments on every partition of `queue`, appended after its last one.
async fn add_segments(c: &tokio_postgres::Client, queue: &str, n: i64) {
    c.execute(
        "INSERT INTO queen.log_segments(partition_id, base_offset, end_offset, created_at, blob)
         SELECT p.id, COALESCE(m.next, 0) + (g - 1) * 10, COALESCE(m.next, 0) + g * 10 - 1, now(), '\\x00'::bytea
         FROM queen.log_partitions p
         JOIN queen.queues q ON q.id = p.queue_id
         LEFT JOIN LATERAL (
             SELECT max(s.end_offset) + 1 AS next FROM queen.log_segments s WHERE s.partition_id = p.id
         ) m ON true,
         generate_series(1, $2::bigint) g
         WHERE q.name = $1",
        &[&queue, &n],
    )
    .await
    .expect("segments");
}

/// (partitions, segments, messages) the LIST function reports for `queue`.
async fn listed(c: &tokio_postgres::Client, queue: &str) -> Option<(i64, i64, i64)> {
    c.query(
        "SELECT partitions, segments, messages FROM queen.log_queue_stats_all_v1($1::text::uuid) \
         WHERE queue_name = $2",
        &[&DEFAULT_TENANT, &queue],
    )
    .await
    .expect("list stats")
    .first()
    .map(|r| (r.get(0), r.get(1), r.get(2)))
}

async fn stat_segment_count(c: &tokio_postgres::Client, queue: &str) -> Option<i64> {
    c.query(
        "SELECT st.segment_count FROM queen.stats st JOIN queen.queues q ON q.id = st.queue_id \
         WHERE st.stat_type = 'queue' AND q.name = $1",
        &[&queue],
    )
    .await
    .expect("stat row")
    .first()
    .map(|r| r.get(0))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn queue_list_reads_the_segment_count_the_slow_lane_writes() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // Boot the real broker purely to apply the real schema (the ADD COLUMN and
    // both functions are include_str!-embedded, so this also proves they compiled
    // in), then talk SQL directly so each step is observable.
    let broker = Broker::start(
        BrokerConfig::new()
            .pg(host.clone(), port, "postgres", "postgres", "postgres")
            .pool_size(4),
    )
    .await
    .expect("broker start");

    let (c, conn) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres password=postgres dbname=postgres"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let queue = unique("segq");
    seed_queue(&c, &queue, 3, 2).await; // 3 partitions x 2 segments, 30 retained frames

    // ------------------------------------------- before any lane pass: live vs lagged
    // The counters refresh creates the 'queue' stat row; the segment count is the
    // lane's, so it reads the DDL default until the lane has been over this queue.
    c.execute("SELECT queen.log_refresh_all_stats_v1()", &[]).await.expect("counters refresh");
    assert_eq!(stat_segment_count(&c, &queue).await, Some(0), "the counters refresh must not write the count");
    assert_eq!(
        listed(&c, &queue).await,
        Some((3, 0, 30)),
        "partitions and frames are live, the segment count is not yet known"
    );

    // ------------------------------------------------------- the lane writes it
    let summary: String = c
        .query_one("SELECT (queen.log_refresh_retained_bytes_v1())::text", &[])
        .await
        .expect("lane")
        .get(0);
    assert!(summary.contains("\"segmentsTotal\""), "the lane's summary must report the count: {summary}");
    assert_eq!(stat_segment_count(&c, &queue).await, Some(6));
    assert_eq!(listed(&c, &queue).await, Some((3, 6, 30)), "the list reports the lane's count");

    // ------------------------------------- the counters refresh must not zero it
    // 011's upsert names every counter column; this one must be self-assigned
    // (like retained_bytes), or the list would flap between 6 and 0 every cycle.
    c.execute("SELECT queen.log_refresh_all_stats_v1()", &[]).await.expect("counters refresh");
    assert_eq!(stat_segment_count(&c, &queue).await, Some(6), "011 zeroed segment_count — it must self-assign it");

    // ------------------------------------------------------------ the lag contract
    // Segments appended after the pass are invisible to the LIST until the next
    // one — the retained frames, which are watermark arithmetic, stay live.
    add_segments(&c, &queue, 1).await; // 3 more segments, 30 more frames
    c.execute(
        "UPDATE queen.log_partitions p SET last_offset = 19 \
         FROM queen.queues q WHERE q.id = p.queue_id AND q.name = $1",
        &[&queue],
    )
    .await
    .expect("advance watermarks");
    assert_eq!(listed(&c, &queue).await, Some((3, 6, 60)), "frames live, segment count lagged");
    c.execute("SELECT queen.log_refresh_retained_bytes_v1()", &[]).await.expect("lane");
    assert_eq!(listed(&c, &queue).await, Some((3, 9, 60)), "caught up on the next pass");

    // A queue the lane has never seen: partitions live, count 0, no error.
    let fresh = unique("freshq");
    seed_queue(&c, &fresh, 2, 4).await;
    assert_eq!(listed(&c, &fresh).await, Some((2, 0, 20)));

    // -------------------------------------------- the plan never opens log_segments
    // This is the property the whole move buys: however many segments a tenant
    // retains, the list reads partitions (live) and one stat row per queue.
    let plan: String = c
        .query(
            "EXPLAIN (COSTS OFF) SELECT * FROM queen.log_queue_stats_all_v1($1::text::uuid)",
            &[&DEFAULT_TENANT],
        )
        .await
        .expect("explain")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !plan.contains("log_segments"),
        "the queue list must not touch log_segments (Θ(segments) per call):\n{plan}"
    );
    assert!(plan.contains("log_partitions"), "partitions are still counted live:\n{plan}");

    broker.shutdown().await;
}
