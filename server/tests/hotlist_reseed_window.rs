//! Contract test for the WINDOWED hot-list reseed (19-wildcard-hotlist §8).
//!
//! `queen.log_hotlist_reseed_window_v1` is the bounded twin of
//! `log_hotlist_reseed_v1`: the same enumeration of a (queue, group)'s probably
//! pending partitions, restricted to the ones written in the last `p_window_ms`.
//! The reseed floor runs it between full walks, so if it ever answered a
//! different question than the full walk the ring would quietly go blind, and
//! the symptom would be undelivered messages rather than an error.
//!
//! The unit tests in `hotlist.rs` cover the POLICY (which scan runs when). This
//! covers the SQL, which they cannot reach: set equivalence with the full walk,
//! the bound itself, and keyset pagination across `last_write_at` ties. Those
//! ties are not exotic — the push path quantizes `last_write_at` to at most one
//! bump per second per partition, so a busy queue routinely has thousands of
//! partitions sharing one value, and a cursor that mishandled them would either
//! loop forever or skip a page.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`:
//!
//! ```bash
//! docker run --rm -d --name queen-window-pg -e POSTGRES_PASSWORD=postgres -p 5465:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5465 cargo test --test hotlist_reseed_window -- --ignored --nocapture
//! ```
//!
//! Its own test binary (and so its own process) because the admission arbiter is
//! process-global and `embedded_smoke` drives a Broker of its own.

use queen::{Broker, BrokerConfig};

const NIL: &str = "00000000-0000-0000-0000-000000000000";

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

/// Every partition id the FULL walk returns, in one page.
async fn full_walk(c: &tokio_postgres::Client, queue: &str, group: &str) -> Vec<String> {
    c.query(
        "SELECT r_id::text FROM queen.log_hotlist_reseed_v1($1,$2,$3::text::uuid,$4)",
        &[&queue, &group, &NIL, &1_000_000i32],
    )
    .await
    .expect("full walk")
    .iter()
    .map(|r| r.get::<_, String>(0))
    .collect()
}

/// Every partition id the WINDOWED walk returns for `window_ms`, paginated at
/// `page` rows exactly the way `hotlist_reseed_run` paginates it: cursor
/// ('-infinity', nil) to start, then the last row's (write, id).
async fn windowed_walk(
    c: &tokio_postgres::Client,
    queue: &str,
    group: &str,
    window_ms: i64,
    page: i32,
) -> (Vec<String>, usize) {
    let mut after_write = "-infinity".to_string();
    let mut after_id = NIL.to_string();
    let mut out = Vec::new();
    let mut pages = 0usize;
    loop {
        let rows = c
            .query(
                "SELECT r_id::text, r_write::text FROM queen.log_hotlist_reseed_window_v1\
                 ($1,$2,$3::text::timestamptz,$4::text::uuid,$5,$6)",
                &[&queue, &group, &after_write, &after_id, &page, &window_ms],
            )
            .await
            .expect("windowed walk");
        if rows.is_empty() {
            break;
        }
        pages += 1;
        for r in &rows {
            out.push(r.get::<_, String>(0));
        }
        let last = rows.last().unwrap();
        after_id = last.get::<_, String>(0);
        after_write = last.get::<_, String>(1);
        if rows.len() < page as usize {
            break;
        }
        assert!(pages < 200, "keyset made no progress: {pages} pages");
    }
    (out, pages)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn windowed_reseed_matches_the_full_walk() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // Boot the real broker purely to apply the real schema — the SQL under test
    // is include_str!-embedded, so this is also what proves the file compiled in.
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

    let queue = unique("winq");
    let group = "winners";

    // 800 partitions, all holding data (last_offset = 10):
    //   * 1..=500 share ONE last_write_at, 5s old — the quantization tie, and it
    //     straddles four page boundaries at page = 100.
    //   * 501..=800 are spread one millisecond apart, all within the last second.
    //   * every third one is fully consumed by `group` (committed = last_offset)
    //     so it must NOT appear; the rest are behind or have no consumer row at
    //     all, which is the "never polled" shape the LEFT JOIN has to include.
    c.execute(
        "INSERT INTO queen.queues(name) VALUES ($1) ON CONFLICT DO NOTHING",
        &[&queue],
    )
    .await
    .expect("queue");
    c.execute(
        "INSERT INTO queen.log_partitions(queue_id,name,last_offset,log_start,last_write_at)
         SELECT (SELECT id FROM queen.queues WHERE name=$1), 'p'||g, 10, 0,
                CASE WHEN g <= 500 THEN now() - interval '5 seconds'
                     ELSE now() - (g || ' milliseconds')::interval END
         FROM generate_series(1,800) g",
        &[&queue],
    )
    .await
    .expect("partitions");
    // committed = 10 (consumed) for every third, 3 (behind) for another third,
    // and no row at all for the last third.
    c.execute(
        "INSERT INTO queen.log_consumers(partition_id,consumer_group,committed)
         SELECT p.id, $2, CASE WHEN (substring(p.name from 2))::int % 3 = 0 THEN 10 ELSE 3 END
         FROM queen.log_partitions p JOIN queen.queues q ON q.id = p.queue_id
         WHERE q.name = $1 AND (substring(p.name from 2))::int % 3 <> 1",
        &[&queue, &group],
    )
    .await
    .expect("consumers");
    c.execute("ANALYZE queen.log_partitions", &[]).await.ok();
    c.execute("ANALYZE queen.log_consumers", &[]).await.ok();

    // ---------------------------------------------------------- equivalence
    let mut full = full_walk(&c, &queue, group).await;
    // An hour-wide window covers every partition seeded above, so the two must
    // agree exactly — same rows, no extras in either direction. This is the
    // property the whole patch rests on.
    let (mut wide, _) = windowed_walk(&c, &queue, group, 3_600_000, 1_000_000).await;
    full.sort();
    wide.sort();
    assert_eq!(
        full.len(),
        800 - 800 / 3,
        "the full walk must skip exactly the fully consumed third"
    );
    assert_eq!(
        wide, full,
        "a window wider than the data must return exactly the full walk's set"
    );

    // ------------------------------------------------------------- the bound
    // 2s excludes the 5s-old tie block and keeps the recent spread.
    let (narrow, _) = windowed_walk(&c, &queue, group, 2_000, 1_000_000).await;
    assert!(
        narrow.len() < full.len() && !narrow.is_empty(),
        "a 2s window must be a strict non-empty subset, got {} of {}",
        narrow.len(),
        full.len()
    );
    let narrow_set: std::collections::HashSet<_> = narrow.iter().collect();
    let full_set: std::collections::HashSet<_> = full.iter().collect();
    assert!(
        narrow_set.is_subset(&full_set),
        "the window must never invent a partition the full walk does not return"
    );

    // ------------------------------------------------- pagination over ties
    // Same query, 100 rows a page: 500 partitions share one timestamp, so five
    // consecutive pages land inside a single tie group. The (write, id) cursor
    // has to advance by id there or it would re-serve the same page forever.
    let (paged, pages) = windowed_walk(&c, &queue, group, 3_600_000, 100).await;
    assert!(pages > 4, "expected several pages at 100/page, got {pages}");
    let unique_paged: std::collections::HashSet<_> = paged.iter().cloned().collect();
    assert_eq!(
        unique_paged.len(),
        paged.len(),
        "keyset returned {} duplicate rows across {pages} pages",
        paged.len() - unique_paged.len()
    );
    let mut paged_sorted = paged.clone();
    paged_sorted.sort();
    assert_eq!(
        paged_sorted, full,
        "paginated windowed walk must cover the full set exactly once"
    );

    // ------------------------------------------------- a zero-width window
    // window_ms = 0 is "written at or after now", which no seeded row satisfies.
    // It must return nothing rather than error or fall back to everything: the
    // broker never passes 0, but a misconfigured knob must fail closed and be
    // healed by the full walk, not silently scan the whole queue.
    let (empty, _) = windowed_walk(&c, &queue, group, 0, 1_000_000).await;
    assert!(empty.is_empty(), "a zero window returned {} rows", empty.len());

    broker.shutdown().await;
}
