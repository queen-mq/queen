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
//! the bound itself, keyset pagination across `last_write_at` ties, and the
//! bound holding still for the length of a walk. Those ties are not exotic — the
//! push path quantizes `last_write_at` to at most one bump per second per
//! partition, so a busy queue routinely has thousands of partitions sharing one
//! value, and a cursor that mishandled them would either loop forever or skip a
//! page.
//!
//! Since 2026-08-23 the broker's FULL walk is this statement too, with the cutoff
//! pinned to '-infinity' from the first page (`ReseedMode::scan_bounds`): the
//! dedicated full-walk statement read every partition in the cell under the generic
//! plan (its header in 004_log_pop.sql). So the equivalence below is the full walk's
//! own contract now, and the last block pins the plan the whole move rests on.
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
// config::DEFAULT_TENANT. Named explicitly by both walks below only because the cutoff is
// the LAST parameter and positional binding cannot skip the one before it.
const DEFAULT_TENANT: &str = "00000000-0000-0000-0000-000000000001";

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
/// ('-infinity', nil) to start, then the last row's (write, id), and the cutoff
/// derived by the first page echoed back on every later one.
///
/// `pin_cutoff = false` is the CONTROL, not a mode the broker has: it re-derives
/// `now() - window` per page, which is what 1.0.1-beta.1 did and what B1 fixed.
/// `pause_ms` stalls the walk between its FIRST and second page, which is where
/// a bound that moves with the clock overtakes a cursor that does not.
async fn windowed_walk_paced(
    c: &tokio_postgres::Client,
    queue: &str,
    group: &str,
    window_ms: i64,
    page: i32,
    pin_cutoff: bool,
    pause_ms: u64,
) -> (Vec<String>, usize, Option<String>) {
    let mut after_write = "-infinity".to_string();
    let mut after_id = NIL.to_string();
    let mut cutoff: Option<String> = None;
    let mut out = Vec::new();
    let mut pages = 0usize;
    loop {
        let rows = c
            .query(
                // The tenant is named explicitly only because the cutoff sits AFTER it and
                // positional binding cannot skip a parameter. That order is deliberate:
                // see the parameter list in 004_log_pop.sql — the cutoff is appended last
                // so the previous release's 7-argument call keeps resolving through a
                // rolling upgrade.
                "SELECT r_id::text, r_write::text, r_cutoff::text \
                 FROM queen.log_hotlist_reseed_window_v1\
                 ($1,$2,$3::text::timestamptz,$4::text::uuid,$5,$6,\
                  $7::text::uuid,$8::text::timestamptz)",
                &[&queue, &group, &after_write, &after_id, &page, &window_ms,
                  &DEFAULT_TENANT, &cutoff],
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
        if pin_cutoff && cutoff.is_none() {
            cutoff = Some(last.get::<_, String>(2));
        }
        if rows.len() < page as usize {
            break;
        }
        assert!(pages < 200, "keyset made no progress: {pages} pages");
        if pause_ms > 0 && pages == 1 {
            tokio::time::sleep(std::time::Duration::from_millis(pause_ms)).await;
        }
    }
    (out, pages, cutoff)
}

/// The broker's own shape: pinned cutoff, no pauses.
async fn windowed_walk(
    c: &tokio_postgres::Client,
    queue: &str,
    group: &str,
    window_ms: i64,
    page: i32,
) -> (Vec<String>, usize) {
    let (out, pages, _) = windowed_walk_paced(c, queue, group, window_ms, page, true, 0).await;
    (out, pages)
}

/// The broker's FULL walk since 2026-08-23: the same statement with the cutoff pinned
/// to '-infinity' before the first page and `p_window_ms` = 0, which is unread once a
/// cutoff is bound — exactly `ReseedMode::Full.scan_bounds()` — paginated at `page`.
async fn unbounded_walk(
    c: &tokio_postgres::Client,
    queue: &str,
    group: &str,
    page: i32,
) -> (Vec<String>, usize) {
    let mut after_write = "-infinity".to_string();
    let mut after_id = NIL.to_string();
    let cutoff = Some("-infinity".to_string());
    let mut out = Vec::new();
    let mut pages = 0usize;
    loop {
        let rows = c
            .query(
                "SELECT r_id::text, r_write::text, r_cutoff::text \
                 FROM queen.log_hotlist_reseed_window_v1\
                 ($1,$2,$3::text::timestamptz,$4::text::uuid,$5,$6,\
                  $7::text::uuid,$8::text::timestamptz)",
                &[&queue, &group, &after_write, &after_id, &page, &0i64, &DEFAULT_TENANT, &cutoff],
            )
            .await
            .expect("unbounded walk");
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
        assert_eq!(
            last.get::<_, String>(2),
            "-infinity",
            "a pinned cutoff must come back unchanged on every page"
        );
        if rows.len() < page as usize {
            break;
        }
        assert!(pages < 200, "keyset made no progress: {pages} pages");
    }
    (out, pages)
}

/// One line per plan node, the way EXPLAIN prints it.
async fn explain(c: &tokio_postgres::Client, sql: &str) -> String {
    c.query(&format!("EXPLAIN (COSTS OFF) {sql}"), &[])
        .await
        .expect("explain")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n")
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

    // ------------------------------------------- the FULL walk (2026-08-23)
    // The broker's full walk is this statement with the cutoff pinned to '-infinity'
    // before the first page. Paged at 100 it crosses the same five pages of the tie
    // group the windowed walk just crossed, and it must be log_hotlist_reseed_v1's
    // exact set, once — that equivalence is what made the move safe.
    let (unbounded, unbounded_pages) = unbounded_walk(&c, &queue, group, 100).await;
    assert!(unbounded_pages > 4, "expected several pages at 100/page, got {unbounded_pages}");
    let unique_unbounded: std::collections::HashSet<_> = unbounded.iter().cloned().collect();
    assert_eq!(
        unique_unbounded.len(),
        unbounded.len(),
        "the pinned walk returned {} duplicate rows across {unbounded_pages} pages",
        unbounded.len() - unique_unbounded.len()
    );
    let mut unbounded_sorted = unbounded.clone();
    unbounded_sorted.sort();
    assert_eq!(
        unbounded_sorted, full,
        "the walk pinned to '-infinity' must return exactly the old full walk's set"
    );

    // ------------------------------------------------- a zero-width window
    // window_ms = 0 is "written at or after now", which no seeded row satisfies.
    // It must return nothing rather than error or fall back to everything: the
    // broker never passes 0, but a misconfigured knob must fail closed and be
    // healed by the full walk, not silently scan the whole queue.
    let (empty, _) = windowed_walk(&c, &queue, group, 0, 1_000_000).await;
    assert!(empty.is_empty(), "a zero window returned {} rows", empty.len());

    // ------------------------------------------- the cutoff is per WALK (B1)
    // Four partitions bunched against the OLD edge of a 5s window, which is where
    // the bug lived: the walk climbs from the OLDEST row, so a bound that
    // re-derives `now() - window` on every page creeps up through the rows the
    // cursor has not reached yet and that pass never returns them.
    //
    // Each walk gets a freshly seeded queue, so both start the same distance from
    // their own data and the only difference between them is the pinning.
    let pinq = unique("pinq");
    seed_bunched(&c, &pinq).await;
    // One row a page, stalled 1.5s between the first and the second: by then
    // `now() - 5s` has passed the age of every remaining row while the cursor is
    // still on the first.
    let (pinned, _, cut) = windowed_walk_paced(&c, &pinq, group, 5_000, 1, true, 1_500).await;
    assert_eq!(
        pinned.len(),
        4,
        "a walk pinned to one cutoff must cover the window it started on, got {:?}",
        pinned
    );
    assert!(cut.is_some(), "the first page must report the cutoff it derived");

    // The control: the same walk re-deriving the bound per page, which is what
    // 1.0.1-beta.1 did — it sees the first row and then nothing. If this ever
    // stops losing rows, the assertion above has stopped testing anything.
    let creepq = unique("creepq");
    seed_bunched(&c, &creepq).await;
    let (creeping, _, _) = windowed_walk_paced(&c, &creepq, group, 5_000, 1, false, 1_500).await;
    assert!(
        creeping.len() < pinned.len(),
        "the per-page bound was expected to skip rows the pinned one keeps, \
         got {} of {}",
        creeping.len(),
        pinned.len()
    );

    // An explicit cutoff is the bound in force and p_window_ms is then unread,
    // which is what every page after the first relies on. '-infinity' against a
    // zero window is the sharpest form of it: that window alone returns nothing.
    let unbounded = c
        .query(
            "SELECT r_id::text FROM queen.log_hotlist_reseed_window_v1\
             ($1,$2,$3::text::timestamptz,$4::text::uuid,$5,$6,\
              $7::text::uuid,$8::text::timestamptz)",
            &[&pinq, &group, &"-infinity", &NIL, &1_000i32, &0i64,
              &DEFAULT_TENANT, &Some("-infinity")],
        )
        .await
        .expect("cutoff walk")
        .len();
    assert_eq!(unbounded, 4, "an explicit cutoff must override p_window_ms");

    // ---------------------------------------------- the plan, GENERIC (2026-08-23)
    // Why the full walk moved onto this statement. The broker calls it through
    // prepare_cached, so after five executions Postgres may switch to a generic plan in
    // which every parameter is unknown — p_limit included, which the planner then
    // assumes keeps 10% of the rows. On the old full-walk statement that made an
    // ordered walk of log_partitions_pkey, with the queue applied as a join filter
    // afterwards, look 10x cheaper than the bitmap on the queue's own index, and it
    // won: every page read every partition in the cell (851k buffers against 20k on the
    // 827k-partition soak shape). This statement orders by its index's own leading
    // columns and resolves the queue by scalar subquery, so no such plan exists for it.
    // Forty queues of 2,000 partitions is enough for the planner to take the bait on the
    // old statement (the control) and to prefer the index on this one (the guard).
    let planq = unique("planq");
    seed_bulk(&c, &planq, 40, 2_000, group).await;
    c.batch_execute(
        "ANALYZE queen.queues; ANALYZE queen.log_partitions; ANALYZE queen.log_consumers; \
         SET plan_cache_mode = force_generic_plan; \
         PREPARE reseed_page(text,text,text,text,int,bigint,text,text) AS \
           SELECT r_id::text, r_name, r_write::text, r_cutoff::text \
           FROM queen.log_hotlist_reseed_window_v1($1,$2,$3::text::timestamptz,$4::text::uuid,$5,$6,\
                                                   $7::text::uuid,$8::text::timestamptz); \
         PREPARE reseed_v1(text,text,text,int,text) AS \
           SELECT r_id::text, r_name FROM queen.log_hotlist_reseed_v1($1,$2,$3::text::uuid,$4,$5::text::uuid)",
    )
    .await
    .expect("analyze + prepare under a generic plan");
    let q7 = format!("{planq}-q7");
    let plan = explain(
        &c,
        &format!("EXECUTE reseed_page('{q7}','{group}','-infinity','{NIL}',10000,0,'{DEFAULT_TENANT}','-infinity')"),
    )
    .await;
    assert!(
        plan.contains("idx_log_partitions_queue_write"),
        "the full walk's generic plan must be bounded by the queue's own index:\n{plan}"
    );
    assert!(
        !plan.contains("log_partitions_pkey"),
        "the full walk's generic plan walked the partitions table in PK order:\n{plan}"
    );
    // The control: the statement the broker used to run, on the same data and plan
    // mode. If this ever stops taking the PK walk, the planner has changed — re-verify
    // at scale before trusting the guard above to mean anything.
    let control = explain(
        &c,
        &format!("EXECUTE reseed_v1('{q7}','{group}','{NIL}',10000,'{DEFAULT_TENANT}')"),
    )
    .await;
    assert!(
        control.contains("Index Scan using log_partitions_pkey"),
        "control: log_hotlist_reseed_v1 no longer takes the PK walk under a generic plan:\n{control}"
    );
    c.batch_execute("DEALLOCATE ALL; RESET plan_cache_mode").await.ok();

    broker.shutdown().await;
}

/// `nq` queues `<prefix>-q1..` of `np` partitions each, every partition holding data
/// (last_offset = 10) with last_write_at spread over the last hour, and a consumer row
/// for `group` on two thirds of them (half consumed, half behind): the population the
/// planner sees, not the rows a walk returns.
async fn seed_bulk(c: &tokio_postgres::Client, prefix: &str, nq: i32, np: i32, group: &str) {
    c.execute(
        "INSERT INTO queen.queues(name) SELECT $1 || '-q' || g FROM generate_series(1,$2) g \
         ON CONFLICT DO NOTHING",
        &[&prefix, &nq],
    )
    .await
    .expect("queues");
    c.execute(
        "INSERT INTO queen.log_partitions(queue_id,name,last_offset,log_start,last_write_at) \
         SELECT q.id, 'p'||g, 10, 0, now() - (random() * interval '1 hour') \
         FROM queen.queues q, generate_series(1,$2) g WHERE q.name LIKE $1 || '-q%'",
        &[&prefix, &np],
    )
    .await
    .expect("partitions");
    c.execute(
        "INSERT INTO queen.log_consumers(partition_id,consumer_group,committed) \
         SELECT p.id, $2, CASE WHEN (substring(p.name from 2))::int % 3 = 0 THEN 10 ELSE 3 END \
         FROM queen.log_partitions p JOIN queen.queues q ON q.id = p.queue_id \
         WHERE q.name LIKE $1 || '-q%' AND (substring(p.name from 2))::int % 3 <> 1",
        &[&prefix, &group],
    )
    .await
    .expect("consumers");
}

/// A queue of four pending partitions written 4.0, 3.9, 3.8 and 3.7 seconds ago:
/// all inside a 5s window at seed time, all outside it 1.5 seconds later.
async fn seed_bunched(c: &tokio_postgres::Client, queue: &str) {
    c.execute(
        "INSERT INTO queen.queues(name) VALUES ($1) ON CONFLICT DO NOTHING",
        &[&queue],
    )
    .await
    .expect("queue");
    c.execute(
        "INSERT INTO queen.log_partitions(queue_id,name,last_offset,log_start,last_write_at)
         SELECT (SELECT id FROM queen.queues WHERE name=$1), 'p'||g, 10, 0,
                now() - ((4100 - g*100) || ' milliseconds')::interval
         FROM generate_series(1,4) g",
        &[&queue],
    )
    .await
    .expect("partitions");
}
