//! Regression suite for the 2026-08-24 retention incident (retention.rs module
//! docs): on the bench cell, the retention cycle's first eligibility SELECT
//! over a 91M-row queen.log_segments died on a statement timeout and the
//! SESSION-scoped belt lock 737001 went back to the pool still held — every
//! replica logged "claimed but advisory lock busy" for 28 minutes until
//! pg_terminate_backend. Two fixes, both covered here against a live Postgres:
//!
//!   1. LOCK SCOPE — the belt lock is now `pg_try_advisory_xact_lock` inside an
//!      explicit transaction on a dedicated holder connection. The tests below
//!      drive the exact deadpool + tokio_postgres shapes retention.rs uses and
//!      assert the lock is free for the next claimant after (a) the holder's
//!      statement dies on a statement timeout and (b) the cycle future is
//!      DROPPED mid-statement (broker-side cancellation) — the two exit paths
//!      the old explicit-unlock protocol missed. Plus the mixed-fleet pin:
//!      session and xact advisory locks exclude each other, so the belt still
//!      works against old-image pods holding the session flavor.
//!
//!   2. BOUNDED ELIGIBILITY — the boundary walks in log_retention_step_v1 /
//!      log_txns_purge_step_v1 are clamped to the batch horizon (006's windowed
//!      helper): drains stay exact (same rows deleted, same watermarks, rule-2
//!      cap intact) while first contact with an all-stale backlog costs
//!      Θ(batch) per call instead of Θ(backlog) per call.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`:
//!
//! ```bash
//! docker run --rm -d --name queen-retlock-pg -e POSTGRES_PASSWORD=postgres -p 5466:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5466 cargo test --test retention_lock_scope -- --ignored --nocapture
//! ```
//!
//! Its own test binary (process-global admission arbiter, like the other
//! embedded suites). The booted broker runs the REAL maintenance loops against
//! this database; the test freezes them by holding the session flavor of locks
//! 737001/737002/737003 for its whole length — which is itself the mixed-fleet
//! exclusion working.

use queen::{Broker, BrokerConfig};

/// Scratch advisory-lock ids for the scope tests — NOT 737001, so nothing here
/// races the booted broker's own cycles (the wiring of 737001 to the xact take
/// is pinned by retention::tests::belt_lock_is_transaction_scoped).
const LOCK_A: i64 = 737_901;
const LOCK_B: i64 = 737_902;

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

/// Session-flavor try-take, the old-image pod's shape.
async fn try_session_lock(c: &tokio_postgres::Client, id: i64) -> bool {
    c.query_one("SELECT pg_try_advisory_lock($1)", &[&id])
        .await
        .expect("session try")
        .get(0)
}

async fn session_unlock(c: &tokio_postgres::Client, id: i64) {
    let released: bool = c
        .query_one("SELECT pg_advisory_unlock($1)", &[&id])
        .await
        .expect("session unlock")
        .get(0);
    assert!(released, "session unlock of {id} reported not-held");
}

/// Poll until the lock is takeable session-style (then release it), or panic
/// after `wait_ms`. Used where the release is asynchronous by design: a dropped
/// holder future's ROLLBACK queues behind its own timed-out statement.
async fn assert_lock_frees(c: &tokio_postgres::Client, id: i64, wait_ms: u64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(wait_ms);
    loop {
        if try_session_lock(c, id).await {
            session_unlock(c, id).await;
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "advisory lock {id} still held after {wait_ms}ms — the holder leaked it"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// The retention drain loop, exactly as phase_worker runs it: one step call per
/// iteration until done or nothing deleted. Returns (total deleted, done flags
/// per call). `max_calls` is the no-spin guard — a windowed step that lied
/// about done would loop here forever.
async fn drain(
    c: &tokio_postgres::Client,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    max_calls: usize,
) -> (i64, Vec<bool>) {
    let mut deleted_total = 0i64;
    let mut dones = Vec::new();
    loop {
        let txt: String = c.query_one(sql, params).await.expect("step call").get(0);
        let v: serde_json::Value = serde_json::from_str(&txt).expect("step json");
        let deleted = v.get("deleted").and_then(|d| d.as_i64()).unwrap_or(0);
        let done = v.get("done").and_then(|d| d.as_bool()).unwrap_or(true);
        deleted_total += deleted;
        dones.push(done);
        if done || deleted == 0 {
            return (deleted_total, dones);
        }
        assert!(dones.len() < max_calls, "step loop made no bounded progress: {dones:?}");
    }
}

const RETENTION_STEP: &str = "SELECT (queen.log_retention_step_v1($1::text::uuid, \
     $2::text::timestamptz, $3::text::timestamptz, $4::int))::text";
const TXNS_STEP: &str =
    "SELECT (queen.log_txns_purge_step_v1($1::text::uuid, $2::text::timestamptz, $3::int))::text";
const EVICT_STEP: &str = "SELECT (queen.log_evict_max_wait_step_v1($1::text::uuid, \
     $2::text::timestamptz, $3::int))::text";

/// Seed one partition under `queue_id` with `n` segments of 10 frames each
/// (bases 0,10,..) — `old_n` of them created 2h ago, the rest just now.
/// Returns the partition id as text.
///
/// The trailing UPDATE sets the retention work-list watermarks
/// (`oldest_live_at` / `oldest_txn_at`, 001_log_schema) to what the push
/// allocator would have written, because these rows go in behind push's back.
/// Without it the 2026-08-24 work list — which finds partitions BY those
/// columns — would never nominate a hand-seeded partition, and the live-sweep
/// section below would wait out its deadline for a sweep that is correctly
/// declining to happen. `oldest_txn_at` stays NULL unless the caller adds
/// log_txns rows of its own (the sidecar seed in 2e sets it there).
async fn seed_partition(
    c: &tokio_postgres::Client,
    queue_id: &str,
    name: &str,
    n: i64,
    old_n: i64,
) -> String {
    let pid: String = c
        .query_one(
            "INSERT INTO queen.log_partitions(queue_id,name,last_offset,log_start)
             VALUES ($1::text::uuid, $2, $3, 0) RETURNING id::text",
            &[&queue_id, &name, &(n * 10 - 1)],
        )
        .await
        .expect("partition")
        .get(0);
    c.execute(
        "INSERT INTO queen.log_segments(partition_id, base_offset, end_offset, created_at, blob)
         SELECT $1::text::uuid, g*10, g*10+9,
                CASE WHEN g < $3::int8 THEN now() - interval '2 hours' ELSE now() END,
                ''::bytea
         FROM generate_series(0, $2::int8 - 1) g",
        &[&pid, &n, &old_n],
    )
    .await
    .expect("segments");
    c.execute(
        "UPDATE queen.log_partitions p
            SET oldest_live_at = (SELECT s.created_at FROM queen.log_segments s
                                  WHERE s.partition_id = p.id AND s.base_offset >= p.log_start
                                  ORDER BY s.base_offset LIMIT 1)
          WHERE p.id = $1::text::uuid",
        &[&pid],
    )
    .await
    .expect("watermark");
    pid
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

async fn log_start(c: &tokio_postgres::Client, pid: &str) -> i64 {
    c.query_one(
        "SELECT log_start FROM queen.log_partitions WHERE id = $1::text::uuid",
        &[&pid],
    )
    .await
    .expect("log_start")
    .get(0)
}

/// Sum every "Buffers: shared hit=… read=…" line of a TEXT-format
/// EXPLAIN ANALYZE output.
fn buffers_of(plan: &str) -> i64 {
    let mut total = 0i64;
    for line in plan.lines() {
        for key in ["hit=", "read="] {
            if let Some(pos) = line.find(key) {
                let digits: String =
                    line[pos + key.len()..].chars().take_while(|c| c.is_ascii_digit()).collect();
                total += digits.parse::<i64>().unwrap_or(0);
            }
        }
    }
    total
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn timed_out_or_cancelled_cycle_frees_the_belt_lock() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // Boot the real broker: applies the real schema (the SQL under test is
    // include_str!-embedded, so this also proves the edited file compiled in)
    // and runs the REAL retention loop with the xact-scoped lock against this
    // database for the length of the test.
    let _broker = Broker::start(
        BrokerConfig::new()
            .pg(host.clone(), port, "postgres", "postgres", "postgres")
            .pool_size(4),
    )
    .await
    .expect("broker start");

    let c = raw_connect(&host, port).await;
    // Freeze the broker's maintenance cycles for the whole test by holding the
    // SESSION flavor of their belt locks — the mixed-fleet exclusion doing its
    // job (an old-image pod's session lock must gate the new xact takes).
    for id in [737_001i64, 737_002, 737_003] {
        c.execute("SELECT pg_advisory_lock($1)", &[&id]).await.expect("freeze lock");
    }

    // A deadpool pool with the broker's own shapes (deadpool_postgres is the
    // same dependency the broker builds its pool from).
    let mut dp = deadpool_postgres::Config::new();
    dp.host = Some(host.clone());
    dp.port = Some(port);
    dp.user = Some("postgres".into());
    dp.password = Some("postgres".into());
    dp.dbname = Some("postgres".into());
    dp.pool = Some(deadpool_postgres::PoolConfig::new(4));
    let pool = dp
        .create_pool(Some(deadpool_postgres::Runtime::Tokio1), tokio_postgres::NoTls)
        .expect("pool");

    let c2 = raw_connect(&host, port).await;

    // ---------------------------------------------------------------- 1a
    // ERROR PATH — the incident's shape: the holder's eligibility statement
    // dies on statement_timeout. run_cycle's error arm rolls the holder
    // transaction back; the lock must be free for the next claimant and the
    // connection must go back to the pool healthy.
    {
        let mut lc = pool.get().await.expect("get");
        let tx = lc.transaction().await.expect("tx");
        tx.batch_execute("SET LOCAL statement_timeout = 100").await.expect("set local");
        let got: bool = tx
            .query_one("SELECT pg_try_advisory_xact_lock($1)", &[&LOCK_A])
            .await
            .expect("xact take")
            .get(0);
        assert!(got, "scratch lock unexpectedly busy");
        assert!(!try_session_lock(&c2, LOCK_A).await, "xact lock must gate the session take");
        let err = tx.batch_execute("SELECT pg_sleep(2)").await;
        assert!(err.is_err(), "the 100ms statement_timeout must kill pg_sleep(2)");
        let _ = tx.rollback().await;
        drop(lc);
        assert!(
            try_session_lock(&c2, LOCK_A).await,
            "lock must be free immediately after the error-path rollback"
        );
        session_unlock(&c2, LOCK_A).await;
        // The pooled connection survived (statement_timeout is not fatal) and
        // carries no SET LOCAL residue.
        let pc = pool.get().await.expect("reuse");
        let t: String = pc.query_one("SHOW statement_timeout", &[]).await.expect("show").get(0);
        assert_ne!(t, "100ms", "SET LOCAL leaked out of the holder transaction");
    }

    // ---------------------------------------------------------------- 1b
    // CANCELLATION PATH — the cycle future is dropped mid-statement (the
    // broker-side timeout/cancel shape). Nothing runs the explicit
    // rollback: the transaction guard's Drop must enqueue it, the abandoned
    // statement dies at ITS OWN statement_timeout bound, and the lock frees.
    // Under the old session-scoped protocol this exact sequence held the lock
    // until pg_terminate_backend.
    {
        let mut lc = pool.get().await.expect("get");
        let tx = lc.transaction().await.expect("tx");
        // The bound the holder sets for itself (Knobs::lock_stmt_timeout_ms
        // in miniature): it is what caps how long the queued ROLLBACK can wait.
        tx.batch_execute("SET LOCAL statement_timeout = 1000").await.expect("set local");
        let got: bool = tx
            .query_one("SELECT pg_try_advisory_xact_lock($1)", &[&LOCK_B])
            .await
            .expect("xact take")
            .get(0);
        assert!(got);
        let abandoned =
            tokio::time::timeout(std::time::Duration::from_millis(50), tx.batch_execute("SELECT pg_sleep(10)"))
                .await;
        assert!(abandoned.is_err(), "the broker-side timeout must elapse first");
        // The cycle future is dropped here: guard Drop enqueues ROLLBACK, the
        // connection object goes back to the pool.
        drop(tx);
        drop(lc);
        // Free within the statement bound (~1s) + slack, NOT 28 minutes.
        assert_lock_frees(&c2, LOCK_B, 8_000).await;
    }

    // ---------------------------------------------------------------- 1c
    // MIXED-FLEET INTEROP — both directions, both flavors, one lock space.
    {
        assert!(try_session_lock(&c2, LOCK_A).await);
        let mut lc = pool.get().await.expect("get");
        let tx = lc.transaction().await.expect("tx");
        let got: bool = tx
            .query_one("SELECT pg_try_advisory_xact_lock($1)", &[&LOCK_A])
            .await
            .expect("xact try")
            .get(0);
        assert!(!got, "a session holder must gate the xact take (old-image pod exclusion)");
        session_unlock(&c2, LOCK_A).await;
        let got: bool = tx
            .query_one("SELECT pg_try_advisory_xact_lock($1)", &[&LOCK_A])
            .await
            .expect("xact retry")
            .get(0);
        assert!(got, "released session lock must be takeable xact-style");
        assert!(!try_session_lock(&c2, LOCK_A).await, "xact holder must gate the session take");
        tx.rollback().await.expect("rollback");
        assert!(try_session_lock(&c2, LOCK_A).await, "rollback must free the xact lock");
        session_unlock(&c2, LOCK_A).await;
    }

    // ================================================================ 2
    // WINDOWED ELIGIBILITY — semantics of the bounded walks, driven through
    // the real step SPs the phase workers call.
    let queue = unique("retq");
    let qid: String = c
        .query_one("INSERT INTO queen.queues(name) VALUES ($1) RETURNING id::text", &[&queue])
        .await
        .expect("queue")
        .get(0);
    let cutoff_now: String =
        c.query_one("SELECT now()::text", &[]).await.expect("now").get(0);
    let no_cutoff: Option<String> = None;

    // 2a — all-stale backlog drains fully, in exactly ceil(N/batch) bounded
    // calls, watermark one past the last frame. This is the 91M shape in
    // miniature: the old walk re-scanned the whole remainder on every call.
    {
        let pid = seed_partition(&c, &qid, "p-stale", 2_500, 2_500).await;
        let (deleted, dones) =
            drain(&c, RETENTION_STEP, &[&pid, &cutoff_now, &no_cutoff, &400i32], 20).await;
        assert_eq!(deleted, 2_500, "all-stale backlog must drain fully");
        assert_eq!(dones.len(), 7, "2500 rows at batch 400 = 7 bounded calls");
        assert!(dones[..6].iter().all(|d| !d), "clipped calls must answer done=false");
        assert!(dones[6], "the final call must answer done=true");
        assert_eq!(segment_count(&c, &pid).await, 0);
        assert_eq!(log_start(&c, &pid).await, 25_000, "watermark = last deleted frame + 1");
    }

    // 2b — mixed backlog: the walk must stop exactly at the first fresh
    // segment, and a head-fresh partition must answer done=true at once.
    {
        let pid = seed_partition(&c, &qid, "p-mixed", 500, 300).await;
        let (deleted, dones) =
            drain(&c, RETENTION_STEP, &[&pid, &cutoff_now, &no_cutoff, &50i32], 20).await;
        assert_eq!(deleted, 300, "only the stale prefix may go");
        assert_eq!(dones.len(), 7, "6 deleting calls + 1 head-fresh done call");
        assert_eq!(segment_count(&c, &pid).await, 200, "fresh tail must survive");
        assert_eq!(log_start(&c, &pid).await, 3_000, "watermark = first fresh base");
    }

    // 2c — rule 2's consumed-only cap must survive the windowing: boundary
    // stops at the slowest cursor's next wanted offset even when the window
    // says more is stale, and the cap answers done=true (not clipped).
    {
        let pid = seed_partition(&c, &qid, "p-rule2", 100, 100).await;
        c.execute(
            "INSERT INTO queen.log_consumers(partition_id, consumer_group, committed)
             VALUES ($1::text::uuid, 'g', 499)",
            &[&pid],
        )
        .await
        .expect("consumer");
        let (deleted, dones) =
            drain(&c, RETENTION_STEP, &[&pid, &no_cutoff, &cutoff_now, &30i32], 20).await;
        assert_eq!(deleted, 50, "only fully consumed segments below MIN(committed)+1");
        assert_eq!(dones, vec![false, true], "clip, then exact cap");
        assert_eq!(segment_count(&c, &pid).await, 50, "unconsumed tail preserved");
        assert_eq!(log_start(&c, &pid).await, 500, "watermark = the rule-2 cap");
        let types: i64 = c
            .query_one(
                "SELECT count(*) FROM queen.retention_history
                 WHERE partition_id = $1::text::uuid AND retention_type = 'completed_retention'",
                &[&pid],
            )
            .await
            .expect("history")
            .get(0);
        assert_eq!(types, 2, "both deleting calls attribute to completed_retention");
    }

    // 2d — the eviction wrapper still delegates through the same windowed path.
    {
        let pid = seed_partition(&c, &qid, "p-evict", 40, 40).await;
        let (deleted, dones) = drain(&c, EVICT_STEP, &[&pid, &cutoff_now, &25i32], 20).await;
        assert_eq!(deleted, 40);
        assert_eq!(dones, vec![false, true]);
        assert_eq!(log_start(&c, &pid).await, 400);
        let types: i64 = c
            .query_one(
                "SELECT count(*) FROM queen.retention_history
                 WHERE partition_id = $1::text::uuid AND retention_type = 'max_wait_time_eviction'",
                &[&pid],
            )
            .await
            .expect("history")
            .get(0);
        assert_eq!(types, 2);
    }

    // 2e — the log_txns sidecar purge: same windowing, same convergence, fresh
    // rows survive.
    {
        let pid = seed_partition(&c, &qid, "p-txns", 1, 0).await;
        c.execute(
            "INSERT INTO queen.log_txns(partition_id, base_offset, end_offset, created_at, hashes)
             SELECT $1::text::uuid, g*10, g*10+9,
                    CASE WHEN g < 10 THEN now() - interval '2 hours' ELSE now() END,
                    ''::bytea
             FROM generate_series(0, 14) g",
            &[&pid],
        )
        .await
        .expect("txns");
        let (deleted, dones) = drain(&c, TXNS_STEP, &[&pid, &cutoff_now, &4i32], 20).await;
        assert_eq!(deleted, 10, "stale sidecar prefix only");
        assert_eq!(dones, vec![false, false, true]);
        let left: i64 = c
            .query_one(
                "SELECT count(*) FROM queen.log_txns WHERE partition_id = $1::text::uuid",
                &[&pid],
            )
            .await
            .expect("left")
            .get(0);
        assert_eq!(left, 5, "fresh sidecar rows must survive");
        let txns_start: i64 = c
            .query_one(
                "SELECT txns_start FROM queen.log_partitions WHERE id = $1::text::uuid",
                &[&pid],
            )
            .await
            .expect("txns_start")
            .get(0);
        assert_eq!(txns_start, 100, "watermark = first fresh sidecar base");
    }

    // 2f — BOUNDEDNESS: the exact predicate the windowed walk runs must be an
    // index range whose cost is the window, not the backlog. Differential
    // buffer proof on a 20k all-stale partition (the shape whose first
    // eligibility SELECT blew the statement timeout at 91M): the unbounded
    // walk touches the whole partition, the windowed one a single batch.
    {
        let pid = seed_partition(&c, &qid, "p-buffers", 20_000, 20_000).await;
        c.execute("ANALYZE queen.log_segments", &[]).await.ok();
        let unbounded = c
            .query(
                &format!(
                    "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                     SELECT s.base_offset FROM queen.log_segments s
                     WHERE s.partition_id = '{pid}'::uuid
                       AND s.base_offset >= 0
                       AND s.created_at >= now()
                     ORDER BY s.base_offset LIMIT 1"
                ),
                &[],
            )
            .await
            .expect("unbounded explain")
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n");
        let windowed = c
            .query(
                &format!(
                    "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                     SELECT s.base_offset FROM queen.log_segments s
                     WHERE s.partition_id = '{pid}'::uuid
                       AND s.base_offset >= 0
                       AND s.base_offset < COALESCE(1000, 9223372036854775807)
                       AND s.created_at >= now()
                     ORDER BY s.base_offset LIMIT 1"
                ),
                &[],
            )
            .await
            .expect("windowed explain")
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            windowed.contains("base_offset < "),
            "the horizon must land in the Index Cond, not a filter:\n{windowed}"
        );
        let (u, w) = (buffers_of(&unbounded), buffers_of(&windowed));
        assert!(
            w * 5 < u,
            "windowed walk must touch a small fraction of the unbounded walk's buffers \
             (windowed={w}, unbounded={u})\n--- windowed:\n{windowed}\n--- unbounded:\n{unbounded}"
        );
    }

    for id in [737_001i64, 737_002, 737_003] {
        session_unlock(&c, id).await;
    }

    // ================================================================ 3
    // LIVE SWEEP — with the freeze lifted, the booted broker's own retention
    // loop (this branch's cycle: xact lock inside the holder transaction, work
    // list on it, fan-out, commit) must claim a period and drain a
    // retention-enabled queue. The happy path of the new lock protocol end to
    // end, not a simulation — and the belt lock must be free between cycles.
    {
        let queue = unique("liveq");
        let qid: String = c
            .query_one(
                "INSERT INTO queen.queues(name, retention_enabled, retention_seconds)
                 VALUES ($1, true, 3600) RETURNING id::text",
                &[&queue],
            )
            .await
            .expect("live queue")
            .get(0);
        let pid = seed_partition(&c, &qid, "p-live", 120, 120).await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(40);
        while segment_count(&c, &pid).await > 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the broker's own retention loop never swept the live queue"
            );
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        assert_eq!(log_start(&c, &pid).await, 1_200, "watermark after the broker's own sweep");
        assert_lock_frees(&c2, 737_001, 10_000).await;
    }
}
