//! Regression suite for the 2026-08-24 boot-deadlock incident (schema.rs module
//! doc): rolling a broker whose 001_log_schema.sql had gained two ALTER TABLE
//! ADD COLUMN IF NOT EXISTS on queen.log_partitions (plus two partial indexes)
//! crash-looped FATAL on the bench cell — 10 of 10 boot attempts — because the
//! applier ran each file as ONE multi-statement transaction: the no-op
//! CREATE INDEX IF NOT EXISTS took and KEPT ShareLock on log_partitions while
//! the ALTER waited for AccessExclusive, and every log_push_multi transaction
//! mid RowShare→RowExclusive upgrade closed the cycle (40P01, deterministically,
//! at 1060 push tx/s).
//!
//! This file replays the ROLL, end to end, against the real embedded corpus:
//!
//!   1. boot A applies the schema to a virgin database and shuts down;
//!   2. one loader per partition then runs the pusher shape in a tight loop —
//!      `BEGIN; SELECT ... FOR UPDATE; <think>; UPDATE ...; COMMIT` on
//!      queen.log_partitions, i.e. a RowShare hold upgrading to RowExclusive,
//!      so some transaction is in the upgrade window at every instant;
//!   3. boot B re-applies the whole schema THROUGH that traffic and must come
//!      up (per-statement transactions: the applier never waits while holding,
//!      so it can appear in no cycle) — and the loaders must see ZERO errors,
//!      because a 40P01 on the loader side would be the same incident with the
//!      victim inverted;
//!   4. the retention watermark columns and their CONCURRENTLY-built partial
//!      indexes must exist and be VALID afterwards (an invalid index would be
//!      an interrupted CREATE INDEX CONCURRENTLY hiding behind IF NOT EXISTS).
//!
//! The deterministic single-collision version of the geometry (old shape MUST
//! deadlock, new shape MUST NOT) lives in schema.rs's own `mod tests`; this
//! suite is the probabilistic, full-corpus, real-boot-path complement — under
//! the pre-fix applier it fails inside seconds.
//!
//! `Broker::start` is used on purpose: it is the only way to run the
//! `include_str!`-embedded copy of the SQL — the one the binary actually
//! carries (see kv_timers_boot_idempotence.rs for the precedent and for why
//! multiple starts in one binary are safe).
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`:
//!
//! ```bash
//! docker run --rm -d --name queen-schema-pg -e POSTGRES_PASSWORD=postgres -p 5473:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5473 cargo test --test schema_apply_boot_under_load -- --ignored --nocapture
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use queen::{Broker, BrokerConfig};

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

fn cfg(host: &str, port: u16, db: &str) -> BrokerConfig {
    // Background loops off so the ONLY traffic during boot B is the loaders':
    // the test must prove the applier rides over the pusher shape, not that a
    // stats cycle happened to win some race.
    BrokerConfig::new()
        .pg(host.to_string(), port, "postgres", "postgres", db.to_string())
        .pool_size(4)
        .retention(false)
        .stats_refresh(false)
        .system_metrics(false)
        .log_reports(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn rolling_boot_reapplies_schema_under_live_push_shaped_writes() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    let admin = connect(&host, port, "postgres").await;

    // A panicking run leaves its database behind; sweep the previous ones so a
    // long-lived container does not accumulate them.
    for row in admin
        .query(
            "SELECT datname FROM pg_database WHERE datname LIKE 'qsch\\_load\\_%'",
            &[],
        )
        .await
        .expect("list leftovers")
    {
        let old: String = row.get(0);
        let _ = admin
            .execute(&format!("DROP DATABASE IF EXISTS \"{old}\" WITH (FORCE)"), &[])
            .await;
    }

    let db = format!(
        "qsch_load_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    admin
        .execute(&format!("CREATE DATABASE \"{db}\""), &[])
        .await
        .expect("create virgin database");

    // ---- boot A: virgin apply -------------------------------------------
    let a = Broker::start(cfg(&host, port, &db))
        .await
        .expect("boot A (virgin apply) failed");
    a.shutdown().await;

    // ---- seed: one queue, eight partitions ------------------------------
    let seeder = connect(&host, port, &db).await;
    let qid: String = seeder
        .query_one(
            "INSERT INTO queen.queues (name) VALUES ('load') RETURNING id::text",
            &[],
        )
        .await
        .expect("seed queue")
        .get(0);
    let pids: Vec<String> = seeder
        .query(
            "INSERT INTO queen.log_partitions (queue_id, name)
             SELECT $1::text::uuid, 'p' || g FROM generate_series(1, 8) g
             RETURNING id::text",
            &[&qid],
        )
        .await
        .expect("seed partitions")
        .iter()
        .map(|r| r.get(0))
        .collect();

    // ---- loaders: the pusher shape, continuously ------------------------
    let stop = Arc::new(AtomicBool::new(false));
    let committed = Arc::new(AtomicU64::new(0));
    let mut loaders = Vec::new();
    for pid in &pids {
        let pid = pid.clone();
        let stop = stop.clone();
        let committed = committed.clone();
        let lc = connect(&host, port, &db).await;
        loaders.push(tokio::spawn(async move {
            let mut errors: Vec<String> = Vec::new();
            let select =
                format!("SELECT last_offset FROM queen.log_partitions WHERE id = '{pid}'::uuid FOR UPDATE");
            let update = format!(
                "UPDATE queen.log_partitions SET last_offset = last_offset + 1 WHERE id = '{pid}'::uuid"
            );
            while !stop.load(Ordering::Relaxed) {
                let attempt: Result<(), tokio_postgres::Error> = async {
                    lc.batch_execute("BEGIN").await?;
                    lc.query_one(select.as_str(), &[]).await?;
                    // Think time INSIDE the transaction: this is what keeps a
                    // RowShare holder in the upgrade window at every instant.
                    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                    lc.execute(update.as_str(), &[]).await?;
                    lc.batch_execute("COMMIT").await?;
                    Ok(())
                }
                .await;
                match attempt {
                    Ok(()) => {
                        committed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        errors.push(format!("{e:?}"));
                        let _ = lc.batch_execute("ROLLBACK").await;
                    }
                }
            }
            errors
        }));
    }

    // Steady state before the roll.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let before_boot = committed.load(Ordering::Relaxed);
    assert!(before_boot > 0, "loaders never committed — fixture broken");

    // ---- boot B: the rolling restart, THROUGH the traffic ---------------
    // Pre-fix applier: 001 deadlocks against the upgrade cycle, five in-process
    // retries all deadlock again, Broker::start returns Err within seconds.
    // Worst case for the fixed applier is bounded lock_timeout retries, so 120s
    // of headroom distinguishes "riding over traffic" from "wedged behind it".
    let b = tokio::time::timeout(
        std::time::Duration::from_secs(120),
        Broker::start(cfg(&host, port, &db)),
    )
    .await
    .expect("boot B did not finish within 120s — the re-apply is wedged behind live traffic")
    .expect("boot B failed — the re-apply could not ride over live push-shaped writes");
    b.shutdown().await;

    // Loaders must keep committing THROUGH and past the apply, error-free.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let after_boot = committed.load(Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);
    let mut all_errors = Vec::new();
    for l in loaders {
        all_errors.extend(l.await.expect("loader join"));
    }
    assert!(
        all_errors.is_empty(),
        "loaders errored during the rolling re-apply (a 40P01 here is the incident with the \
         victim inverted): {all_errors:?}"
    );
    assert!(
        after_boot > before_boot,
        "loaders made no progress across boot B — the apply blocked traffic"
    );

    // ---- the DDL that started it all must be in place, and VALID ---------
    let cols: i64 = seeder
        .query_one(
            "SELECT count(*) FROM information_schema.columns
             WHERE table_schema = 'queen' AND table_name = 'log_partitions'
               AND column_name IN ('oldest_live_at', 'oldest_txn_at')",
            &[],
        )
        .await
        .expect("columns probe")
        .get(0);
    assert_eq!(cols, 2, "the retention watermark columns must exist");

    let idx: i64 = seeder
        .query_one(
            "SELECT count(*) FROM pg_indexes
             WHERE schemaname = 'queen'
               AND indexname IN ('idx_log_partitions_queue_oldest',
                                 'idx_log_partitions_queue_oldest_txn')",
            &[],
        )
        .await
        .expect("indexes probe")
        .get(0);
    assert_eq!(idx, 2, "the CONCURRENTLY-built partial indexes must exist");

    let invalid: i64 = seeder
        .query_one(
            "SELECT count(*) FROM pg_index x
             JOIN pg_class ic ON ic.oid = x.indexrelid
             JOIN pg_class tc ON tc.oid = x.indrelid
             JOIN pg_namespace n ON n.oid = tc.relnamespace
             WHERE n.nspname = 'queen' AND NOT x.indisvalid",
            &[],
        )
        .await
        .expect("invalid-index probe")
        .get(0);
    assert_eq!(
        invalid, 0,
        "no queen.* index may be left INVALID after a clean boot (an invalid index is an \
         interrupted CREATE INDEX CONCURRENTLY hiding behind IF NOT EXISTS)"
    );

    drop(seeder);
    admin
        .execute(&format!("DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)"), &[])
        .await
        .expect("drop scratch database");
}
