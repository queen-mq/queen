//! Boot-time schema applier.
//!
//! The broker is standalone and owns its schema: at startup it applies
//! `sql/schema.sql` + every `sql/procedures/*.sql` (the PROCEDURES list below),
//! in lexical order. The deployment model is always-virgin — a deployment
//! starts from an empty database and this list IS the schema. There is no
//! upgrade path and no redefinition ordering: each function and table is
//! defined exactly once, by exactly one file, for the one (log) engine.
//!
//! DDL is serialized cluster-wide with a **session advisory lock**, so
//! replicas booting concurrently never interleave statements. Every statement
//! is idempotent (`CREATE OR REPLACE`, `IF NOT EXISTS`), so re-apply on every
//! boot is safe.
//!
//! ## One statement per transaction (2026-08-24 incident)
//!
//! Statements are executed ONE PER TRANSACTION, split out of each file by a
//! dollar-quote-aware splitter — never a whole file as one `batch_execute`.
//! The whole-file shape crash-looped a rolling broker on the bench cell
//! (1060 push tx/s, 827k partitions, 10 of 10 boot attempts FATAL) through a
//! lock-upgrade cycle that needs no second table:
//!
//!   * `ALTER TABLE`, `CREATE INDEX` and friends take their relation lock
//!     BEFORE evaluating `IF NOT EXISTS`, so even a no-op re-apply of
//!     `CREATE INDEX IF NOT EXISTS` takes (and, mid-file, KEEPS) `ShareLock`
//!     on the hottest table;
//!   * a `log_push_multi` transaction already holds `RowShareLock` on that
//!     table (the allocator's `FOR UPDATE`) and upgrades to `RowExclusiveLock`
//!     for its UPDATE — which conflicts with the applier's held `ShareLock`;
//!   * the applier's next statement waits for `AccessExclusiveLock` — which
//!     conflicts with the pusher's held `RowShareLock`. Cycle, `40P01`, every
//!     time, because under load some push transaction is always mid-upgrade.
//!
//! Single-statement transactions dissolve the cycle structurally: the applier
//! never WAITS while HOLDING, so it can appear in no cycle. The residual cost
//! is queueing, and that is bounded too:
//!
//!   * every non-CONCURRENTLY statement runs under `lock_timeout` =
//!     [`APPLY_LOCK_TIMEOUT_MS`], so a lock-taking DDL that cannot get its
//!     lock promptly fails with `55P03` instead of parking an
//!     `AccessExclusiveLock` request in the queue where it would block all
//!     traffic behind it indefinitely;
//!   * `55P03` and `40P01` are retried with jittered backoff up to
//!     [`APPLY_LOCK_ATTEMPTS`] times per statement, then the boot fails with
//!     the file, statement ordinal, statement preview, the full server error
//!     and the list of sessions that were holding conflicting locks
//!     ([`blocker_report`]) — fail-fast is preserved, only the transient
//!     lock-contention class is absorbed;
//!   * a run of consecutive `DROP ...` statements is grouped with the first
//!     statement that follows it into ONE transaction, so the corpus's
//!     `DROP TRIGGER` + `CREATE TRIGGER` pair (019) and any future
//!     drop-then-recreate stays atomic and a live HA peer can never observe
//!     the gap. Groups form only around DROPs, so they never hold the no-op
//!     `IF NOT EXISTS` locks that built the incident's cycle;
//!   * `CREATE INDEX CONCURRENTLY` is detected and run with `lock_timeout`
//!     disabled and no retry (its own waits are the point — it does not block
//!     writes). It is legal here precisely because statements run outside any
//!     multi-statement transaction block. CAVEAT: an interrupted CIC leaves
//!     an INVALID index that `IF NOT EXISTS` then skips silently on every
//!     later boot; the applier WARNs about invalid `queen.*` indexes after
//!     every successful apply so the leftover cannot hide.
//!
//! ## Waiting is the only tool, so it is sliced and patient (same incident)
//!
//! The per-statement + `lock_timeout` shape above turned the deadlock into
//! clean `55P03` retries — and the cell then lost 10 of 10 boots ANYWAY,
//! because a retry only helps if the blocker has GAPS and these had none:
//!
//!   * the retention cycle's holder transaction sat `idle in transaction`
//!     holding `AccessShare` on `queen.log_partitions` for a WHOLE cycle, and
//!     cycles ran for minutes while a backlog burned (retention.rs now keeps
//!     that transaction down to the advisory lock alone, which removes this
//!     broker's own contribution but not everyone else's);
//!   * the rig's collector queries held `AccessShare` for 40-115s a piece.
//!
//! Against a standing holder the only lever left is PATIENCE, and patience is
//! cheap in exactly the way cancellation is not. The physics: while an
//! `AccessExclusive` request waits, every NEW acquirer of that relation queues
//! BEHIND it, so one wait of length T is a full write stall of length T. That
//! is what makes the [`APPLY_LOCK_TIMEOUT_MS`] slicing load-bearing — traffic
//! stalls for at most one slice, never for the whole window — and it is also
//! what makes extra slices free: sleeping between them holds nothing at all.
//! So the window is ~3 minutes of 2s slices ([`APPLY_LOCK_ATTEMPTS`]), and the
//! backoff between them carries JITTER so two brokers rolled in the same second
//! de-phase instead of colliding on the same gaps forever.
//!
//! There is deliberately NO `pg_cancel_backend`/`pg_terminate_backend`
//! machinery: a boot that kills other people's sessions to make room for its
//! own DDL is a worse failure than a boot that does not come up. When the
//! window is exhausted the applier fails the boot and NAMES the blockers
//! ([`blocker_report`]) so an operator can decide.
//!
//! ## Authoring rules for these files
//!
//!   * Every STATEMENT must be idempotent on its own (it always was — the
//!     file-level transaction never provided ordering guarantees a re-apply
//!     could rely on). A boot that dies mid-file leaves the file partially
//!     applied; that is fine because the process exits (fail-fast) and the
//!     next boot re-applies everything from the top. Last-boot-wins is
//!     unchanged: every boot still rewrites every object.
//!   * A NEW index on an already-populated hot table must be written
//!     `CREATE INDEX CONCURRENTLY IF NOT EXISTS` (the 2026-08-24 retention
//!     watermark indexes in 001 are the precedent). Plain `CREATE INDEX` is
//!     for indexes born with their table — on an always-virgin deployment the
//!     table is empty at first boot, and the no-op re-apply take of
//!     `ShareLock` is instant under the per-statement regime.
//!   * A `DROP` that must be atomic with its re-`CREATE` must sit IMMEDIATELY
//!     before it (the grouping rule above is positional). A `DROP` followed
//!     by `CREATE INDEX CONCURRENTLY` is NOT grouped — CIC cannot run inside
//!     a transaction block.
//!
//! The SQL is embedded at compile time (`include_str!`) so the binary is
//! self-contained — no need to also ship `sql/` into the container image.

use deadpool_postgres::Pool;
use tokio_postgres::error::SqlState;

/// Stable key for the schema-apply session advisory lock (arbitrary but fixed).
const SCHEMA_LOCK_KEY: i64 = 778_120_010;

const SCHEMA_SQL: &str = include_str!("../sql/schema.sql");

/// (filename, contents) in the exact order they must be applied. schema.sql runs
/// first (handled separately); these are the `procedures/*.sql` in lexical order.
const PROCEDURES: &[(&str, &str)] = &[
    // Renumbered 2026-07-31 (queue-identity merge + always-virgin deployment
    // model): the migration-only files (seg_* teardown, rows-engine drops, the
    // identity-merge data migration) are GONE — a deployment starts from an
    // empty database and this list IS the schema. Order is load-bearing only
    // where SQL-language function bodies resolve tables at creation time:
    // tables (001, 002) come first, engine SPs next, management plane after,
    // and the partition-counter trigger attachment (020) after both the table
    // it attaches to (001) and the functions it binds (019).
    //
    // 024/025 (kv, timers) come LAST, and that position is itself load-bearing
    // twice over (PLAN_KV_TIMERS §3.1, §1.2):
    //   * 005_log_ack.sql applies BEFORE them and calls queen.kv_apply_v1 and
    //     queen.log_timers_apply_v1. That works only because the body of
    //     log_transaction_wire_v1 is plpgsql, which resolves names at RUNTIME.
    //     Rewriting it as LANGUAGE sql would kill the boot at creation time.
    //   * conversely, nothing that applies before 024/025 can reference their
    //     tables from a LANGUAGE sql body without failing the boot — which is
    //     the free mechanical guard that keeps log_partition_dead_v1 (006,
    //     LANGUAGE sql) from ever growing a veto leg on queen.kv or
    //     queen.log_timers, and with it the O(partitions) scan it fronts.
    ("001_log_schema.sql", include_str!("../sql/procedures/001_log_schema.sql")),
    ("002_streams_schema.sql", include_str!("../sql/procedures/002_streams_schema.sql")),
    ("003_log_push.sql", include_str!("../sql/procedures/003_log_push.sql")),
    ("004_log_pop.sql", include_str!("../sql/procedures/004_log_pop.sql")),
    ("005_log_ack.sql", include_str!("../sql/procedures/005_log_ack.sql")),
    ("006_log_maintenance.sql", include_str!("../sql/procedures/006_log_maintenance.sql")),
    ("007_log_streams.sql", include_str!("../sql/procedures/007_log_streams.sql")),
    ("008_streams_register_query_v1.sql", include_str!("../sql/procedures/008_streams_register_query_v1.sql")),
    ("009_streams_state_get_v1.sql", include_str!("../sql/procedures/009_streams_state_get_v1.sql")),
    ("010_log_admin.sql", include_str!("../sql/procedures/010_log_admin.sql")),
    ("011_log_stats.sql", include_str!("../sql/procedures/011_log_stats.sql")),
    ("012_configure.sql", include_str!("../sql/procedures/012_configure.sql")),
    ("013_analytics.sql", include_str!("../sql/procedures/013_analytics.sql")),
    ("014_consumer_groups.sql", include_str!("../sql/procedures/014_consumer_groups.sql")),
    ("015_status.sql", include_str!("../sql/procedures/015_status.sql")),
    ("016_messages.sql", include_str!("../sql/procedures/016_messages.sql")),
    ("017_traces.sql", include_str!("../sql/procedures/017_traces.sql")),
    ("018_stats.sql", include_str!("../sql/procedures/018_stats.sql")),
    ("019_worker_metrics.sql", include_str!("../sql/procedures/019_worker_metrics.sql")),
    ("020_log_partition_counters.sql", include_str!("../sql/procedures/020_log_partition_counters.sql")),
    ("021_postgres_stats.sql", include_str!("../sql/procedures/021_postgres_stats.sql")),
    ("022_retention_analytics.sql", include_str!("../sql/procedures/022_retention_analytics.sql")),
    ("023_prometheus.sql", include_str!("../sql/procedures/023_prometheus.sql")),
    ("024_kv.sql", include_str!("../sql/procedures/024_kv.sql")),
    ("025_log_timers.sql", include_str!("../sql/procedures/025_log_timers.sql")),
    // The sweeper's two slow phases (§7.5). Separate from 024 because these are
    // the only KV functions no request calls, and the only two the broker is
    // designed to run WITHOUT: sweeper.rs degrades each to OffConfig on SQLSTATE
    // class 42 and keeps serving, which is what made their earlier absence a
    // pair of startup WARNs instead of a boot failure.
    ("026_kv_sweeper.sql", include_str!("../sql/procedures/026_kv_sweeper.sql")),
    // The quota/measurement read (§9.3). A third actor again: not the request
    // path (024) and not the sweeper (026), but a read-only poll that EVERY
    // broker runs — including one with QUEEN_SWEEPER=false, which still has to
    // enforce quotas. Read-only, so it takes no lock-order argument and writes
    // nothing.
    ("027_kv_quota.sql", include_str!("../sql/procedures/027_kv_quota.sql")),
    // The retained-bytes slow lane (PLAN_STATS_REFRESH.md T1.0): the one
    // O(segments) heap scan left in the stats family, moved out of 011's
    // per-cadence refresh onto its own loop (stats.rs::spawn_retained_bytes).
    // Ships in the SAME image as the 011 that stopped writing the column: a
    // fleet where 011 self-assigns retained_bytes and this file is absent
    // leaves the proxy's storage-quota gauge with no writer at all.
    ("028_retained_bytes.sql", include_str!("../sql/procedures/028_retained_bytes.sql")),
    // The durable per-task scheduler (PLAN_STATS_REFRESH.md T2.1): cadence for
    // the cluster-singleton loops (stats, retained-bytes, retention) as one row
    // per task, DB-clock arbitrated, lease-bounded, fenced. The 737_00x
    // advisory locks stay in the loops as belt until every deployment
    // schedules through this table.
    ("029_maintenance_leases.sql", include_str!("../sql/procedures/029_maintenance_leases.sql")),
    // The ephemeral queue class's ENTIRE database footprint (EPHEMERAL_QUEUES.md
    // §2): two cold tables (declared configs, grant rows) and their SPs. Position
    // carries no ordering argument — nothing before it references these tables and
    // nothing here references anything before it — so it sits last simply because
    // it is newest. The load-bearing property is the opposite of 024/025's: this
    // file is touched at boot, at `configure` and at `delete`, and NEVER by push,
    // pop or ack, which construct no SQL at all.
    ("030_ephemeral.sql", include_str!("../sql/procedures/030_ephemeral.sql")),
    // The tenant purge (queen.delete_tenant_data_v1). LAST, and that position
    // is load-bearing in one direction only: its body is plpgsql, so it
    // resolves names at RUNTIME and could sit anywhere — but it names tables
    // from 001 (hotlist_repairs), 019 (queue_parked_replica), 024 (kv,
    // kv_quota, kv_usage), 025 (log_timers) and 030 (ephemeral_*) and calls
    // queen.delete_queue_v1 from 013, so last is the position at which a
    // reader can see that every one of them already exists. Rewriting it as
    // LANGUAGE sql would make that a hard requirement instead of a courtesy.
    ("031_tenant_purge.sql", include_str!("../sql/procedures/031_tenant_purge.sql")),
];

/// Minimum PostgreSQL this schema can be applied to, as `server_version_num`
/// (14.0 = 140000).
///
/// PLAN_KV_TIMERS §0 pins the floor at 14 and asks for it to be checked here.
/// `queen.kv` is the first thing in this schema to use `GENERATED ALWAYS AS ...
/// STORED` (needs 12) and `starts_with()` (needs 11); neither has a precedent in
/// the pre-024 files, so nothing before them would have noticed an older server.
/// Without this check the symptom is not a clear error but an apply that fails
/// mid-file, after the per-statement lock retries below, with a message about a
/// syntax error that names no version at all.
const MIN_SERVER_VERSION_NUM: i32 = 140_000;

/// `lock_timeout` (ms) for every statement except `CREATE INDEX CONCURRENTLY`.
/// 2s is the value the 2026-08-24 manual procedure used on the live cell, where
/// every single-statement ALTER succeeded on the first try — the killer was the
/// multi-statement transaction, not the lock itself. The timeout also bounds
/// how long a queued AccessExclusive request may dam the traffic behind it.
const APPLY_LOCK_TIMEOUT_MS: u32 = 2_000;

/// The slice IS the write stall (module docs: everything that arrives while an
/// `AccessExclusive` request is queued waits behind it), and the whole patience
/// design rests on that stall staying short enough to be invisible to a client.
/// Raising this is therefore not a tuning change but a traffic change — the
/// lever for "wait longer" is [`APPLY_LOCK_ATTEMPTS`], which holds nothing
/// between slices. Compile-time, like retention.rs's parallelism floor.
const _: () = assert!(APPLY_LOCK_TIMEOUT_MS <= 2_000);

/// Attempts per statement (or drop-group) on `40P01` / `55P03` before the boot
/// fails. 40 slices of [`APPLY_LOCK_TIMEOUT_MS`] plus their backoffs is a
/// patience window of ~3 minutes ([`patience_window_ms`] computes the exact
/// worst case, and the tests pin it to that band).
///
/// It was 10 (~30s) and that window lost 10 of 10 boots on the 2026-08-24 cell:
/// the blockers there were an idle-in-transaction cycle holder and 40-115s
/// collector queries, i.e. holders measured in MINUTES, so a 30s window could
/// only ever have won by luck. Widening it costs live traffic nothing (module
/// docs: the stall is one slice, never the window), so the ceiling is set by
/// how long an operator should wait for a boot to declare a stuck database, not
/// by what is safe.
const APPLY_LOCK_ATTEMPTS: u32 = 40;

/// Hard ceiling on ONE backoff sleep, jitter included: `2000 + 50%`. Clamped in
/// [`lock_backoff`] rather than merely asserted, so a later widening of the base
/// or of the jitter cannot quietly turn a gap between slices into a longer idle
/// than the slices themselves — the sleep holds no lock, but it also makes no
/// progress, and the window's whole value is the number of TRIES inside it.
const APPLY_BACKOFF_MAX_MS: u64 = 3_000;

/// Deterministic part of the backoff before retry `attempt + 1`: linear 250ms
/// steps, capped at 2s. Monotone by construction — [`lock_backoff`] adds the
/// jitter on top, so the schedule still climbs while individual draws vary.
fn lock_backoff_base(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_millis((u64::from(attempt) * 250).min(2_000))
}

/// [`lock_backoff_base`] plus 0..=50% of itself, drawn fresh per attempt.
///
/// The jitter exists for one shape: a rolling restart puts several brokers into
/// this loop within the same second, all slicing at the same 2s cadence against
/// the same table. Without it they re-collide on every slice for the whole
/// window (and each one's queued `AccessExclusive` request is a stall the
/// others then wait behind); with it they de-phase within a couple of attempts.
/// `rand::random` is the house idiom for exactly this (hotlist.rs's reseed
/// phase) — no reproducibility is wanted here, only that two processes disagree.
fn lock_backoff(attempt: u32) -> std::time::Duration {
    let base = lock_backoff_base(attempt).as_millis() as u64;
    let jitter = u64::from(rand::random::<u32>()) % (base / 2 + 1);
    std::time::Duration::from_millis((base + jitter).min(APPLY_BACKOFF_MAX_MS))
}

/// Worst-case patience window for `attempts` tries, in ms: every slice waits
/// its full [`APPLY_LOCK_TIMEOUT_MS`] and every backoff draws its maximum
/// jitter. Named (rather than spelled in prose) because the boot's FATAL error
/// quotes it to the operator and a test pins it — a comment that says
/// "~3 minutes" next to a constant someone raised is worse than no comment.
fn patience_window_ms(attempts: u32) -> u64 {
    let slices = u64::from(attempts) * u64::from(APPLY_LOCK_TIMEOUT_MS);
    let sleeps: u64 = (1..attempts)
        .map(|a| {
            let base = lock_backoff_base(a).as_millis() as u64;
            base + base / 2
        })
        .sum();
    slices + sleeps
}

/// Apply the schema at boot. Set `QUEEN_APPLY_SCHEMA=0` to skip (e.g. when the DB
/// is managed externally). Fails the process on any DDL error (fail-fast boot),
/// and — before any of it — on a PostgreSQL older than [`MIN_SERVER_VERSION_NUM`].
/// The version check rides with the apply on purpose: it guards the statements
/// this function is about to run, so `QUEEN_APPLY_SCHEMA=0` skips both together
/// and a boot that writes no DDL is not killed by a check for DDL it never runs.
pub async fn apply(pool: &Pool) -> Result<(), Box<dyn std::error::Error>> {
    if !crate::config::env_bool("QUEEN_APPLY_SCHEMA", true) {
        tracing::info!(target: "schema", "apply skipped (QUEEN_APPLY_SCHEMA=0)");
        return Ok(());
    }

    let started = std::time::Instant::now();

    // The apply connection is TAKEN out of the pool, not borrowed: the applier
    // mutates session state (lock_timeout, application_name, the session
    // advisory lock), and returning it to the pool would leak a 2s lock_timeout
    // into request traffic, turning ordinary row-lock waits into 55P03 errors.
    // Dropping the taken client closes the connection instead; the pool
    // re-creates the slot lazily.
    let client = deadpool_postgres::Client::take(pool.get().await.map_err(|e| {
        format!("getting the apply connection from the pool: {e}")
    })?);

    // Named so the AccessExclusive waits this connection produces during a
    // rolling boot are attributable in pg_stat_activity and the server log.
    client
        .batch_execute("SET application_name = 'queen-schema-apply'")
        .await
        .map_err(|e| format!("SET application_name: {}", pg_error_text(&e)))?;

    // Refuse an unsupported server BEFORE taking the advisory lock and before
    // the first DDL statement: a version too old is a permanent, operator-level
    // fault, and the retry machinery below exists for transient lock contention
    // against live traffic. Retrying this would only delay the exit and bury
    // the cause.
    let server_version = check_server_version(&client).await?;

    // Serialize DDL across replicas. Session-level lock held on THIS connection;
    // released below (and, belt-and-braces, when the connection closes).
    //
    // lock_timeout is EXPLICITLY cleared first: lock_timeout applies to
    // advisory locks too, and a pooled connection can arrive carrying session
    // state from an earlier apply attempt. A replica arriving second on a
    // VIRGIN database waits here for the whole first apply (schema + every
    // procedure + CIC — seconds), which is exactly the wait this lock exists
    // for; timing it out turned the two-broker fresh-boot into a FATAL race
    // (caught by the mesh/ha suite, 2026-08-25).
    client
        .execute("SET lock_timeout = 0", &[])
        .await
        .map_err(|e| format!("clearing lock_timeout before the schema lock: {}", pg_error_text(&e)))?;
    // POLL, never block: a blocking `pg_advisory_lock` wait is itself a live
    // (implicit) transaction holding a snapshot, and the peer's
    // CREATE INDEX CONCURRENTLY must wait for every older snapshot to end —
    // so two brokers cold-booting a virgin database deadlocked, provably:
    //   Process 76 waits for advisory lock; blocked by 75.
    //   Process 75 (CIC) waits for virtual transaction of 76.
    // (mesh/ha suite, 2026-08-25; PG killed the second applier every time.)
    // Each try below is a sub-millisecond statement, so between polls this
    // session holds NO snapshot and the peer's CIC sails past it.
    let waited = std::time::Instant::now();
    loop {
        let got: bool = client
            .query_one("SELECT pg_try_advisory_lock($1)", &[&SCHEMA_LOCK_KEY])
            .await
            .map_err(|e| format!("trying the schema apply advisory lock: {}", pg_error_text(&e)))?
            .get(0);
        if got {
            break;
        }
        if waited.elapsed() > std::time::Duration::from_secs(600) {
            return Err("another replica has held the schema apply lock for over \
                        10 minutes; its apply is stuck or its session leaked the \
                        lock — inspect pg_locks for objid 778120010"
                .into());
        }
        if waited.elapsed().as_secs() % 10 == 0 {
            tracing::info!(
                target: "schema",
                waited_s = waited.elapsed().as_secs(),
                "waiting for another replica's schema apply to finish"
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    let result = apply_all(&client).await;

    // Always release the lock, even if apply failed.
    let _ = client
        .execute("SELECT pg_advisory_unlock($1)", &[&SCHEMA_LOCK_KEY])
        .await;

    result?;

    // An interrupted CREATE INDEX CONCURRENTLY (a killed boot, a manual build)
    // leaves an INVALID index that IF NOT EXISTS then skips silently forever —
    // queries would just quietly not use it. Non-fatal on purpose: an invalid
    // index is a performance defect, not a correctness one, and failing the
    // boot for it would trade a warning for a crash loop that needs the same
    // manual DROP INDEX CONCURRENTLY anyway.
    warn_invalid_indexes(&client).await;

    tracing::info!(
        target: "schema",
        procedures = PROCEDURES.len(),
        server_version = %server_version,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "applied schema.sql + procedures (one statement per transaction)"
    );
    Ok(())
}

/// Fail the boot on a PostgreSQL older than [`MIN_SERVER_VERSION_NUM`], with a
/// message that names the version found and what to do about it. Returns the
/// human-readable version string on success, for the applied-schema log line.
///
/// Takes no lock of any kind, so it is safe anywhere in the boot sequence.
async fn check_server_version(
    client: &tokio_postgres::Client,
) -> Result<String, Box<dyn std::error::Error>> {
    let row = client
        .query_one(
            "SELECT current_setting('server_version_num')::int, current_setting('server_version')",
            &[],
        )
        .await
        .map_err(|e| format!("could not read the PostgreSQL server version: {e}"))?;
    let version_num: i32 = row.get(0);
    let version: String = row.get(1);

    if version_num < MIN_SERVER_VERSION_NUM {
        // Major derived from the constant, never spelled twice: a message that
        // says "requires 14" next to a floor someone raised to 15 is worse than
        // no message, because it sends the operator to check the wrong thing.
        let min_major = MIN_SERVER_VERSION_NUM / 10_000;
        return Err(format!(
            "PostgreSQL {version} (server_version_num={version_num}) is too old: this schema \
             requires PostgreSQL {min_major} or newer (server_version_num >= \
             {MIN_SERVER_VERSION_NUM}). queen.kv uses GENERATED ALWAYS AS ... STORED and \
             starts_with(), so the apply would die part-way through procedures/024_kv.sql and \
             leave the database half-built. Upgrade this server, or point \
             PG_HOST/PG_PORT/PG_DATABASE at a PostgreSQL {min_major}+ instance. \
             QUEEN_APPLY_SCHEMA=0 skips the apply but does not make the broker work on this \
             version."
        )
        .into());
    }

    Ok(version)
}

async fn apply_all(client: &tokio_postgres::Client) -> Result<(), String> {
    // Tracks the session's current lock_timeout so files switch it only when a
    // CREATE INDEX CONCURRENTLY actually appears.
    let mut lock_timeout_ms: Option<u32> = None;
    apply_sql_file(client, "schema.sql", SCHEMA_SQL, &mut lock_timeout_ms).await?;
    for (name, sql) in PROCEDURES {
        let label = format!("procedures/{name}");
        apply_sql_file(client, &label, sql, &mut lock_timeout_ms).await?;
    }
    Ok(())
}

/// Apply one file, statement by statement (see the module doc for why).
/// `lock_timeout_ms` carries the session's current setting across files.
async fn apply_sql_file(
    client: &tokio_postgres::Client,
    file: &str,
    sql: &str,
    lock_timeout_ms: &mut Option<u32>,
) -> Result<(), String> {
    // (1-based ordinal among executable statements, text, kind)
    let items: Vec<(usize, &str, StatementKind)> = split_sql_statements(sql)
        .into_iter()
        .filter(|s| s.executable)
        .enumerate()
        .map(|(i, s)| (i + 1, s.text, statement_kind(s.text)))
        .collect();

    let mut i = 0;
    while i < items.len() {
        let (ordinal, text, kind) = items[i];
        match kind {
            StatementKind::CreateIndexConcurrently => {
                // CIC performs its own waits (on lockers and snapshots) as
                // heavyweight lock waits; a 2s lock_timeout would kill a
                // perfectly healthy build half-way and leave an INVALID index.
                // It does not block writes, so waiting is the safe side. No
                // retry either: after a failed CIC the leftover invalid index
                // makes IF NOT EXISTS a silent no-op, so a blind retry would
                // "succeed" while fixing nothing.
                set_lock_timeout(client, 0, lock_timeout_ms).await?;
                if let Err(e) = client.batch_execute(text).await {
                    return Err(format!(
                        "{file}: statement {ordinal} ({}): {}. NOTE: an interrupted CREATE INDEX \
                         CONCURRENTLY leaves an INVALID index behind, and IF NOT EXISTS then \
                         skips it silently on every later boot — drop it (DROP INDEX \
                         CONCURRENTLY IF EXISTS <name>) before booting again. The applier warns \
                         about invalid queen.* indexes after every successful apply.",
                        preview(text),
                        pg_error_text(&e)
                    ));
                }
                i += 1;
            }
            StatementKind::Drop => {
                // Group consecutive DROPs plus the first following statement
                // into one transaction, so drop-then-recreate (019's trigger
                // pair) stays atomic and a live HA peer never observes the gap.
                // Never absorb a CIC — it cannot run inside a transaction block.
                let start = i;
                while i < items.len() && items[i].2 == StatementKind::Drop {
                    i += 1;
                }
                if i < items.len() && items[i].2 != StatementKind::CreateIndexConcurrently {
                    i += 1;
                }
                let group = &items[start..i];
                let group_sql: String = group.iter().map(|(_, t, _)| *t).collect();
                let label = if group.len() == 1 {
                    format!("statement {}", group[0].0)
                } else {
                    format!("statements {}-{}", group[0].0, group[group.len() - 1].0)
                };
                set_lock_timeout(client, APPLY_LOCK_TIMEOUT_MS, lock_timeout_ms).await?;
                run_with_lock_retry(
                    client,
                    file,
                    &label,
                    &group_sql,
                    &preview(group[0].1),
                    APPLY_LOCK_ATTEMPTS,
                )
                .await?;
            }
            StatementKind::Plain => {
                set_lock_timeout(client, APPLY_LOCK_TIMEOUT_MS, lock_timeout_ms).await?;
                run_with_lock_retry(
                    client,
                    file,
                    &format!("statement {ordinal}"),
                    text,
                    &preview(text),
                    APPLY_LOCK_ATTEMPTS,
                )
                .await?;
                i += 1;
            }
        }
    }

    tracing::debug!(target: "schema", file, statements = items.len(), "applied");
    Ok(())
}

/// Run one statement (or drop-group) in its own implicit transaction, retrying
/// `40P01` (deadlock — still conceivable for a single multi-lock DDL) and
/// `55P03` (our own `lock_timeout`) with jittered backoff, up to `attempts`
/// times. Anything else fails immediately with full context.
///
/// `attempts` is a parameter rather than a read of [`APPLY_LOCK_ATTEMPTS`] so
/// the PG-gated test can exercise the exhaustion path — and its blocker
/// report — in seconds instead of the production window's ~3 minutes.
async fn run_with_lock_retry(
    client: &tokio_postgres::Client,
    file: &str,
    label: &str,
    sql: &str,
    preview: &str,
    attempts: u32,
) -> Result<(), String> {
    let mut attempt = 1u32;
    loop {
        match client.batch_execute(sql).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let retryable = matches!(
                    e.code(),
                    Some(&SqlState::T_R_DEADLOCK_DETECTED) | Some(&SqlState::LOCK_NOT_AVAILABLE)
                );
                if retryable && attempt < attempts {
                    tracing::warn!(
                        target: "schema",
                        file,
                        statement = %label,
                        attempt,
                        sqlstate = e.code().map(|c| c.code()).unwrap_or("?"),
                        preview = %preview,
                        "DDL blocked by live traffic; backing off and retrying"
                    );
                    tokio::time::sleep(lock_backoff(attempt)).await;
                    attempt += 1;
                    continue;
                }
                let exhausted = if retryable {
                    // The ONE place the applier gives up on a contended lock,
                    // so it is also the one place worth a round trip: without
                    // the blocker list the operator gets "could not obtain
                    // lock" and no way to find out who held it (the incident's
                    // boot log said exactly that, ten times).
                    let window_s = patience_window_ms(attempts) / 1_000;
                    let blockers = blocker_report(client, BLOCKER_MIN_AGE_S).await;
                    format!(
                        " [gave up after {attempt} attempts over up to ~{window_s}s under lock \
                         contention; lock_timeout={APPLY_LOCK_TIMEOUT_MS}ms per attempt, retried \
                         on SQLSTATE 40P01/55P03]. {blockers} waiting cannot outlast an \
                         idle-in-transaction session or pg_dump; clear the listed pid(s) and \
                         reboot"
                    )
                } else {
                    String::new()
                };
                return Err(format!(
                    "{file}: {label} ({preview}): {}{exhausted}",
                    pg_error_text(&e)
                ));
            }
        }
    }
}

/// Minimum session age for [`blocker_report`]. A holder younger than this is
/// one the window would have outlasted, so listing it would only bury the
/// standing holder that actually beat us. Applied ONLY where the age is
/// knowable — see the fail-open note on the query.
const BLOCKER_MIN_AGE_S: i64 = 5;

/// Blocker rows named in the FATAL error. More than a handful is not a list an
/// operator reads, it is a list they skim past — and the oldest ones are the
/// ones that beat a 3-minute window. The message states the TOTAL alongside, so
/// a cap that hides something says so.
const BLOCKER_ROWS: i64 = 10;

/// The two verdicts the report can reach without naming a pid. Constants
/// because they are OPPOSITE operational instructions — "nobody was holding
/// anything, retry" versus "I could not find out, go look yourself" — and the
/// 2026-08-24 15:15:33Z miss was the report printing the first when the truth
/// was the second. A test pins that they never blur.
const NO_BLOCKERS: &str = "no session held a granted relation lock for more than";
const UNKNOWN_BLOCKERS: &str = "the blocking sessions could NOT be listed";

/// Name the sessions that were holding conflicting locks when the patience
/// window ran out, for the boot's FATAL error.
///
/// Shape chosen (2026-08-24), out of the three that were on the table:
///
///   * `pg_blocking_pids(pg_backend_pid())` is the precise answer and is NOT
///     available: by the time `55P03` has come back, this backend is waiting
///     for nothing and the array is empty. Asking DURING the wait needs a
///     second connection running concurrently with the blocked statement — the
///     applier owns exactly one connection, on purpose (it carries session
///     state: `lock_timeout`, `application_name`, the schema advisory lock);
///   * the relation from the error's context does not exist: `lock_timeout`
///     reports "canceling statement due to lock timeout" and names nothing, so
///     that path would print a blank list precisely when it matters;
///   * so: every session holding a GRANTED relation lock on a non-system
///     relation, oldest first. Slightly wider than "the blockers of THIS
///     statement" — it cannot know which relation the statement wanted — and
///     that is the trade: it needs no second connection, no concurrency and no
///     error-text parsing, and it always names the class of holder that can
///     actually beat the window (an idle-in-transaction cycle holder, a long
///     collector query, pg_dump).
///
/// ## `pg_locks` decides, `pg_stat_activity` only decorates (15:15:33Z miss)
///
/// The first cut of this query INNER JOINed `pg_stat_activity` and gated
/// inclusion on `now() - COALESCE(xact_start, query_start, backend_start)`.
/// On the cell it then reported "no session held a granted relation lock" while
/// pid 99511 sat `idle in transaction` on `queen.log_partitions` — because
/// PostgreSQL MASKS that view: for a session owned by a role the reader is not
/// a member of (and without `pg_read_all_stats`), `state`, `xact_start`,
/// `query_start` AND `backend_start` all come back NULL and `query` becomes
/// `<insufficient privilege>`. `NULL > interval` is NULL, the row failed the
/// WHERE, and the one blocker that mattered vanished. Two brokers under
/// different roles — or one broker and anybody else's session — is all it takes.
///
/// So: `pg_locks` is the source of truth (it is visible to EVERY role, fastpath
/// entries included — verified), `pg_stat_activity` is a LEFT JOIN, and an age
/// that cannot be read counts as INFINITELY OLD. A diagnostic must fail open:
/// listing a young session costs a line of text, hiding a real holder costs the
/// incident. Unknown-age rows sort first for the same reason.
///
/// Read-only, one round trip, on the applier's own connection AFTER the last
/// failure; every failure mode degrades to a sentence, because a boot that is
/// already dying must not also die in its own diagnostics. `min_age_s` is a
/// parameter rather than a read of [`BLOCKER_MIN_AGE_S`] so the PG-gated tests
/// can age a fixture in seconds instead of the live 90+.
///
/// The schema filter is "not a system catalog" rather than `queen.*`: this
/// deployment also owns `queen_streams`, and a blocker there is just as fatal.
async fn blocker_report(client: &tokio_postgres::Client, min_age_s: i64) -> String {
    const SQL: &str = "WITH holders AS ( \
                           SELECT l.pid AS pid, \
                                  COALESCE(a.usename, '?') AS usename, \
                                  COALESCE(a.state, '?') AS state, \
                                  (EXTRACT(EPOCH FROM now() \
                                      - COALESCE(a.xact_start, a.query_start, a.backend_start) \
                                  ))::bigint AS age_s, \
                                  string_agg(DISTINCT n.nspname || '.' || c.relname || ' ' \
                                             || l.mode, ', ') AS locks, \
                                  left(COALESCE(a.query, ''), 80) AS query \
                           FROM pg_locks l \
                           JOIN pg_class c ON c.oid = l.relation \
                           JOIN pg_namespace n ON n.oid = c.relnamespace \
                           LEFT JOIN pg_stat_activity a ON a.pid = l.pid \
                           WHERE l.granted AND l.locktype = 'relation' \
                             AND l.pid <> pg_backend_pid() \
                             AND n.nspname NOT IN ('pg_catalog', 'information_schema', \
                                                   'pg_toast') \
                             AND COALESCE(now() - COALESCE(a.xact_start, a.query_start, \
                                                           a.backend_start), \
                                          interval '100 years') \
                                 > ($1::bigint * interval '1 second') \
                           GROUP BY 1, 2, 3, 4, 6 \
                       ) \
                       SELECT pid, usename, state, age_s, locks, query, count(*) OVER () \
                       FROM holders \
                       ORDER BY age_s DESC NULLS FIRST \
                       LIMIT $2::bigint";
    let rows = match client.query(SQL, &[&min_age_s, &BLOCKER_ROWS]).await {
        Ok(rows) => rows,
        // NOT the empty-list sentence: "I could not find out" must never read
        // as "there was nobody". The operator's next move differs completely.
        Err(e) => return format!("{UNKNOWN_BLOCKERS} ({});", pg_error_text(&e)),
    };
    if rows.is_empty() {
        return format!(
            "{NO_BLOCKERS} {min_age_s}s at the time of the failure — the blocker was either \
             shorter-lived than that (retry the boot) or the contention is a burst of short \
             writers;"
        );
    }
    let total: i64 = rows[0].get(6);
    let mut s = format!(
        "sessions holding granted relation locks (oldest first, {} shown of {total}):",
        rows.len()
    );
    let mut masked = false;
    for row in &rows {
        let pid: i32 = row.get(0);
        let usename: String = row.get(1);
        let state: String = row.get(2);
        // NULL when this role may not read that session's stats — the whole
        // point of the fail-open above, so it must survive decoding too.
        let age: Option<i64> = row.get(3);
        let locks: String = row.get(4);
        let query: String = row.get(5);
        masked |= age.is_none();
        let age = age.map_or_else(|| "?".to_string(), |a| format!("{a}s"));
        s.push_str(&format!(
            " [pid {pid} user={usename} state=\"{state}\" age={age} locks=\"{locks}\" \
             query=\"{}\"]",
            query.split_whitespace().collect::<Vec<_>>().join(" ")
        ));
    }
    if masked {
        s.push_str(
            " (age/state \"?\" means this role cannot read that session's pg_stat_activity — \
             GRANT pg_read_all_stats to it to see them; the LOCK itself is not in doubt)",
        );
    }
    s.push(';');
    s
}

/// `SET lock_timeout` only when the wanted value differs from the session's
/// current one. `0` disables the timeout (the CREATE INDEX CONCURRENTLY case).
async fn set_lock_timeout(
    client: &tokio_postgres::Client,
    ms: u32,
    current: &mut Option<u32>,
) -> Result<(), String> {
    if *current == Some(ms) {
        return Ok(());
    }
    client
        .batch_execute(&format!("SET lock_timeout = {ms}"))
        .await
        .map_err(|e| format!("SET lock_timeout = {ms}: {}", pg_error_text(&e)))?;
    *current = Some(ms);
    Ok(())
}

/// WARN (never fail) about invalid indexes in the `queen` schema — the durable
/// residue of an interrupted `CREATE INDEX CONCURRENTLY`, which `IF NOT EXISTS`
/// then skips silently on every later boot.
async fn warn_invalid_indexes(client: &tokio_postgres::Client) {
    const SQL: &str = "SELECT n.nspname || '.' || ic.relname \
                       FROM pg_index x \
                       JOIN pg_class ic ON ic.oid = x.indexrelid \
                       JOIN pg_class tc ON tc.oid = x.indrelid \
                       JOIN pg_namespace n ON n.oid = tc.relnamespace \
                       WHERE n.nspname = 'queen' AND NOT x.indisvalid";
    match client.query(SQL, &[]).await {
        Ok(rows) => {
            for row in rows {
                let index: String = row.get(0);
                tracing::warn!(
                    target: "schema",
                    index = %index,
                    "INVALID index (interrupted CREATE INDEX CONCURRENTLY?): queries will not \
                     use it and IF NOT EXISTS will not rebuild it. Fix: DROP INDEX CONCURRENTLY \
                     IF EXISTS <name>, then restart the broker (or re-create it manually)."
                );
            }
        }
        Err(e) => {
            tracing::warn!(target: "schema", "invalid-index check failed: {}", pg_error_text(&e));
        }
    }
}

/// Human-readable text for a tokio_postgres error. `Display` on a db error can
/// be as terse as "db error" (the incident's boot log carried exactly that and
/// nothing else); the SQLSTATE, message, detail and hint live on the DbError.
fn pg_error_text(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        let mut s = format!(
            "{}: {} (SQLSTATE {})",
            db.severity(),
            db.message(),
            db.code().code()
        );
        if let Some(d) = db.detail() {
            s.push_str(&format!("; detail: {d}"));
        }
        if let Some(h) = db.hint() {
            s.push_str(&format!("; hint: {h}"));
        }
        s
    } else {
        // Connection-level error: Display plus the source chain, deduplicated.
        let mut s = e.to_string();
        let mut src = std::error::Error::source(e);
        while let Some(cause) = src {
            let cs = cause.to_string();
            if !s.contains(&cs) {
                s.push_str(": ");
                s.push_str(&cs);
            }
            src = cause.source();
        }
        s
    }
}

// ---------------------------------------------------------------------------
// SQL statement splitting
// ---------------------------------------------------------------------------

/// One fragment of a file. Fragments PARTITION the input: concatenating the
/// `text` of every fragment reproduces the file byte for byte (a comment
/// between statements travels with the statement that follows it). `executable`
/// is false for fragments that contain only whitespace and comments.
#[derive(Debug)]
struct SqlStatement<'a> {
    text: &'a str,
    executable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementKind {
    Plain,
    Drop,
    CreateIndexConcurrently,
}

/// Split a file into statements at top-level `;`, honoring PostgreSQL lexing:
/// `'...'` strings (with `''` escapes), `E'...'` strings (backslash escapes),
/// `"..."` identifiers, `--` line comments, nested `/* ... */` block comments,
/// and `$tag$ ... $tag$` dollar quoting (the function bodies). A `$` that does
/// not open a valid tag (`$1`, `$1::text::uuid`) is ordinary code.
///
/// The splitter does not validate SQL. Unterminated constructs swallow the rest
/// of the file into the final fragment — the server then reports the real error
/// when that fragment is executed, exactly as it did for the whole file before.
fn split_sql_statements(sql: &str) -> Vec<SqlStatement<'_>> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut has_code = false;

    while i < b.len() {
        match b[i] {
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                // Block comments NEST in PostgreSQL.
                let mut depth = 1u32;
                i += 2;
                while i < b.len() && depth > 0 {
                    if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                        depth += 1;
                        i += 2;
                    } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
            }
            b'\'' => {
                let estring = is_estring_quote(b, i);
                has_code = true;
                i += 1;
                while i < b.len() {
                    if estring && b[i] == b'\\' {
                        i += 2; // the escaped char, whatever it is (incl. \')
                        continue;
                    }
                    if b[i] == b'\'' {
                        if b.get(i + 1) == Some(&b'\'') {
                            i += 2; // '' escape
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'"' => {
                has_code = true;
                i += 1;
                while i < b.len() {
                    if b[i] == b'"' {
                        if b.get(i + 1) == Some(&b'"') {
                            i += 2; // "" escape
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'$' => {
                has_code = true;
                if let Some(delim_len) = dollar_tag_len(b, i) {
                    let delim = &b[i..i + delim_len];
                    i += delim_len;
                    while i < b.len() {
                        if b[i] == b'$' && b[i..].starts_with(delim) {
                            i += delim_len;
                            break;
                        }
                        i += 1;
                    }
                } else {
                    i += 1; // positional parameter or bare $: ordinary code
                }
            }
            b';' => {
                i += 1;
                out.push(SqlStatement {
                    text: &sql[start..i],
                    executable: has_code,
                });
                start = i;
                has_code = false;
            }
            c if c.is_ascii_whitespace() => {
                i += 1;
            }
            _ => {
                has_code = true;
                i += 1;
            }
        }
    }

    if start < b.len() {
        out.push(SqlStatement {
            text: &sql[start..],
            executable: has_code,
        });
    }
    out
}

/// Is the quote at `quote_idx` the body-opener of an `E'...'` string? True when
/// the previous byte is `E`/`e` and the one before that is not an identifier
/// character (so `TABLE'x'` and `CASE'x'` stay ordinary strings).
fn is_estring_quote(b: &[u8], quote_idx: usize) -> bool {
    if quote_idx == 0 {
        return false;
    }
    let p = b[quote_idx - 1];
    if p != b'E' && p != b'e' {
        return false;
    }
    if quote_idx == 1 {
        return true;
    }
    !is_ident_char(b[quote_idx - 2])
}

/// If `b[i]` (`$`) opens a dollar-quote delimiter, its total length (`$$` = 2,
/// `$fn$` = 4). Tags are identifier-like and may not start with a digit, which
/// is what keeps `$1`, `$2::uuid` and `$1$` out.
fn dollar_tag_len(b: &[u8], i: usize) -> Option<usize> {
    let mut j = i + 1;
    while j < b.len() && is_ident_char(b[j]) {
        j += 1;
    }
    if j < b.len() && b[j] == b'$' {
        if j > i + 1 && b[i + 1].is_ascii_digit() {
            return None;
        }
        Some(j - i + 1)
    } else {
        None
    }
}

/// Identifier characters as PostgreSQL's lexer sees them for dollar-quote tags:
/// ASCII alphanumerics, underscore, and any non-ASCII byte.
fn is_ident_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c >= 0x80
}

/// Byte offset of the first character that is neither whitespace nor part of a
/// comment. Only ever lands on an ASCII byte or a UTF-8 lead byte, so slicing
/// at the result is safe.
fn skip_ws_and_comments(b: &[u8], mut i: usize) -> usize {
    loop {
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
        if i + 1 < b.len() && b[i] == b'-' && b[i + 1] == b'-' {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if i + 1 < b.len() && b[i] == b'/' && b[i + 1] == b'*' {
            let mut depth = 1u32;
            i += 2;
            while i < b.len() && depth > 0 {
                if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                    depth += 1;
                    i += 2;
                } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            continue;
        }
        return i;
    }
}

/// Up to `max` leading keyword-shaped tokens of a statement, uppercased, with
/// comments and whitespace skipped. Stops at the first non-word character.
fn leading_keywords(stmt: &str, max: usize) -> Vec<String> {
    let b = stmt.as_bytes();
    let mut i = 0usize;
    let mut out = Vec::new();
    while out.len() < max {
        i = skip_ws_and_comments(b, i);
        let start = i;
        while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
            i += 1;
        }
        if i == start {
            break;
        }
        out.push(stmt[start..i].to_ascii_uppercase());
    }
    out
}

fn statement_kind(stmt: &str) -> StatementKind {
    let kws = leading_keywords(stmt, 4);
    let k: Vec<&str> = kws.iter().map(|s| s.as_str()).collect();
    match k.as_slice() {
        ["DROP", ..] => StatementKind::Drop,
        ["CREATE", "INDEX", "CONCURRENTLY", ..] => StatementKind::CreateIndexConcurrently,
        ["CREATE", "UNIQUE", "INDEX", "CONCURRENTLY"] => StatementKind::CreateIndexConcurrently,
        _ => StatementKind::Plain,
    }
}

/// One-line, comment-stripped, whitespace-collapsed prefix of a statement for
/// log lines and error messages.
fn preview(stmt: &str) -> String {
    let start = skip_ws_and_comments(stmt.as_bytes(), 0);
    let collapsed = stmt[start..].split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() > 96 {
        let mut s: String = collapsed.chars().take(96).collect();
        s.push('…');
        s
    } else {
        collapsed
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The pure tests pin the splitter/classifier against the lexing hazards of the
/// corpus (dollar-quoted bodies, `$1::text::uuid`, comments) and against the
/// embedded corpus itself (lossless partition of every file).
///
/// The `#[ignore]` tests need a throwaway Postgres and replay the 2026-08-24
/// boot-deadlock geometry deterministically — the one-transaction-per-file
/// shape must deadlock, the per-statement shape must not, and the lock_timeout
/// retry must survive a long-held conflicting lock:
///
/// ```bash
/// docker run --rm -d --name queen-schema-pg -e POSTGRES_PASSWORD=postgres -p 5473:5432 postgres:16-alpine
/// QUEEN_EMBEDDED_TEST_PG=localhost:5473 cargo test -p queen-engine --lib schema -- --ignored --nocapture
/// ```
///
/// (A panicking run leaves its scratch schema behind in the throwaway
/// container; names are nanosecond-unique so reruns never collide.)
#[cfg(test)]
mod tests {
    use super::*;

    fn exec_texts(sql: &str) -> Vec<&str> {
        split_sql_statements(sql)
            .into_iter()
            .filter(|s| s.executable)
            .map(|s| s.text)
            .collect()
    }

    #[test]
    fn splits_plain_statements() {
        let v = exec_texts("SELECT 1;\nSELECT 2;");
        assert_eq!(v, vec!["SELECT 1;", "\nSELECT 2;"]);
    }

    #[test]
    fn semicolon_in_string_does_not_split() {
        let v = exec_texts("INSERT INTO t VALUES ('a;b');SELECT 1;");
        assert_eq!(v.len(), 2);
        assert!(v[0].contains("'a;b'"));
    }

    #[test]
    fn doubled_quote_escape() {
        let v = exec_texts("SELECT 'it''s; fine';SELECT 2;");
        assert_eq!(v.len(), 2);
        assert!(v[0].contains("'it''s; fine'"));
    }

    #[test]
    fn estring_backslash_quote_stays_inside() {
        // E'a\'b;c' — the \' does not close the string, so the ; stays inside.
        let v = exec_texts("SELECT E'a\\'b;c';SELECT 2;");
        assert_eq!(v.len(), 2, "backslash escape in E-string mishandled: {v:?}");
        assert!(v[0].contains(";c'"));
    }

    #[test]
    fn estring_prefix_needs_word_boundary() {
        // TABLE'x;y' — the quote follows an identifier, so it is NOT an
        // E-string and backslashes are literal.
        let v = exec_texts("SELECT 'CASE'||'x\\';SELECT 2;");
        assert_eq!(v.len(), 2);
        assert!(v[0].ends_with("'x\\';"));
    }

    #[test]
    fn quoted_identifier_with_semicolon() {
        let v = exec_texts(r#"CREATE TABLE "we;ird" (i int);SELECT 1;"#);
        assert_eq!(v.len(), 2);
        assert!(v[0].contains(r#""we;ird""#));
    }

    #[test]
    fn line_comment_semicolon_ignored() {
        let v = exec_texts("-- nope; not a split\nSELECT 1;");
        assert_eq!(v.len(), 1);
        assert!(v[0].starts_with("-- nope"));
    }

    #[test]
    fn nested_block_comment_ignored() {
        let v = exec_texts("/* a /* b; */ c; */ SELECT 1;");
        assert_eq!(v.len(), 1);
    }

    #[test]
    fn dollar_body_semicolons_stay_inside() {
        let v = exec_texts(
            "CREATE FUNCTION f() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql;SELECT 1;",
        );
        assert_eq!(v.len(), 2);
        assert!(v[0].contains("END; $$"));
    }

    #[test]
    fn tagged_dollar_with_inner_dollar_quotes() {
        let v = exec_texts(
            "CREATE FUNCTION g() RETURNS text AS $fn$ SELECT '$'; SELECT $$x;y$$; $fn$ LANGUAGE sql;",
        );
        assert_eq!(v.len(), 1, "inner $$ must not close a $fn$ body: {v:?}");
    }

    #[test]
    fn positional_params_are_not_dollar_quotes() {
        // The corpus GOTCHA shape: $1::text::uuid.
        let v = exec_texts("UPDATE t SET a = $1::text::uuid, b = $2 WHERE c = $3;DELETE FROM t;");
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn last_statement_without_semicolon() {
        let v = exec_texts("SELECT 1;SELECT 2");
        assert_eq!(v, vec!["SELECT 1;", "SELECT 2"]);
    }

    #[test]
    fn comment_only_tail_is_not_executable() {
        let all = split_sql_statements("SELECT 1;\n-- trailing note\n");
        assert_eq!(all.len(), 2);
        assert!(all[0].executable);
        assert!(!all[1].executable);
    }

    #[test]
    fn unterminated_dollar_swallows_rest() {
        // Garbage in, one fragment out; the server reports the real error.
        let v = exec_texts("SELECT $$oops; never closed");
        assert_eq!(v.len(), 1);
    }

    #[test]
    fn classifier_kinds() {
        use StatementKind::*;
        assert_eq!(statement_kind("CREATE INDEX CONCURRENTLY IF NOT EXISTS i ON t(x);"), CreateIndexConcurrently);
        assert_eq!(statement_kind("CREATE UNIQUE INDEX CONCURRENTLY i ON t(x);"), CreateIndexConcurrently);
        assert_eq!(
            statement_kind("-- c\n/* c */ create index\n  concurrently if not exists i ON t(x);"),
            CreateIndexConcurrently
        );
        assert_eq!(statement_kind("CREATE INDEX IF NOT EXISTS i ON t(x);"), Plain);
        assert_eq!(statement_kind("CREATE TABLE t (i int);"), Plain);
        assert_eq!(statement_kind("ALTER TABLE t ADD COLUMN IF NOT EXISTS c int;"), Plain);
        assert_eq!(statement_kind("DROP FUNCTION IF EXISTS f(TEXT);"), Drop);
        assert_eq!(statement_kind("  -- x\nDROP TRIGGER IF EXISTS tr ON t;"), Drop);
    }

    /// Every embedded file must partition losslessly (the splitter can lose or
    /// duplicate NOTHING) and yield at least one executable statement.
    #[test]
    fn corpus_splits_losslessly() {
        let mut files: Vec<(&str, &str)> = vec![("schema.sql", SCHEMA_SQL)];
        files.extend(PROCEDURES.iter().copied());
        for (name, sql) in files {
            let stmts = split_sql_statements(sql);
            let rejoined: String = stmts.iter().map(|s| s.text).collect();
            assert_eq!(rejoined, sql, "{name}: fragments must concatenate back to the file");
            let exec = stmts.iter().filter(|s| s.executable).count();
            assert!(exec >= 1, "{name}: no executable statements found");
        }
    }

    /// The 2026-08-24 retention watermark indexes arrive on an already-populated
    /// hot table, so they MUST be CREATE INDEX CONCURRENTLY (module doc,
    /// authoring rules). Pins both the 001 statements and the classifier that
    /// routes them outside a transaction.
    #[test]
    fn corpus_001_retention_indexes_are_concurrent() {
        let (_, sql) = PROCEDURES
            .iter()
            .find(|(n, _)| *n == "001_log_schema.sql")
            .expect("001 in PROCEDURES");
        let cic = split_sql_statements(sql)
            .iter()
            .filter(|s| s.executable && statement_kind(s.text) == StatementKind::CreateIndexConcurrently)
            .count();
        assert_eq!(
            cic, 2,
            "001_log_schema.sql must carry exactly the two retention watermark \
             indexes as CREATE INDEX CONCURRENTLY"
        );
    }

    /// The patience window (2026-08-24, second half). Four properties, all of
    /// them load-bearing: no single sleep may exceed the cap (a gap longer than
    /// the slices is patience spent asleep, not waiting); the deterministic
    /// spine still CLIMBS; the jitter is alive, because a schedule that draws
    /// the same number every time de-phases nothing and two brokers rolled
    /// together keep colliding; and the total lands in the ~3 minute band the
    /// module doc promises — comfortably past the 40-115s holds the cell
    /// measured, which the old 10-attempt (~30s) window could only have
    /// outlasted by luck.
    #[test]
    fn the_backoff_schedule_is_bounded_monotone_and_jittered() {
        let bases: Vec<u64> = (1..=APPLY_LOCK_ATTEMPTS)
            .map(|a| lock_backoff_base(a).as_millis() as u64)
            .collect();
        assert!(bases.windows(2).all(|w| w[1] >= w[0]), "the base schedule must not go backwards");
        assert_eq!(*bases.last().unwrap(), 2_000, "the base saturates at one slice");

        for attempt in 1..=APPLY_LOCK_ATTEMPTS {
            let base = lock_backoff_base(attempt).as_millis() as u64;
            for _ in 0..64 {
                let ms = lock_backoff(attempt).as_millis() as u64;
                assert!(ms >= base, "attempt {attempt}: {ms}ms is below its base {base}ms");
                assert!(ms <= base + base / 2, "attempt {attempt}: {ms}ms is more than +50%");
                assert!(ms <= APPLY_BACKOFF_MAX_MS, "attempt {attempt}: {ms}ms breaks the cap");
            }
        }

        // 64 draws at a 2000ms base cannot all collide unless the randomness is
        // dead — which is indistinguishable from having no jitter at all.
        let draws: std::collections::HashSet<u128> = (0..64)
            .map(|_| lock_backoff(APPLY_LOCK_ATTEMPTS).as_millis())
            .collect();
        assert!(draws.len() > 1, "the backoff is not jittered");

        let window_ms = patience_window_ms(APPLY_LOCK_ATTEMPTS);
        assert!(
            (150_000..=210_000).contains(&window_ms),
            "the patience window is {window_ms}ms, outside the ~3 minute band the module doc \
             promises (attempts={APPLY_LOCK_ATTEMPTS}, slice={APPLY_LOCK_TIMEOUT_MS}ms)"
        );
        assert!(
            window_ms > 115_000,
            "the window must outlast the 40-115s collector holds measured on the cell"
        );
    }

    // -- PG-gated tests (the 2026-08-24 geometry, deterministically) ---------

    async fn pg_target() -> (String, u16) {
        let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
            .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
        target
            .split_once(':')
            .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
            .unwrap_or((target.clone(), 5432))
    }

    async fn connect() -> tokio_postgres::Client {
        connect_as("postgres", "postgres").await
    }

    /// Connect as an arbitrary role. The masked-blocker test needs an applier
    /// that is NOT the blocker's role — which is the whole difference between
    /// the fixture that passed and the cell that did not.
    async fn connect_as(user: &str, password: &str) -> tokio_postgres::Client {
        let (host, port) = pg_target().await;
        let (c, conn) = tokio_postgres::connect(
            &format!("host={host} port={port} user={user} password={password} dbname=postgres"),
            tokio_postgres::NoTls,
        )
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
        tokio::spawn(async move {
            let _ = conn.await;
        });
        c
    }

    fn unique() -> u128 {
        // Nanos alone collide when parallel tests hit the same clock tick
        // (macOS reports microsecond granularity); the counter breaks the tie.
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        nanos * 100 + u128::from(SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % 100)
    }

    /// Scratch table with one locker-visible row, in its own schema.
    /// Returns (schema, table) fully-unique names.
    async fn scratch_table(c: &tokio_postgres::Client) -> (String, String) {
        let n = unique();
        let schema = format!("qsch_{n}");
        let table = format!("t_{n}");
        c.batch_execute(&format!(
            "CREATE SCHEMA {schema}; \
             CREATE TABLE {schema}.{table} (id int PRIMARY KEY, v int NOT NULL); \
             INSERT INTO {schema}.{table} VALUES (1, 0); \
             CREATE INDEX {table}_v ON {schema}.{table} (v);"
        ))
        .await
        .expect("scratch table");
        (schema, table)
    }

    /// Wait until an ungranted AccessExclusiveLock request on `table` shows up
    /// in pg_locks — i.e. the applier has reached its ALTER and is queued.
    async fn wait_for_waiting_ae(c: &tokio_postgres::Client, table: &str) {
        for _ in 0..200 {
            let n: i64 = c
                .query_one(
                    "SELECT count(*) FROM pg_locks l JOIN pg_class r ON r.oid = l.relation \
                     WHERE r.relname = $1 AND l.mode = 'AccessExclusiveLock' AND NOT l.granted",
                    &[&table],
                )
                .await
                .expect("pg_locks probe")
                .get(0);
            if n > 0 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("no waiting AccessExclusiveLock on {table} appeared");
    }

    /// The INCIDENT, replayed: one implicit transaction carrying a no-op
    /// `CREATE INDEX IF NOT EXISTS` (takes and KEEPS ShareLock) followed by an
    /// `ALTER TABLE ADD COLUMN IF NOT EXISTS` (waits for AccessExclusive),
    /// against one pusher-shaped transaction that holds RowShare (FOR UPDATE)
    /// and then upgrades to RowExclusive (UPDATE). PostgreSQL must kill one
    /// side with 40P01 — this is the negative control that proves the fixture
    /// really builds the cycle the per-statement test below dissolves.
    #[tokio::test]
    #[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
    async fn one_transaction_per_file_deadlocks_under_the_upgrade_cycle() {
        let monitor = connect().await;
        let (schema, table) = scratch_table(&monitor).await;

        let loader = connect().await;
        loader.batch_execute("BEGIN").await.expect("begin");
        loader
            .query_one(&format!("SELECT v FROM {schema}.{table} WHERE id = 1 FOR UPDATE"), &[])
            .await
            .expect("for update");

        let applier = connect().await;
        let file_shaped = format!(
            "CREATE INDEX IF NOT EXISTS {table}_v ON {schema}.{table} (v); \
             ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS extra int;"
        );
        let apply_task = tokio::spawn(async move { applier.batch_execute(&file_shaped).await });

        wait_for_waiting_ae(&monitor, &table).await;

        // The pusher's upgrade: RowExclusive conflicts with the applier's HELD
        // ShareLock, while the applier's AE wait conflicts with our HELD
        // RowShare. Cycle closed; the deadlock detector fires within ~1s.
        let update = loader
            .execute(&format!("UPDATE {schema}.{table} SET v = v + 1 WHERE id = 1"), &[])
            .await;
        let apply_res = apply_task.await.expect("join");

        let codes: Vec<String> = [
            update.err().and_then(|e| e.code().map(|c| c.code().to_string())),
            apply_res.err().and_then(|e| e.code().map(|c| c.code().to_string())),
        ]
        .into_iter()
        .flatten()
        .collect();
        assert!(
            codes.iter().any(|c| c == "40P01"),
            "expected a 40P01 deadlock on one side, got {codes:?} — the fixture no longer \
             reproduces the 2026-08-24 geometry"
        );

        let _ = loader.batch_execute("ROLLBACK").await;
        let _ = monitor
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await;
    }

    /// The FIX: the same two statements through apply_sql_file, against the
    /// same pusher-shaped transaction. Per-statement transactions mean the
    /// applier holds nothing while it waits, the pusher's upgrade jumps the
    /// lock queue (it already holds a lock on the relation), commits, and the
    /// ALTER then lands. No deadlock is possible and nobody errors.
    #[tokio::test]
    #[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
    async fn per_statement_apply_survives_the_upgrade_cycle() {
        let monitor = connect().await;
        let (schema, table) = scratch_table(&monitor).await;

        let loader = connect().await;
        loader.batch_execute("BEGIN").await.expect("begin");
        loader
            .query_one(&format!("SELECT v FROM {schema}.{table} WHERE id = 1 FOR UPDATE"), &[])
            .await
            .expect("for update");

        let applier = connect().await;
        let file_shaped = format!(
            "CREATE INDEX IF NOT EXISTS {table}_v ON {schema}.{table} (v); \
             ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS extra int;"
        );
        let apply_task = tokio::spawn(async move {
            let mut lt = None;
            apply_sql_file(&applier, "fixture.sql", &file_shaped, &mut lt).await
        });

        wait_for_waiting_ae(&monitor, &table).await;

        // Must SUCCEED (no 40P01): the applier holds nothing while queued.
        loader
            .execute(&format!("UPDATE {schema}.{table} SET v = v + 1 WHERE id = 1"), &[])
            .await
            .expect("the pusher-shaped upgrade must not deadlock against the per-statement apply");
        loader.batch_execute("COMMIT").await.expect("commit");

        apply_task
            .await
            .expect("join")
            .expect("per-statement apply must succeed once the pusher commits");

        let cols: i64 = monitor
            .query_one(
                "SELECT count(*) FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 AND column_name = 'extra'",
                &[&schema, &table],
            )
            .await
            .expect("columns probe")
            .get(0);
        assert_eq!(cols, 1, "the ALTER must actually have landed");

        let _ = monitor
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await;
    }

    /// The lock_timeout + backoff leg: a conflicting lock held for ~5s forces
    /// at least two 55P03 expiries (2s each), and the applier must keep
    /// retrying and land the ALTER once the holder commits, instead of failing
    /// the boot on first contact.
    #[tokio::test]
    #[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
    async fn lock_timeout_retry_survives_a_long_holder() {
        let monitor = connect().await;
        let (schema, table) = scratch_table(&monitor).await;

        let holder = connect().await;
        holder.batch_execute("BEGIN").await.expect("begin");
        holder
            .batch_execute(&format!("LOCK TABLE {schema}.{table} IN SHARE MODE"))
            .await
            .expect("share lock");

        let applier = connect().await;
        let sql = format!("ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS extra int;");
        let started = std::time::Instant::now();
        let apply_task = tokio::spawn(async move {
            let mut lt = None;
            apply_sql_file(&applier, "fixture.sql", &sql, &mut lt).await
        });

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        holder.batch_execute("COMMIT").await.expect("commit");

        apply_task
            .await
            .expect("join")
            .expect("apply must survive a ~5s conflicting lock via lock_timeout retries");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_secs(4),
            "apply returned after {elapsed:?} — it cannot have gone through the 2s \
             lock_timeout + backoff retry path"
        );

        let cols: i64 = monitor
            .query_one(
                "SELECT count(*) FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 AND column_name = 'extra'",
                &[&schema, &table],
            )
            .await
            .expect("columns probe")
            .get(0);
        assert_eq!(cols, 1);

        let _ = monitor
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await;
    }

    /// The blocker diagnostic: when the window DOES run out, the FATAL error
    /// must name the session that beat it. The fixture is the incident's own
    /// blocker shape — a transaction that SELECTed once and then went `idle in
    /// transaction`, holding AccessShare to its end (the retention cycle holder
    /// before retention.rs stopped running its work list inside the holder
    /// transaction; the rig's collectors; pg_dump). A boot that says only
    /// "could not obtain lock" leaves the operator to find that pid by hand,
    /// which is what the incident actually cost.
    ///
    /// `attempts = 2` on purpose: the production window is ~3 minutes and the
    /// code path under test is identical.
    #[tokio::test]
    #[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
    async fn the_exhausted_window_names_the_idle_in_transaction_blocker() {
        let monitor = connect().await;
        let (schema, table) = scratch_table(&monitor).await;

        let blocker = connect().await;
        let blocker_pid: i32 = blocker
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .expect("blocker pid")
            .get(0);
        blocker.batch_execute("BEGIN").await.expect("begin");
        blocker
            .query_one(&format!("SELECT count(*) FROM {schema}.{table}"), &[])
            .await
            .expect("the blocker's one statement");
        // Age the holder past BLOCKER_MIN_AGE_S: the report deliberately skips
        // younger sessions, because those are the ones the window outlasts.
        tokio::time::sleep(std::time::Duration::from_secs(BLOCKER_MIN_AGE_S as u64 + 1)).await;

        let applier = connect().await;
        let mut lt = None;
        set_lock_timeout(&applier, APPLY_LOCK_TIMEOUT_MS, &mut lt)
            .await
            .expect("lock_timeout");
        let sql = format!("ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS extra int;");
        let err = run_with_lock_retry(&applier, "fixture.sql", "statement 1", &sql, "ALTER", 2)
            .await
            .expect_err("AccessExclusive cannot be had past an idle-in-transaction AccessShare");

        assert!(
            err.contains(&format!("[pid {blocker_pid} ")),
            "the blocker's pid is missing from the FATAL error:\n{err}"
        );
        assert!(
            err.contains("state=\"idle in transaction\""),
            "the blocker's state is missing from the FATAL error:\n{err}"
        );
        assert!(
            err.contains(&format!("{schema}.{table}")),
            "the held relation is missing from the FATAL error:\n{err}"
        );
        assert!(
            err.contains(
                "waiting cannot outlast an idle-in-transaction session or pg_dump; clear the \
                 listed pid(s) and reboot"
            ),
            "the operator instruction is missing from the FATAL error:\n{err}"
        );

        let _ = blocker.batch_execute("ROLLBACK").await;
        let _ = monitor
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await;
    }

    /// The 2026-08-24T15:15:33Z FALSE NEGATIVE, replayed.
    ///
    /// On the cell the report said "no session held a granted relation lock"
    /// while pid 99511 — the OTHER broker's old-build retention holder — sat
    /// `idle in transaction` holding granted AccessShare on
    /// queen.log_partitions, queen.queues and six indexes, 97s after its last
    /// query. Neither the age nor the lock was the problem: PostgreSQL BLANKS
    /// `state`, `xact_start`, `query_start` AND `backend_start` in
    /// pg_stat_activity for a session owned by a role the reader is not a
    /// member of, so the age test that row had to pass was NULL and the INNER
    /// JOIN dropped it.
    ///
    /// So the fixture adds the one thing the test above lacks — blocker and
    /// applier under DIFFERENT roles — plus the live lock shape (a multi-table
    /// SELECT, which locks every index of every relation it plans against, for
    /// the length of the transaction). Against the pre-fix query this test
    /// reports zero holders.
    #[tokio::test]
    #[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
    async fn a_blocker_whose_activity_row_is_masked_is_still_named() {
        let owner = connect().await;
        let (schema, table) = scratch_table(&owner).await;
        let n = unique();
        let other = format!("t2_{n}");
        let role = format!("qsch_app_{n}");
        owner
            .batch_execute(&format!(
                "CREATE TABLE {schema}.{other} (id int PRIMARY KEY, w int NOT NULL); \
                 INSERT INTO {schema}.{other} VALUES (1, 0); \
                 CREATE INDEX {other}_w ON {schema}.{other} (w); \
                 CREATE INDEX {table}_v2 ON {schema}.{table} (v, id); \
                 CREATE INDEX {table}_v3 ON {schema}.{table} ((v + 1));"
            ))
            .await
            .expect("the live holder's lock shape");

        // The blocker: the old build's cycle holder in miniature — one
        // multi-table SELECT, then idle in transaction for the rest of the test.
        let blocker = connect().await;
        let blocker_pid: i32 = blocker
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .expect("blocker pid")
            .get(0);
        blocker.batch_execute("BEGIN").await.expect("begin");
        blocker
            .query_one(
                &format!(
                    "SELECT count(*) FROM {schema}.{table} t \
                     JOIN {schema}.{other} u ON u.id = t.id WHERE t.v < 100"
                ),
                &[],
            )
            .await
            .expect("the holder's work-list-shaped statement");

        // The applier under a role of its own. It needs no grant on the
        // fixture: the report reads catalogs, which every role may read — that
        // asymmetry with pg_stat_activity IS the bug.
        owner
            .batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'applier'"))
            .await
            .expect("applier role");
        let applier = connect_as(&role, "applier").await;

        // Prove the masking is actually in play, or this test proves nothing:
        // a fixture where the applier CAN read the blocker's stats is the one
        // that passed while the cell burned.
        let masked: bool = applier
            .query_one(
                "SELECT xact_start IS NULL AND query_start IS NULL AND backend_start IS NULL \
                 FROM pg_stat_activity WHERE pid = $1",
                &[&blocker_pid],
            )
            .await
            .expect("stat probe")
            .get(0);
        assert!(
            masked,
            "fixture drift: the applier role can read the blocker's pg_stat_activity, so this \
             test no longer reproduces the 15:15:33Z miss"
        );

        // Old enough to qualify on age too, if its age could be read at all.
        tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
        let report = blocker_report(&applier, 1).await;

        assert!(
            !report.contains(NO_BLOCKERS),
            "the report claimed there were no blockers while one was holding \
             {schema}.{table}:\n{report}"
        );
        assert!(
            report.contains(&format!("[pid {blocker_pid} ")),
            "the masked blocker's pid is missing:\n{report}"
        );
        assert!(
            report.contains(&format!("{schema}.{table}")),
            "the held relation is missing:\n{report}"
        );
        assert!(
            report.contains("age=?"),
            "an unreadable age must be listed as unknown, never used to drop the row:\n{report}"
        );
        assert!(
            report.contains("pg_read_all_stats"),
            "the operator is left to guess why the fields are '?':\n{report}"
        );

        let _ = blocker.batch_execute("ROLLBACK").await;
        drop(applier);
        let _ = owner.batch_execute(&format!("DROP SCHEMA {schema} CASCADE")).await;
        let _ = owner.batch_execute(&format!("DROP ROLE IF EXISTS {role}")).await;
    }

    /// "No blockers" is a CLAIM, and it may only be made when it is true: the
    /// list came back empty AND the query that produced it succeeded. A
    /// diagnostic that could not run must say THAT — the 15:15:33Z miss is the
    /// standing proof of how expensive "there was nobody" is when it actually
    /// means "I could not find out".
    #[tokio::test]
    #[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
    async fn a_failed_diagnostic_never_claims_there_were_no_blockers() {
        let client = connect().await;

        // Genuinely empty: no session in a throwaway server is a day old.
        let empty = blocker_report(&client, 86_400).await;
        assert!(empty.contains(NO_BLOCKERS), "an empty list must say so plainly:\n{empty}");
        assert!(!empty.contains(UNKNOWN_BLOCKERS), "an empty list is not an unknown one");

        // Now make the diagnostic itself fail. An aborted transaction rejects
        // every following statement with 25P02 — a real state for a connection
        // that has just had an error, and the cheapest honest injection.
        client.batch_execute("BEGIN").await.expect("begin");
        assert!(client.batch_execute("SELECT 1/0").await.is_err(), "the injection must fail");
        let failed = blocker_report(&client, 5).await;
        assert!(
            failed.contains(UNKNOWN_BLOCKERS),
            "a diagnostic that could not run must say so:\n{failed}"
        );
        assert!(
            !failed.contains(NO_BLOCKERS),
            "a FAILED diagnostic masqueraded as an empty one — the exact shape of the \
             15:15:33Z miss:\n{failed}"
        );
        assert!(
            failed.contains("25P02"),
            "the reason must travel with the verdict, or the operator cannot act on it:\n{failed}"
        );
        let _ = client.batch_execute("ROLLBACK").await;
    }
}
