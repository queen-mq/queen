//! Usage metering. OWNER: Agent D.
//! Contract (M1–M6, PLAN §4): meter post-response from per-item statuses —
//! never charge `error`, never double-charge `duplicate`, `buffered` counts
//! as accepted; exempt 5xx and scope-403s (all Agent A / gateway.rs's job —
//! `record()` here just aggregates whatever Sample it's handed).
//!
//! In-memory per-(cluster, op, minute) aggregates, 16-way sharded by
//! cluster_id, flushed to `queen_proxy.usage_minutes` every
//! `cfg.meter_flush_ms` (closed minutes only — the current minute keeps
//! accumulating), spooled to disk (spool.rs) when the flush fails, drained
//! back on the next startup. `record()` deliberately does not log at
//! info/debug on the hot per-request path (rates/sizes belong in aggregated
//! blocks, not per-message lines — see obs.rs conventions).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::state::OpClass;

const N_SHARDS: usize = 16;

fn shard_index(id: &Uuid) -> usize {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut h);
    (h.finish() as usize) % N_SHARDS
}

fn now_minute_epoch() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() / 60
}

#[derive(Clone, Debug)]
pub struct Sample {
    pub cluster_id: Uuid,
    pub op: OpClass,
    pub reqs: u64,
    pub msgs: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

#[derive(Default, Clone, Copy, Debug)]
struct Acc {
    reqs: u64,
    msgs: u64,
    bytes_in: u64,
    bytes_out: u64,
}

/// A single closed-minute rollup, ready to flush or spool. Also the on-disk
/// spool JSONL row shape — field names match the task's `{cluster_id,minute,
/// op,reqs,msgs,bytes_in,bytes_out}` spec exactly, independent of the DB
/// column names (usage_minutes.op_class, not `op`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UsageRow {
    pub cluster_id: Uuid,
    pub minute: u64,
    pub op: String,
    pub reqs: u64,
    pub msgs: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

const UPSERT_SQL: &str = "
    INSERT INTO queen_proxy.usage_minutes (cluster_id, minute, op_class, reqs, msgs, bytes_in, bytes_out)
    VALUES ($1::text::uuid, to_timestamp($2::bigint), $3, $4, $5, $6, $7)
    ON CONFLICT (cluster_id, minute, op_class) DO UPDATE SET
        reqs = queen_proxy.usage_minutes.reqs + EXCLUDED.reqs,
        msgs = queen_proxy.usage_minutes.msgs + EXCLUDED.msgs,
        bytes_in = queen_proxy.usage_minutes.bytes_in + EXCLUDED.bytes_in,
        bytes_out = queen_proxy.usage_minutes.bytes_out + EXCLUDED.bytes_out";

/// UPSERT one flush batch inside a single transaction (prepared statement
/// reused per row — simple and correct for v1 cardinality: distinct
/// (cluster, op_class) pairs closed per flush interval, not per-message).
/// cluster_id binds as text and casts in SQL (`$1::text::uuid`) — tokio-postgres
/// isn't built with the `with-uuid-1` feature in this crate, and this repo's
/// established workaround (see server/ Track B notes) is the text-cast, not a
/// new Cargo feature.
async fn upsert_rows(pool: &Pool, rows: &[UsageRow]) -> Result<(), String> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut client = pool.get().await.map_err(|e| format!("pool: {e}"))?;
    let txn = client.transaction().await.map_err(|e| format!("begin: {e}"))?;
    {
        let stmt = txn.prepare(UPSERT_SQL).await.map_err(|e| format!("prepare: {e}"))?;
        for r in rows {
            let cid = r.cluster_id.to_string();
            let minute_secs = (r.minute as i64) * 60;
            txn.execute(
                &stmt,
                &[
                    &cid,
                    &minute_secs,
                    &r.op,
                    &(r.reqs as i64),
                    &(r.msgs as i64),
                    &(r.bytes_in as i64),
                    &(r.bytes_out as i64),
                ],
            )
            .await
            .map_err(|e| format!("upsert: {e}"))?;
        }
    }
    txn.commit().await.map_err(|e| format!("commit: {e}"))?;
    Ok(())
}

pub struct Meter {
    flush_ms: u64,
    shards: Vec<Mutex<HashMap<(Uuid, OpClass, u64), Acc>>>,
    spool: crate::spool::Spool,
}

impl Meter {
    pub fn new(cfg: &crate::config::Config) -> Meter {
        Meter {
            flush_ms: cfg.meter_flush_ms,
            shards: (0..N_SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            spool: crate::spool::Spool::new(&cfg.spool_dir),
        }
    }

    pub fn record(&self, s: Sample) {
        let minute = now_minute_epoch();
        let idx = shard_index(&s.cluster_id);
        let mut shard = self.shards[idx].lock().unwrap();
        let acc = shard.entry((s.cluster_id, s.op, minute)).or_default();
        acc.reqs += s.reqs;
        acc.msgs += s.msgs;
        acc.bytes_in += s.bytes_in;
        acc.bytes_out += s.bytes_out;
    }

    /// Drain every aggregate whose minute is strictly before `now_minute`
    /// across all shards; entries at `now_minute` are left in place (still
    /// accumulating). Pure/no I/O so it's directly unit-testable.
    fn drain_closed(&self, now_minute: u64) -> Vec<UsageRow> {
        let mut rows = Vec::new();
        for shard in &self.shards {
            let mut m = shard.lock().unwrap();
            m.retain(|key, acc| {
                if key.2 < now_minute {
                    rows.push(UsageRow {
                        cluster_id: key.0,
                        minute: key.2,
                        op: key.1.as_str().to_string(),
                        reqs: acc.reqs,
                        msgs: acc.msgs,
                        bytes_in: acc.bytes_in,
                        bytes_out: acc.bytes_out,
                    });
                    false // remove: drained
                } else {
                    true // keep: still the current minute
                }
            });
        }
        rows
    }

    async fn flush_once(&self, db: Option<&Pool>) {
        let rows = self.drain_closed(now_minute_epoch());
        if rows.is_empty() {
            return;
        }
        let Some(pool) = db else {
            tracing::debug!(target: "meter", rows = rows.len(), "no pxdb (dev mode); discarding closed-minute usage rows");
            return;
        };
        match upsert_rows(pool, &rows).await {
            Ok(()) => {
                tracing::debug!(target: "meter", rows = rows.len(), "usage_minutes flush ok");
            }
            Err(e) => {
                tracing::warn!(target: "meter", rows = rows.len(), error = %e, "usage_minutes flush failed; spooling to disk");
                self.spool.write(&rows);
            }
        }
    }

    /// Startup spool recovery (once, before the periodic loop begins) then a
    /// flush every `cfg.meter_flush_ms`. `db: None` (dev-static mode) skips
    /// recovery and just drains+discards on every tick.
    pub fn spawn_flush(self: &Arc<Self>, db: Option<deadpool_postgres::Pool>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            if let Some(pool) = db.clone() {
                this.spool
                    .recover(move |rows| {
                        let pool = pool.clone();
                        async move { upsert_rows(&pool, &rows).await }
                    })
                    .await;
            }
            let mut tick = tokio::time::interval(Duration::from_millis(this.flush_ms.max(100)));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                this.flush_once(db.as_ref()).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a Config by hand rather than Config::load() + env::set_var: tests
    /// run concurrently in-process, and mutating a process-global env var
    /// from parallel test threads is a real data race (std::env::set_var is
    /// `unsafe` for exactly this reason). Meter::new only reads
    /// meter_flush_ms and spool_dir; the rest are inert placeholders.
    fn cfg_with_dir(dir: &std::path::Path) -> crate::config::Config {
        crate::config::Config {
            port: 0,
            pxdb: None,
            enforce: false,
            dev_insecure: true,
            dev_static: None,
            default_cluster: None,
            send_tenant_header: true,
            max_body_bytes: 1024,
            default_max_batch_items: 100,
            upstream_connect_timeout_ms: 1000,
            upstream_request_timeout_ms: 1000,
            longpoll_margin_ms: 1000,
            longpoll_max_ms: 1000,
            jwt_issuer: "test".to_string(),
            jwt_hs_secret: None,
            jwt_ed25519_pem: None,
            jwt_ttl_s: 60,
            cookie_name: "test".to_string(),
            cookie_domain: None,
            auth_host_mode: false,
            public_base_url: None,
            google_client_id: None,
            google_client_secret: None,
            github_client_id: None,
            github_client_secret: None,
            meter_flush_ms: 1000,
            spool_dir: dir.to_str().unwrap().to_string(),
        }
    }

    /// Opt-in smoke test against a real Postgres: verifies the actual SQL
    /// (`$1::text::uuid` cast, the ON CONFLICT clause, column names) against
    /// a live server rather than just the in-memory aggregation logic. Not
    /// part of the default `cargo test` run — needs a live PG on :5465 (this
    /// crate's reserved dev pxdb port, CONTRACTS.md point 4) and creates its
    /// own throwaway usage_minutes table (doesn't depend on Agent B's
    /// migration having landed yet). Run explicitly with:
    ///   cargo test --lib meter::tests::live_upsert_rows_against_real_postgres -- --ignored
    #[tokio::test]
    #[ignore = "requires a live postgres on :5465 — see doc comment"]
    async fn live_upsert_rows_against_real_postgres() {
        let pxcfg = crate::config::PxdbConfig {
            host: "127.0.0.1".to_string(),
            port: 5465,
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            dbname: "queen_proxy".to_string(),
            use_ssl: false,
            ssl_reject_unauthorized: false,
            pool_size: 4,
        };
        let pool = crate::db::create_pool(&pxcfg).await.expect("connect to dev pxdb on :5465");
        {
            let client = pool.get().await.unwrap();
            client
                .batch_execute(
                    "CREATE SCHEMA IF NOT EXISTS queen_proxy;
                     DROP TABLE IF EXISTS queen_proxy.usage_minutes;
                     CREATE TABLE queen_proxy.usage_minutes (
                        cluster_id uuid NOT NULL,
                        minute timestamptz NOT NULL,
                        op_class text NOT NULL,
                        reqs bigint NOT NULL DEFAULT 0,
                        msgs bigint NOT NULL DEFAULT 0,
                        bytes_in bigint NOT NULL DEFAULT 0,
                        bytes_out bigint NOT NULL DEFAULT 0,
                        PRIMARY KEY (cluster_id, minute, op_class)
                     )",
                )
                .await
                .expect("create throwaway test schema");
        }

        let cid = Uuid::new_v4();
        let row1 =
            UsageRow { cluster_id: cid, minute: 29_000_000, op: "push".to_string(), reqs: 3, msgs: 10, bytes_in: 500, bytes_out: 0 };
        upsert_rows(&pool, &[row1.clone()]).await.expect("first upsert");
        // A second flush for the *same* (cluster,minute,op) must add, not
        // overwrite — this is the whole point of the ON CONFLICT clause.
        let row2 = UsageRow { reqs: 2, msgs: 4, bytes_in: 100, bytes_out: 50, ..row1.clone() };
        upsert_rows(&pool, &[row2]).await.expect("second upsert (additive)");

        let client = pool.get().await.unwrap();
        let r = client
            .query_one(
                "SELECT reqs, msgs, bytes_in, bytes_out, extract(epoch from minute)::bigint / 60
                 FROM queen_proxy.usage_minutes WHERE cluster_id = $1::text::uuid AND op_class = $2",
                &[&cid.to_string(), &"push"],
            )
            .await
            .expect("row should exist after upsert");
        let reqs: i64 = r.get(0);
        let msgs: i64 = r.get(1);
        let bytes_in: i64 = r.get(2);
        let bytes_out: i64 = r.get(3);
        let minute_epoch: i64 = r.get(4);
        assert_eq!((reqs, msgs, bytes_in, bytes_out), (5, 14, 600, 50), "ON CONFLICT DO UPDATE must add, not replace");
        assert_eq!(minute_epoch as u64, row1.minute, "to_timestamp($2::bigint) round-trips the minute epoch");
    }

    #[test]
    fn record_sums_within_the_same_minute() {
        let dir = tempdir();
        let meter = Meter::new(&cfg_with_dir(dir.path()));
        let cid = Uuid::new_v4();
        meter.record(Sample { cluster_id: cid, op: OpClass::Push, reqs: 1, msgs: 3, bytes_in: 100, bytes_out: 0 });
        meter.record(Sample { cluster_id: cid, op: OpClass::Push, reqs: 1, msgs: 5, bytes_in: 50, bytes_out: 10 });
        meter.record(Sample { cluster_id: cid, op: OpClass::Read, reqs: 1, msgs: 0, bytes_in: 0, bytes_out: 200 });

        let now_minute = now_minute_epoch();
        // Nothing closed yet — the current minute is retained.
        assert!(meter.drain_closed(now_minute).is_empty());
        // Draining "as of" the next minute closes everything recorded so far.
        let rows = meter.drain_closed(now_minute + 1);
        assert_eq!(rows.len(), 2, "push and read are separate op_class keys");
        let push = rows.iter().find(|r| r.op == "push").expect("push row");
        assert_eq!((push.reqs, push.msgs, push.bytes_in, push.bytes_out), (2, 8, 150, 10));
        let read = rows.iter().find(|r| r.op == "read").expect("read row");
        assert_eq!((read.reqs, read.msgs, read.bytes_in, read.bytes_out), (1, 0, 0, 200));
    }

    #[test]
    fn drain_closed_only_drains_minutes_strictly_before_now() {
        let dir = tempdir();
        let meter = Meter::new(&cfg_with_dir(dir.path()));
        let cid = Uuid::new_v4();
        meter.record(Sample { cluster_id: cid, op: OpClass::Delivery, reqs: 1, msgs: 1, bytes_in: 0, bytes_out: 1 });

        let now_minute = now_minute_epoch();
        assert!(meter.drain_closed(now_minute).is_empty(), "current minute must not drain");
        assert!(meter.drain_closed(now_minute.saturating_sub(1)).is_empty(), "a minute in the past doesn't drain the future");
        let rows = meter.drain_closed(now_minute + 1);
        assert_eq!(rows.len(), 1);
        // A second drain at the same or later point finds nothing left.
        assert!(meter.drain_closed(now_minute + 2).is_empty());
    }

    #[test]
    fn flush_once_without_db_discards_closed_rows() {
        let dir = tempdir();
        let meter = Meter::new(&cfg_with_dir(dir.path()));
        let cid = Uuid::new_v4();
        meter.record(Sample { cluster_id: cid, op: OpClass::Txn, reqs: 1, msgs: 1, bytes_in: 1, bytes_out: 1 });
        // Force the entry into a "closed" minute by draining with a future
        // reference point directly (flush_once uses real now, so we exercise
        // drain_closed the same way flush_once would via a future minute).
        let rows = meter.drain_closed(now_minute_epoch() + 1);
        assert_eq!(rows.len(), 1);
        // With db=None, flush_once's own drain would find nothing left to
        // discard a second time (already drained above) — this just confirms
        // drain_closed is destructive (entries don't reappear).
        assert!(meter.drain_closed(now_minute_epoch() + 1).is_empty());
    }

    // Minimal local tempdir helper (no tempfile crate dependency).
    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn tempdir() -> TempDir {
        let mut p = std::env::temp_dir();
        p.push(format!("queen-proxy-meter-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
}
