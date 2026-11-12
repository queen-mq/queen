//! The MySQL source: table → queue through the binlog, in the broker.
//!
//! The same contract as the Postgres source (`crate::source`), on MySQL's
//! machinery:
//!
//! * **Reading.** The broker connects as a replica (`COM_BINLOG_DUMP_GTID`,
//!   through `mysql_async`) and asks for every transaction outside the
//!   pointer's GTID set. Column names, keys, ENUM and SET values come from
//!   the binlog's own table maps (`binlog_row_metadata=FULL`), so there is
//!   no DDL parser and no schema history to keep; rows are whole
//!   (`binlog_row_image=FULL`).
//! * **Exactly once.** One MySQL transaction is one Queen `/transaction`:
//!   its messages and the pointer with its GTID added, fenced by the
//!   pointer's version ([`crate::source::bundle`], shared with Postgres).
//!   Message ids come from the GTID and the change's place in it, so a
//!   resend is a duplicate however it happens. A transaction too big for one
//!   bundle goes in parts, each moving `inTxn`.
//! * **Failover.** A GTID is the same on every server of a replication
//!   group: after a MySQL failover the next start asks the new primary for
//!   the same set.
//! * **No slot.** MySQL keeps its binlog for a time
//!   (`binlog_expire_logs_seconds`, 30 days by default), not for a reader: a
//!   stopped source never fills the database's disk, and one stopped longer
//!   than that finds its binlog purged (`binlog_purged`: resync).
//! * **Snapshot.** DBLog chunks with read-only watermarks ([`snapshot`]):
//!   the source's user only needs `REPLICATION SLAVE, REPLICATION CLIENT,
//!   SELECT`.
//! * **One owner.** The same lease as Postgres ([`crate::source::lease`]):
//!   one node of the cluster streams, the others stand by.
//!
//! Module map: [`gtid`] (sets), [`conn`] (connections, errors), [`catalog`]
//! (server checks, tables), [`values`] (cells and JSON), [`table`] (table
//! maps), [`rows`] (row images), [`events`] (payloads, ids), [`pointer`],
//! [`snapshot`],
//! [`pipeline`] (the pure decoder), [`engine`] (the loop).

pub mod catalog;
pub mod conn;
mod engine;
pub mod events;
pub mod gtid;
pub mod pipeline;
pub mod pointer;
pub mod rows;
pub mod snapshot;
pub mod table;
pub mod values;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use mysql_async::BinlogStreamRequest;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::time::Instant;

use crate::config::{ConnectorDoc, Engine as DbEngine, Kind, SnapshotMode, SourceSpec};
use crate::connector::{Context, RunEnd};
use crate::error::{Error, Result};
use crate::metrics::ConnMetrics;
use crate::pg::connect::EgressPolicy;
use crate::source::bundle::{Backoff, Limits};
use crate::source::lease::{self, Acquired, Lease, RefreshTask};
use crate::source::pointer::{self as shared, Held, SnapshotProgress};
use crate::status::StatusCell;
use crate::stop::Stop;

use catalog::{MyTableName, ServerInfo};
use gtid::GtidSet;
use pipeline::Pipeline;
use pointer::{MyPointer, MyPosition};
use table::MyTable;

/// `QUEEN_PG_SNAPSHOT_CHUNK_MAX_BYTES` (default 64 MiB): what one chunk may
/// hold before the next is made smaller (shared with Postgres).
fn snapshot_chunk_max_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("QUEEN_PG_SNAPSHOT_CHUNK_MAX_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(64 * 1024 * 1024)
    })
}

/// The replica id a source registers with: the document's, or one derived
/// from the tenant and the name in 2^31..2^32, away from the small ids
/// people give their replicas (two readers with one id: the server drops
/// the older).
pub fn server_id(tenant: &str, name: &str, spec: &SourceSpec) -> u32 {
    if let Some(id) = spec.server_id {
        return id;
    }
    let d = Sha256::digest(format!("{tenant}/{name}").as_bytes());
    let n = u32::from_be_bytes([d[0], d[1], d[2], d[3]]);
    n | 0x8000_0000
}

/// Session settings of the connection that reads the catalog and the
/// snapshot: TIMESTAMPs in UTC, strings as stored (the binlog's bytes),
/// repeatable reads.
fn query_setup() -> Vec<String> {
    vec![
        "SET SESSION time_zone = '+00:00'".into(),
        "SET SESSION character_set_results = binary".into(),
        "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ".into(),
    ]
}

/// Session settings of the binlog connection: heartbeats every
/// `heartbeatSeconds` while idle (both names: 8.0.26 renamed the variable),
/// and an hour before the server gives up on a reader that waits for Queen.
fn binlog_setup(heartbeat_seconds: u64) -> Vec<String> {
    let ns = heartbeat_seconds.max(1).saturating_mul(1_000_000_000);
    vec![
        format!("SET @source_heartbeat_period = {ns}"),
        format!("SET @master_heartbeat_period = {ns}"),
        "SET SESSION net_write_timeout = 3600".into(),
        "SET SESSION net_read_timeout = 3600".into(),
    ]
}

pub struct MySource {
    ctx: Context,
    doc: ConnectorDoc,
    spec: SourceSpec,
    password: Option<String>,
    status: StatusCell,
    metrics: Arc<ConnMetrics>,
    server_id: u32,
    pointer_key: String,
    lease_key: String,
}

impl MySource {
    pub fn new(ctx: Context, doc: ConnectorDoc, password: Option<String>) -> Result<Arc<MySource>> {
        if doc.kind != Kind::Source || doc.engine != DbEngine::Mysql {
            return Err(Error::config("not a MySQL source document"));
        }
        let spec = doc
            .source
            .clone()
            .ok_or_else(|| Error::config("a source needs a `source` block"))?;
        if spec.tables.is_empty() {
            return Err(Error::config("source.tables is empty"));
        }
        let metrics = ctx.conn_metrics_of(Kind::Source, DbEngine::Mysql);
        let runs = doc.enabled || doc.deleting.is_some();
        let status = StatusCell::new(if runs { "connecting" } else { "disabled" });
        let server_id = server_id(&ctx.tenant, &ctx.name, &spec);
        status.set("engine", json!("mysql"));
        status.set("serverId", json!(server_id));
        Ok(Arc::new(MySource {
            pointer_key: shared::pointer_key(&ctx.name),
            lease_key: shared::lease_key(&ctx.name),
            ctx,
            doc,
            spec,
            password,
            status,
            metrics,
            server_id,
        }))
    }

    pub fn status(&self) -> serde_json::Value {
        self.status.to_json()
    }

    pub async fn run(self: Arc<Self>, stop: Stop) -> Result<RunEnd> {
        if !self.doc.enabled && self.doc.deleting.is_none() {
            self.status.set_phase("disabled");
            stop.wait().await;
            return Ok(RunEnd::Stopped);
        }
        let lease = Arc::new(Lease::new(
            self.ctx.api.clone(),
            self.lease_key.clone(),
            &self.ctx.knobs.node,
            self.ctx.knobs.lease_ttl_ms,
        ));
        let mut refresher: Option<RefreshTask> = None;
        let mut backoff = Backoff::default();
        let out = loop {
            if stop.is_stopped() {
                break Ok(RunEnd::Stopped);
            }
            let started = Instant::now();
            match self.run_once(&stop, &lease, &mut refresher).await {
                Ok(end) => break Ok(end),
                Err(Error::Stopped) => break Ok(RunEnd::Stopped),
                Err(e) => {
                    self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                    self.status.set_error(&e);
                    if started.elapsed() > Duration::from_secs(60) {
                        backoff.reset();
                    }
                    tracing::warn!(
                        target: crate::LOG_TARGET,
                        connector = %self.ctx.name,
                        code = e.code(),
                        error = %e,
                        retryable = e.is_retryable(),
                        "MySQL source stopped streaming"
                    );
                    match &e {
                        Error::Fenced(_) => {
                            refresher = None;
                            lease.release().await;
                            self.metrics.owner.store(0, Ordering::Relaxed);
                            self.status.set_phase("connecting");
                            if stop.sleep(jitter(Duration::from_secs(1))).await {
                                break Ok(RunEnd::Stopped);
                            }
                        }
                        e if e.is_retryable() => {
                            self.status.set_phase("error");
                            if stop.sleep(jitter(backoff.step())).await {
                                break Ok(RunEnd::Stopped);
                            }
                        }
                        _ => {
                            self.status.set_phase("error");
                            break Err(e);
                        }
                    }
                }
            }
        };
        drop(refresher);
        lease.release().await;
        self.metrics.owner.store(0, Ordering::Relaxed);
        match &out {
            Ok(RunEnd::Stopped) => self.status.set_phase("stopped"),
            Ok(RunEnd::TornDown) => {
                self.status.set_phase("stopped");
                self.status.set("tornDown", json!(true));
            }
            Err(_) => {}
        }
        out
    }

    /// Wait for the lease (status `standby` while another node holds it).
    async fn claim(&self, stop: &Stop, lease: &Lease) -> Result<()> {
        loop {
            match lease.acquire().await {
                Ok(Acquired::Taken) => {
                    self.status.remove("owner");
                    return Ok(());
                }
                Ok(Acquired::HeldBy(node)) => {
                    self.status.set_phase("standby");
                    self.status.set("owner", json!(node));
                    self.metrics.owner.store(0, Ordering::Relaxed);
                }
                Err(e) if e.is_retryable() => {
                    self.status.set_phase("standby");
                    self.status.set_error(&e);
                }
                Err(e) => return Err(e),
            }
            if stop.sleep(jitter(lease.claim_interval())).await {
                return Err(Error::Stopped);
            }
        }
    }

    fn policy(&self) -> EgressPolicy {
        EgressPolicy {
            allow_private: self.ctx.knobs.allow_private_networks,
        }
    }

    async fn run_once(
        &self,
        stop: &Stop,
        lease: &Arc<Lease>,
        refresher: &mut Option<RefreshTask>,
    ) -> Result<RunEnd> {
        if !lease.held() {
            *refresher = None;
            self.claim(stop, lease).await?;
            *refresher = Some(lease::spawn_refresh(lease.clone()));
        }
        self.metrics.owner.store(1, Ordering::Relaxed);
        self.status.set_phase("connecting");
        let api = self.ctx.api.clone();
        let held = shared::read_doc::<MyPointer>(api.as_ref(), &self.pointer_key).await?;
        if self.doc.deleting.is_some() {
            // Nothing lives on the database for a MySQL source: the
            // pointer goes, whatever `dropSlot` said.
            return self.teardown(held).await;
        }
        let conn = &self.doc.connection;
        let policy = self.policy();
        let mut q = conn::connect(conn, self.password.as_deref(), &policy, &query_setup()).await?;
        let info = catalog::server_info(&mut q).await?;
        catalog::check_server(&info)?;
        let status = catalog::binlog_status(&mut q, &info).await?;
        self.status.set("serverUuid", json!(info.server_uuid));
        self.status.set("serverVersion", json!(info.version));
        let tables = Arc::new(self.resolve_tables(&mut q).await?);

        let held = match (held, self.doc.resync_requested_at.clone()) {
            (Some(h), Some(at)) if h.doc.resynced_at.as_deref() < Some(at.as_str()) => Some(
                self.resync(h, &info, &status.executed, &at, &tables)
                    .await?,
            ),
            (h, _) => h,
        };
        let held = match held {
            Some(h) => h,
            None => {
                let doc = self.fresh_pointer(&info, status.executed.clone(), &tables, false);
                let version = shared::create(api.as_ref(), &self.pointer_key, &doc).await?;
                tracing::info!(
                    target: crate::LOG_TARGET,
                    connector = %self.ctx.name,
                    gtids = %doc.pos.gtids,
                    epoch = %doc.epoch,
                    "MySQL source pointer created"
                );
                Held { doc, version }
            }
        };
        let fix = format!("POST /api/v1/connectors/{}/resync", self.ctx.name);
        if !status.executed.contains_set(&held.doc.pos.gtids) {
            return Err(Error::fatal(
                "server_behind",
                format!(
                    "the server has not executed transactions the pointer covers ({}): it was \
                     restored from a backup, or it is another server; {fix} to snapshot again",
                    status.executed.missing_from(&held.doc.pos.gtids)
                ),
            ));
        }
        let purged = catalog::gtid_purged(&mut q).await?;
        if !held.doc.pos.gtids.contains_set(&purged) {
            return Err(Error::fatal(
                "binlog_purged",
                format!(
                    "the server purged binlog the source has not read ({}): it was stopped \
                     longer than binlog_expire_logs_seconds ({}); {fix} to snapshot again",
                    held.doc.pos.gtids.missing_from(&purged),
                    info.expire_seconds
                        .map(|s| format!("{s} s"))
                        .unwrap_or_else(|| "unknown".into())
                ),
            ));
        }

        let mut bc = conn::connect(
            conn,
            self.password.as_deref(),
            &policy,
            &binlog_setup(self.spec.heartbeat_seconds),
        )
        .await?;
        // The two connections must reach the same server (a proxy in front
        // could split them during a failover).
        let other = catalog::server_info(&mut bc).await?;
        if other.server_uuid != info.server_uuid {
            return Err(Error::Mysql {
                code: None,
                message: format!(
                    "the two connections reached different servers ({} and {})",
                    info.server_uuid, other.server_uuid
                ),
                transient: true,
            });
        }
        let request = BinlogStreamRequest::new(self.server_id)
            .with_gtid()
            .with_gtid_set(held.doc.pos.gtids.to_sids()?);
        let stream = bc
            .get_binlog_stream(request)
            .await
            .map_err(|e| engine::stream_error(&e, &self.ctx.name))?;
        tracing::info!(
            target: crate::LOG_TARGET,
            connector = %self.ctx.name,
            server_uuid = %info.server_uuid,
            server_id = self.server_id,
            gtids = %held.doc.pos.gtids,
            epoch = %held.doc.epoch,
            snapshot = held.doc.pos.snapshot.is_some(),
            "MySQL source streaming"
        );
        self.status.clear_error();
        let limits = Limits {
            max_messages: self.spec.max_bundle_messages.max(1) as usize,
            max_bytes: self.spec.max_bundle_bytes.max(1) as usize,
            linger: Duration::from_millis(self.spec.linger_ms),
        };
        let pipe = Pipeline::new(
            held.doc.epoch.clone(),
            tables,
            held.doc.pos.clone(),
            limits,
            self.spec.on_truncate,
        );
        engine::Engine::new(self, stop, lease, q, info, stream, pipe, held)
            .run()
            .await
    }

    async fn resolve_tables(&self, q: &mut mysql_async::Conn) -> Result<Vec<MyTable>> {
        let mut out = Vec::with_capacity(self.spec.tables.len());
        for spec in &self.spec.tables {
            let tn = MyTableName::parse(&spec.table, &self.doc.connection.database)?;
            let info = catalog::table_info(q, &tn).await?.ok_or_else(|| {
                Error::fatal(
                    "table_missing",
                    format!("the table {tn} does not exist (or is not a base table)"),
                )
            })?;
            out.push(table::resolve(spec, &info)?);
        }
        Ok(out)
    }

    /// A pointer for a new run under a new epoch, starting at `gtids`.
    fn fresh_pointer(
        &self,
        info: &ServerInfo,
        gtids: GtidSet,
        tables: &[MyTable],
        snapshot_all: bool,
    ) -> MyPointer {
        let snapshot = if snapshot_all || self.spec.snapshot == SnapshotMode::Initial {
            let mut names: Vec<String> = Vec::new();
            for t in tables {
                if !names.contains(&t.name) {
                    names.push(t.name.clone());
                }
            }
            SnapshotProgress::start(names)
        } else {
            None
        };
        MyPointer {
            epoch: shared::new_epoch(),
            server_uuid: info.server_uuid.clone(),
            pos: MyPosition {
                gtids,
                in_txn: None,
                snapshot,
            },
            resynced_at: self.doc.resync_requested_at.clone(),
            updated_at: crate::values::iso_utc_micros(crate::status::now_us()),
        }
    }

    /// A resync: a new epoch from what the server has executed now, every
    /// table read again. The pointer is replaced (fenced), never deleted
    /// first: a crash in here resyncs again on the next start.
    async fn resync(
        &self,
        held: Held<MyPointer>,
        info: &ServerInfo,
        executed: &GtidSet,
        at: &str,
        tables: &[MyTable],
    ) -> Result<Held<MyPointer>> {
        tracing::info!(
            target: crate::LOG_TARGET,
            connector = %self.ctx.name,
            requested_at = %at,
            "MySQL source resync: a new epoch from the executed set"
        );
        let mut doc = self.fresh_pointer(info, executed.clone(), tables, true);
        doc.resynced_at = Some(at.to_string());
        let version =
            shared::replace(self.ctx.api.as_ref(), &self.pointer_key, &doc, held.version).await?;
        self.status.set("resyncedAt", json!(at));
        Ok(Held { doc, version })
    }

    async fn teardown(&self, held: Option<Held<MyPointer>>) -> Result<RunEnd> {
        if let Some(h) = held {
            shared::delete(self.ctx.api.as_ref(), &self.pointer_key, h.version).await?;
        }
        tracing::info!(
            target: crate::LOG_TARGET,
            connector = %self.ctx.name,
            "MySQL source torn down"
        );
        Ok(RunEnd::TornDown)
    }
}

/// ±20 %, so nodes that failed together do not retry together.
fn jitter(d: Duration) -> Duration {
    d.mul_f64(0.8 + rand::random::<f64>() * 0.4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_ids_are_stable_high_and_overridable() {
        let spec = |id: Option<u32>| SourceSpec {
            tables: vec![],
            slot: None,
            publication: None,
            manage_publication: true,
            server_id: id,
            snapshot: SnapshotMode::Initial,
            snapshot_chunk_rows: 2000,
            max_bundle_messages: 1000,
            max_bundle_bytes: 1 << 22,
            linger_ms: 20,
            heartbeat_seconds: 10,
            on_truncate: crate::config::TruncatePolicy::Skip,
        };
        let a = server_id("t1", "shop", &spec(None));
        assert_eq!(a, server_id("t1", "shop", &spec(None)));
        assert_ne!(a, server_id("t2", "shop", &spec(None)));
        assert!(a >= 0x8000_0000);
        assert_eq!(server_id("t1", "shop", &spec(Some(77))), 77);
        assert_eq!(
            binlog_setup(10)[0],
            "SET @source_heartbeat_period = 10000000000"
        );
    }
}
