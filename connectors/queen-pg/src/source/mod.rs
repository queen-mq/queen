//! The source: table → queue through logical replication (PLAN §4). OWNER: agent S.
//!
//! [`Source::run`] is the whole life of one source on one node: claim the
//! lease (or wait as `standby`), start (PLAN §4.3: pointer, server checks,
//! publication, slot, `IDENTIFY_SYSTEM`, `START_REPLICATION` at the
//! pointer), stream ([`engine`]), and on the way out confirm what committed
//! and give the lease back.
//!
//! Errors decide what happens next ([`Error::is_retryable`]): a retryable
//! one backs off (1 s doubling to 30 s) and starts again from the pointer,
//! keeping the lease; [`Error::Fenced`] gives the lease up and starts from
//! the top; anything else ends `run` with an error status naming the fix,
//! and the broker's supervisor restarts the unit slowly.
//!
//! Module map: [`pointer`] (the exactly-once marker), [`lease`], [`table`]
//! (a configured table against the catalog), [`events`] (payloads,
//! partitions, ids), [`types`] (domains and user arrays), [`pipeline`] (the
//! pure decoder: pgoutput in, units out), [`bundle`] (units into
//! `/transaction` calls, and the answer decision table), [`snapshot`] (DBLog
//! chunks), [`slot`] (the database side of a start), [`engine`] (the loop).

pub mod bundle;
mod engine;
pub mod events;
pub mod lease;
pub mod pipeline;
pub mod pointer;
pub mod slot;
pub mod snapshot;
pub mod table;
pub mod types;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::time::Instant;
use tokio_postgres::Client;

use crate::config::{ConnectorDoc, Kind, SnapshotMode, SourceSpec};
use crate::connector::{Context, RunEnd};
use crate::error::{Error, Result};
use crate::metrics::ConnMetrics;
use crate::pg::catalog::{self, TableInfo, TableName};
use crate::pg::connect::{self, EgressPolicy};
use crate::repl::{Lsn, ReplicationClient, SystemIdentity};
use crate::status::StatusCell;
use crate::stop::Stop;

use bundle::{Backoff, Limits};
use events::TypeConv;
use lease::{Acquired, Lease, RefreshTask};
use pipeline::Pipeline;
use pointer::{Held, Pointer, SnapshotProgress, POINTER_FORMAT};
use table::TableState;

/// How often a source waiting for its slot looks again.
const SLOT_WAIT: Duration = Duration::from_secs(2);

pub struct Source {
    ctx: Context,
    doc: ConnectorDoc,
    spec: SourceSpec,
    password: Option<String>,
    status: StatusCell,
    metrics: Arc<ConnMetrics>,
    slot: String,
    publication: String,
    pointer_key: String,
    lease_key: String,
    app_name: String,
}

impl Source {
    pub fn new(ctx: Context, doc: ConnectorDoc, password: Option<String>) -> Result<Arc<Source>> {
        if doc.kind != Kind::Source {
            return Err(Error::config("not a source document"));
        }
        let spec = doc
            .source
            .clone()
            .ok_or_else(|| Error::config("a source needs a `source` block"))?;
        if spec.tables.is_empty() {
            return Err(Error::config("source.tables is empty"));
        }
        let slot = doc.slot_name(&ctx.name);
        let publication = doc.publication_name(&ctx.name);
        let metrics = ctx.conn_metrics(Kind::Source);
        let runs = doc.enabled || doc.deleting.is_some();
        let status = StatusCell::new(if runs { "connecting" } else { "disabled" });
        status.set("slot", json!(slot));
        status.set("publication", json!(publication));
        // application_name is cut at 63 bytes by the server.
        let mut app_name = format!("{} {}", crate::application_name(), ctx.name);
        app_name.truncate(63);
        Ok(Arc::new(Source {
            pointer_key: pointer::pointer_key(&ctx.name),
            lease_key: pointer::lease_key(&ctx.name),
            ctx,
            doc,
            spec,
            password,
            status,
            metrics,
            slot,
            publication,
            app_name,
        }))
    }

    pub async fn run(self: Arc<Self>, stop: Stop) -> Result<RunEnd> {
        // A delete request wins over `enabled: false`: a disabled source that
        // is being deleted with `dropSlot` must still drop its slot (which
        // otherwise pins WAL forever) and let the document go.
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
                        "source stopped streaming"
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
                            // 55006: START_REPLICATION met a slot another
                            // walsender still holds (a consumer that grabbed
                            // it after the check, or an earlier life's
                            // walsender not gone yet): the same wait as an
                            // active slot at the check.
                            let busy = matches!(
                                e,
                                Error::Pg { sqlstate: Some(s), .. } if s == "55006"
                            );
                            self.status
                                .set_phase(if busy { "waiting_for_slot" } else { "error" });
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

    pub fn status(&self) -> serde_json::Value {
        self.status.to_json()
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
        let held = pointer::read(api.as_ref(), &self.pointer_key).await?;
        let policy = EgressPolicy {
            allow_private: self.ctx.knobs.allow_private_networks,
        };
        let conn = &self.doc.connection;
        let pg = connect::connect(conn, self.password.as_deref(), &policy, &self.app_name).await?;
        if let Some(d) = &self.doc.deleting {
            return self.teardown(&pg, held, d.drop_slot).await;
        }
        let info = catalog::server_info(&pg).await?;
        slot::check_server(info.version_num, &info.version, &info.wal_level)?;
        let names = self
            .spec
            .tables
            .iter()
            .map(|t| TableName::parse(&t.table))
            .collect::<Result<Vec<_>>>()?;
        // Every table is checked BEFORE the publication is touched: a table
        // without a usable replica identity in a publication makes the
        // application's own UPDATEs and DELETEs on it fail.
        let mut types = HashMap::new();
        let (infos, _) = self.resolve_tables(&pg, &names, None, &mut types).await?;
        let whole: Vec<(TableName, Vec<String>)> = names
            .iter()
            .zip(&infos)
            .map(|(n, i)| {
                let cols = i.columns.iter().filter(|c| !c.generated);
                (n.clone(), cols.map(|c| c.name.clone()).collect())
            })
            .collect();
        slot::ensure_publication(&pg, &self.publication, &whole, self.spec.manage_publication)
            .await?;
        let (_, tables) = self
            .resolve_tables(&pg, &names, Some(infos), &mut types)
            .await?;
        let tables = Arc::new(tables);

        let mut repl =
            ReplicationClient::connect(conn, self.password.as_deref(), &policy, &self.app_name)
                .await?;
        let ident = repl.identify_system().await?;
        let held = match (held, self.resync_due()) {
            (Some(h), Some(at)) if h.doc.resynced_at.as_deref() < Some(at.as_str()) => {
                Some(self.resync(&pg, h, &ident, &at, &tables).await?)
            }
            (h, _) => h,
        };
        let held = self
            .ensure_slot(&pg, stop, lease, held, &ident, &tables)
            .await?;
        if held.doc.system_id != ident.system_id {
            return Err(Error::fatal(
                "system_changed",
                format!(
                    "the database system changed (pointer {}, server {}): restored from a backup \
                     or a different cluster; POST /api/v1/connectors/{}/resync to snapshot again",
                    held.doc.system_id, ident.system_id, self.ctx.name
                ),
            ));
        }
        self.status.set("systemId", json!(ident.system_id));
        let stream = repl
            .start_logical(&self.slot, held.doc.lsn, &self.publication)
            .await?;
        tracing::info!(
            target: crate::LOG_TARGET,
            connector = %self.ctx.name,
            slot = %self.slot,
            lsn = %held.doc.lsn,
            epoch = %held.doc.epoch,
            snapshot = held.doc.snapshot.is_some(),
            "source streaming"
        );
        let limits = Limits {
            max_messages: self.spec.max_bundle_messages.max(1) as usize,
            max_bytes: self.spec.max_bundle_bytes.max(1) as usize,
            linger: Duration::from_millis(self.spec.linger_ms),
        };
        let pipe = Pipeline::new(
            held.doc.epoch.clone(),
            tables,
            held.doc.position(),
            limits,
            self.spec.on_truncate,
            rand::random::<u32>() as u64,
        );
        engine::Engine::new(self, stop, lease, pg, stream, pipe, types, held)
            .run()
            .await
    }

    /// Each configured table against the catalog (types resolved first, so
    /// a domain column renders as its base type in the snapshot too). With
    /// `infos` (the catalog read of the first pass) the publication exists
    /// and its column lists narrow the columns; without, the tables are only
    /// checked (identity, key, partitionBy) and read.
    async fn resolve_tables(
        &self,
        pg: &Client,
        names: &[TableName],
        infos: Option<Vec<TableInfo>>,
        types: &mut HashMap<u32, TypeConv>,
    ) -> Result<(Vec<TableInfo>, Vec<TableState>)> {
        let published_pass = infos.is_some();
        let infos = match infos {
            Some(i) => i,
            None => {
                let mut v = Vec::with_capacity(names.len());
                for name in names {
                    v.push(catalog::table_info(pg, name).await?.ok_or_else(|| {
                        Error::fatal(
                            "table_missing",
                            format!("the table {name} does not exist (or is not a table)"),
                        )
                    })?);
                }
                v
            }
        };
        let mut out = Vec::with_capacity(names.len());
        for ((spec, name), info) in self.spec.tables.iter().zip(names).zip(&infos) {
            let published = if published_pass {
                slot::published_columns(pg, &self.publication, name).await?
            } else {
                None
            };
            let oids: Vec<u32> = info.columns.iter().map(|c| c.type_oid).collect();
            types::resolve(pg, &oids, types).await?;
            out.push(table::resolve(spec, info, published.as_deref(), types)?);
        }
        Ok((infos, out))
    }

    fn resync_due(&self) -> Option<String> {
        self.doc.resync_requested_at.clone()
    }

    /// A pointer for a new slot (or an adopted one) under a new epoch.
    fn fresh_pointer(
        &self,
        ident: &SystemIdentity,
        lsn: Lsn,
        tables: &[TableState],
        snapshot_all: bool,
    ) -> Pointer {
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
        Pointer {
            v: POINTER_FORMAT,
            epoch: pointer::new_epoch(),
            system_id: ident.system_id.clone(),
            slot: self.slot.clone(),
            lsn,
            in_txn: None,
            snapshot,
            resynced_at: self.doc.resync_requested_at.clone(),
            updated_at: crate::values::iso_utc_micros(crate::status::now_us()),
        }
    }

    /// PLAN §4.3 step 5.
    async fn ensure_slot(
        &self,
        pg: &Client,
        stop: &Stop,
        lease: &Lease,
        held: Option<Held>,
        ident: &SystemIdentity,
        tables: &[TableState],
    ) -> Result<Held> {
        let api = self.ctx.api.as_ref();
        let db = slot::current_database(pg).await?;
        let row = loop {
            if lease.lost() {
                return Err(Error::Fenced("the source lease was lost".into()));
            }
            let row = slot::read_slot(pg, &self.slot).await?;
            match &row {
                Some(r) if r.active_pid.is_some() => {
                    self.status.set_phase("waiting_for_slot");
                    self.status.set("activePid", json!(r.active_pid));
                    if stop.sleep(SLOT_WAIT).await {
                        return Err(Error::Stopped);
                    }
                }
                _ => break row,
            }
        };
        self.status.remove("activePid");
        self.status.set_phase("connecting");
        let fix = format!("POST /api/v1/connectors/{}/resync", self.ctx.name);
        match (row, held) {
            (None, None) => {
                let lsn = slot::create_slot(pg, &self.slot).await?;
                let doc = self.fresh_pointer(ident, lsn, tables, false);
                let version = pointer::create(api, &self.pointer_key, &doc).await?;
                tracing::info!(
                    target: crate::LOG_TARGET,
                    connector = %self.ctx.name,
                    slot = %self.slot,
                    lsn = %lsn,
                    epoch = %doc.epoch,
                    "replication slot created"
                );
                Ok(Held { doc, version })
            }
            (None, Some(_)) => Err(Error::fatal(
                "slot_lost",
                format!(
                    "the replication slot {} is gone — dropped, or lost in a failover without \
                     failover slots; changes since the pointer cannot be read: {fix} to snapshot \
                     again",
                    self.slot
                ),
            )),
            (Some(r), held) => {
                if r.slot_type != "logical"
                    || r.plugin.as_deref() != Some("pgoutput")
                    || r.database.as_deref() != Some(db.as_str())
                {
                    return Err(Error::fatal(
                        "slot_mismatch",
                        format!(
                            "a slot named {} exists but is not a pgoutput slot of database {db} \
                             ({} {:?} {:?}); name another slot or drop it",
                            self.slot, r.slot_type, r.plugin, r.database
                        ),
                    ));
                }
                if r.invalidation_reason.is_some() || r.wal_status.as_deref() == Some("lost") {
                    return Err(Error::fatal(
                        "slot_invalidated",
                        format!(
                            "the replication slot {} was invalidated ({}): the WAL it needed is \
                             gone; {fix} to snapshot again (and raise max_slot_wal_keep_size)",
                            self.slot,
                            r.invalidation_reason
                                .as_deref()
                                .or(r.wal_status.as_deref())
                                .unwrap_or("lost")
                        ),
                    ));
                }
                match held {
                    None => {
                        // A slot with no pointer: created by an earlier life
                        // that died before writing it. Nothing was consumed
                        // from it, so its confirmed position is the start.
                        let Some(lsn) = r.confirmed_flush else {
                            return Err(Error::fatal(
                                "slot_invalidated",
                                format!("the slot {} has no confirmed position; {fix}", self.slot),
                            ));
                        };
                        let doc = self.fresh_pointer(ident, lsn, tables, false);
                        let version = pointer::create(api, &self.pointer_key, &doc).await?;
                        tracing::info!(
                            target: crate::LOG_TARGET,
                            connector = %self.ctx.name,
                            slot = %self.slot,
                            lsn = %lsn,
                            "replication slot adopted"
                        );
                        Ok(Held { doc, version })
                    }
                    Some(h) => {
                        if h.doc.slot != self.slot {
                            return Err(Error::fatal(
                                "slot_changed",
                                format!(
                                    "the pointer belongs to slot {:?}, the document names {:?}; \
                                     {fix} to start over on the new slot",
                                    h.doc.slot, self.slot
                                ),
                            ));
                        }
                        if r.confirmed_flush.is_some_and(|c| c > h.doc.lsn) {
                            // A pointer read before another owner's last
                            // commit would look like this too: read again.
                            let again = pointer::read(api, &self.pointer_key).await?;
                            if again.as_ref().map(|a| a.version) != Some(h.version) {
                                return Err(Error::Fenced(
                                    "the pointer moved during the start".into(),
                                ));
                            }
                            return Err(Error::fatal(
                                "slot_ahead",
                                format!(
                                    "the slot {} was consumed past the pointer ({} > {}): \
                                     another client read it, and the changes between are not \
                                     in Queen; {fix} to snapshot again",
                                    self.slot,
                                    r.confirmed_flush.unwrap_or_default(),
                                    h.doc.lsn
                                ),
                            ));
                        }
                        Ok(h)
                    }
                }
            }
        }
    }

    /// Carry out a resync request: a new slot, a new epoch, every table
    /// snapshotted again. The pointer is REPLACED (fenced), never deleted
    /// first, so a crash anywhere in here just resyncs again on the next
    /// start (the request is still newer than the pointer's `resyncedAt`).
    async fn resync(
        &self,
        pg: &Client,
        held: Held,
        ident: &SystemIdentity,
        at: &str,
        tables: &[TableState],
    ) -> Result<Held> {
        tracing::info!(
            target: crate::LOG_TARGET,
            connector = %self.ctx.name,
            requested_at = %at,
            "resync: dropping and recreating the slot"
        );
        slot::drop_slot(pg, &self.slot).await?;
        let lsn = slot::create_slot(pg, &self.slot).await?;
        let mut doc = self.fresh_pointer(ident, lsn, tables, true);
        doc.resynced_at = Some(at.to_string());
        let version =
            pointer::replace(self.ctx.api.as_ref(), &self.pointer_key, &doc, held.version).await?;
        self.status.set("resyncedAt", json!(at));
        Ok(Held { doc, version })
    }

    /// `DELETE …?dropSlot=true`: drop the slot and a managed publication,
    /// delete the pointer (the lease goes with [`Lease::release`]).
    async fn teardown(&self, pg: &Client, held: Option<Held>, drop_slot: bool) -> Result<RunEnd> {
        if drop_slot {
            slot::drop_slot(pg, &self.slot).await?;
            if self.spec.manage_publication {
                slot::drop_publication(pg, &self.publication).await?;
            }
        }
        if let Some(h) = held {
            pointer::delete(self.ctx.api.as_ref(), &self.pointer_key, h.version).await?;
        }
        tracing::info!(
            target: crate::LOG_TARGET,
            connector = %self.ctx.name,
            drop_slot,
            "source torn down"
        );
        Ok(RunEnd::TornDown)
    }
}

/// ±20 %, so nodes that failed together do not retry together.
fn jitter(d: Duration) -> Duration {
    d.mul_f64(0.8 + rand::random::<f64>() * 0.4)
}
