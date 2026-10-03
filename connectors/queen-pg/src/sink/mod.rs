//! The sink: queue → table through a consumer group (PLAN §5). OWNER: agent K.
//!
//! A sink is a consumer group the broker runs for you. Every node of a
//! cluster runs `workers` workers for it, each with its own PostgreSQL
//! connection; Queen's partition leases spread the partitions over all of
//! them, and a node that dies leaves its leases to expire into the others.
//! Nothing coordinates the nodes but those leases and the progress table.
//!
//! Exactly-once effects come from one rule (PLAN §0): the progress marker —
//! the highest applied offset of each partition — commits in the SAME
//! PostgreSQL transaction as the rows (`progress.rs`). Pop, apply and ack are
//! otherwise at-least-once, and the marker turns every repeat into a no-op.
//!
//! * `params.rs` — sql mode's parameter paths;
//! * `apply.rs` — the four modes: what a message writes, the statements;
//! * `progress.rs` — the progress table, the batch's order and filter;
//! * `engine.rs` — the workers: pop → transaction → ack, failures, leases.

mod apply;
mod engine;
mod params;
mod progress;

use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::task::JoinSet;

use crate::config::{ConnectionSpec, ConnectorDoc, Kind, SinkMode, SinkSpec};
use crate::connector::{Context, RunEnd};
use crate::error::{Error, Result};
use crate::metrics::ConnMetrics;
use crate::pg::catalog::TableName;
use crate::queen::Popped;
use crate::status::{now_us, StatusCell};
use crate::stop::{stop_pair, Stop};

pub use engine::POP_WAIT_MS;
pub use params::{Meta, Param};

/// The `application_name` of a sink's connections:
/// `queen-pg/<version> sink:<connector>:<worker>` (`setup` for the one that
/// checks the tables before the workers start), so an operator finds them in
/// `pg_stat_activity`. Cut to the server's 63 bytes.
pub fn application_name(connector: &str, who: &str) -> String {
    let mut s = format!("{} sink:{connector}:{who}", crate::application_name());
    if s.len() > 63 {
        let mut end = 63;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
    }
    s
}

/// What the workers of one sink share.
pub(crate) struct Shared {
    pub ctx: Context,
    pub spec: SinkSpec,
    pub connection: ConnectionSpec,
    pub password: Option<String>,
    pub enabled: bool,
    pub group: String,
    /// The progress table's `sink` column: `<tenant>/<connector name>`.
    pub sink_key: String,
    pub table: TableName,
    pub progress: TableName,
    /// sql mode.
    pub statement: Option<String>,
    pub params: Vec<Param>,
    pub status: StatusCell,
    pub metrics: Arc<ConnMetrics>,
    applied: AtomicU64,
    skipped: AtomicU64,
    dlq: AtomicU64,
    batches: AtomicU64,
    last_applied_us: AtomicI64,
    /// Workers whose last step failed: the phase is `error` while any is.
    pub failing: AtomicUsize,
}

impl Shared {
    /// Count a settled batch.
    fn account(&self, disp: &[engine::Disp]) {
        let (mut applied, mut skipped, mut dead) = (0u64, 0u64, 0u64);
        for d in disp {
            match d {
                engine::Disp::Applied => applied += 1,
                engine::Disp::Skipped => skipped += 1,
                engine::Disp::Dead(_) => dead += 1,
                engine::Disp::Pending => {}
            }
        }
        self.applied.fetch_add(applied, Ordering::Relaxed);
        self.skipped.fetch_add(skipped, Ordering::Relaxed);
        self.dlq.fetch_add(dead, Ordering::Relaxed);
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.metrics.applied.fetch_add(applied, Ordering::Relaxed);
        self.metrics.skipped.fetch_add(skipped, Ordering::Relaxed);
        self.metrics.dlq.fetch_add(dead, Ordering::Relaxed);
        self.metrics.batches.fetch_add(1, Ordering::Relaxed);
        if applied > 0 {
            self.last_applied_us.store(now_us(), Ordering::Relaxed);
        }
    }

    /// The current failure (`error`, cleared when the workers recover) and
    /// the last one ever (`lastError`, kept).
    fn record_error(&self, e: &Error) {
        self.status.set_error(e);
        self.status.set(
            "lastError",
            json!({"code": e.code(), "message": e.to_string(), "atUs": now_us()}),
        );
        self.metrics.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_dead_letter(&self, m: &Popped, text: &str) {
        self.status.set(
            "lastError",
            json!({
                "code": "dlq",
                "message": format!("partition {} offset {}: {text}", m.partition, m.offset),
                "atUs": now_us(),
            }),
        );
    }
}

/// A configured sink.
pub struct Sink {
    sh: Arc<Shared>,
}

impl Sink {
    /// Build the sink of `doc` (already validated by the caller,
    /// [`ConnectorDoc::validate`]); nothing is contacted until
    /// [`Sink::run`]. `password` is the unsealed password.
    pub fn new(ctx: Context, doc: ConnectorDoc, password: Option<String>) -> Result<Arc<Sink>> {
        if doc.kind != Kind::Sink {
            return Err(Error::config("not a sink document"));
        }
        let spec = doc
            .sink
            .clone()
            .ok_or_else(|| Error::config("a sink needs its `sink` section"))?;
        let table = TableName::parse(&spec.table)?;
        let progress = TableName::parse(&spec.progress_table)?;
        let (statement, params) = match spec.mode {
            SinkMode::Sql => {
                let st = spec
                    .statement
                    .clone()
                    .filter(|s| !s.trim().is_empty())
                    .ok_or_else(|| Error::config("sink.mode sql needs sink.statement"))?;
                let params = spec
                    .params
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|p| Param::parse(p).map_err(Error::config))
                    .collect::<Result<Vec<_>>>()?;
                (Some(st), params)
            }
            _ => (None, Vec::new()),
        };
        let group = doc.consumer_group(&ctx.name);
        let status = StatusCell::new(if doc.enabled { "stopped" } else { "disabled" });
        status.set("queue", Value::String(spec.queue.clone()));
        status.set("group", Value::String(group.clone()));
        status.set("table", Value::String(table.to_string()));
        status.set("mode", json!(spec.mode));
        status.set("workers", json!(spec.workers.max(1)));
        let metrics = ctx.conn_metrics(Kind::Sink);
        let sink_key = format!("{}/{}", ctx.tenant, ctx.name);
        Ok(Arc::new(Sink {
            sh: Arc::new(Shared {
                connection: doc.connection.clone(),
                enabled: doc.enabled,
                ctx,
                spec,
                password,
                group,
                sink_key,
                table,
                progress,
                statement,
                params,
                status,
                metrics,
                applied: AtomicU64::new(0),
                skipped: AtomicU64::new(0),
                dlq: AtomicU64::new(0),
                batches: AtomicU64::new(0),
                last_applied_us: AtomicI64::new(0),
                failing: AtomicUsize::new(0),
            }),
        }))
    }

    /// Run until `stop`: the checks of [`engine::setup`] (retried while the
    /// database is unreachable), then `workers` workers. On stop each worker
    /// stops popping, finishes the batch in flight within its lease and
    /// acks it; what it leaves unacked comes back through the broker's
    /// redelivery and is applied once. `Err` for what only an operator can
    /// fix (the status names it); the broker restarts the sink after its own
    /// back-off.
    pub async fn run(self: Arc<Self>, stop: Stop) -> Result<RunEnd> {
        let sh = &self.sh;
        if !sh.enabled {
            sh.status.set_phase("disabled");
            stop.wait().await;
            return Ok(RunEnd::Stopped);
        }
        sh.status.set_phase("running");
        sh.status.clear_error();
        sh.metrics.owner.store(1, Ordering::Relaxed);
        let r = self.run_inner(stop).await;
        sh.metrics.owner.store(0, Ordering::Relaxed);
        sh.failing.store(0, Ordering::SeqCst);
        match r {
            Ok(()) => {
                sh.status.set_phase("stopped");
                Ok(RunEnd::Stopped)
            }
            Err(e) => {
                tracing::error!(target: "queen-pg", connector = %sh.ctx.name, error = %e, "sink stopped");
                sh.record_error(&e);
                sh.status.set_phase("error");
                Err(e)
            }
        }
    }

    async fn run_inner(&self, stop: Stop) -> Result<()> {
        let sh = &self.sh;
        let mut backoff = engine::Backoff::new();
        loop {
            if stop.is_stopped() {
                return Ok(());
            }
            match engine::setup(sh).await {
                Ok(()) => break,
                Err(e) if !e.is_retryable() => return Err(e),
                Err(e) => {
                    tracing::warn!(target: "queen-pg", connector = %sh.ctx.name, error = %e, "sink setup failed; retrying");
                    sh.record_error(&e);
                    sh.status.set_phase("error");
                    if stop.sleep(backoff.next_delay()).await {
                        return Ok(());
                    }
                }
            }
        }
        sh.status.clear_error();
        sh.status.set_phase("running");
        tracing::info!(target: "queen-pg", connector = %sh.ctx.name, queue = %sh.spec.queue, table = %sh.table, workers = sh.spec.workers, "sink running");

        // The workers watch their own stop: the caller's, or the first
        // worker that cannot go on (Fatal) stopping the others.
        let (handle, inner) = stop_pair();
        let mut set = JoinSet::new();
        for id in 0..sh.spec.workers.max(1) as usize {
            set.spawn(engine::Worker::new(Arc::clone(sh), id, inner.clone()).run());
        }
        let mut first: Option<Error> = None;
        loop {
            tokio::select! {
                _ = stop.wait(), if !handle.is_stopped() => handle.stop(),
                r = set.join_next() => match r {
                    None => break,
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(e))) => {
                        first.get_or_insert(e);
                        handle.stop();
                    }
                    Some(Err(j)) => {
                        first.get_or_insert(Error::io(format!("a sink worker panicked: {j}")));
                        handle.stop();
                    }
                },
            }
        }
        match first {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// The status block (PLAN §5.5): phase, error, queue, group, table,
    /// mode, workers, the counters, lastAppliedAt, lastError.
    pub fn status(&self) -> Value {
        let sh = &self.sh;
        let mut v = sh.status.to_json();
        if let Value::Object(m) = &mut v {
            m.insert("applied".into(), json!(sh.applied.load(Ordering::Relaxed)));
            m.insert("skipped".into(), json!(sh.skipped.load(Ordering::Relaxed)));
            m.insert("dlq".into(), json!(sh.dlq.load(Ordering::Relaxed)));
            m.insert("batches".into(), json!(sh.batches.load(Ordering::Relaxed)));
            let at = sh.last_applied_us.load(Ordering::Relaxed);
            m.insert(
                "lastAppliedAt".into(),
                if at > 0 {
                    Value::String(crate::values::iso_utc_micros(at))
                } else {
                    Value::Null
                },
            );
            // `lastError.atUs` → `at`, rendered here (the cell keeps the
            // number so recording an error never formats a time).
            if let Some(Value::Object(le)) = m.get_mut("lastError") {
                if let Some(us) = le.remove("atUs").and_then(|v| v.as_i64()) {
                    le.insert(
                        "at".into(),
                        Value::String(crate::values::iso_utc_micros(us)),
                    );
                }
            }
        }
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_names_identify_the_sink_and_fit_the_server() {
        let n = application_name("orders-copy", "3");
        assert!(n.starts_with("queen-pg/"), "{n}");
        assert!(n.ends_with(" sink:orders-copy:3"), "{n}");
        let long = application_name(&"x".repeat(48), "setup");
        assert_eq!(long.len(), 63);
    }
}
