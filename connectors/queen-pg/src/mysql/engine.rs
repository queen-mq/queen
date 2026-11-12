//! The MySQL source's loop: the binlog stream, the bundle in flight, the
//! snapshot's chunk reads and the timers, in one task. The same shape as the
//! Postgres engine (`crate::source::engine`): events are decoded in binlog
//! order into the [`Pipeline`]; ONE bundle is in flight at a time; the next
//! is planned against the version the previous one returned. There is no
//! flush to confirm: MySQL keeps no state for its readers, the pointer in
//! Queen is the whole truth.

use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::{FutureExt, StreamExt};
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStream, Conn, Row};
use mysql_common::binlog::events::{Event, EventData, RowsEventData};
use serde_json::{json, Value};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::connector::RunEnd;
use crate::error::{Error, Result};
use crate::source::bundle::{Bundle, Commit};
use crate::source::lease::Lease;
use crate::source::pointer::Held;
use crate::stop::Stop;

use super::catalog::{binlog_status, ServerInfo};
use super::conn::classify;
use super::gtid::Gtid;
use super::pipeline::{Pipeline, RowsKind};
use super::pointer::MyPointer;
use super::snapshot::{chunk_params, chunk_sql, Chunk};
use super::values::Cell;
use super::MySource;

/// Idle advancement at most this often.
const IDLE_EVERY: Duration = Duration::from_secs(10);
/// How long an error exit waits for the bundle in flight to settle.
const ERROR_SETTLE: Duration = Duration::from_secs(5);
/// Rows of the first chunk of a table, before its width is known.
const FIRST_CHUNK_ROWS: usize = 256;
/// The largest chunk a document may ask for, whatever it says.
const CHUNK_ROWS_CEILING: usize = 10_000;

#[derive(Debug, Clone, Copy)]
struct Stats {
    txns: u64,
    messages: u64,
    snapshot_rows: u64,
    last_commit_us: Option<i64>,
}

struct InFlight {
    handle: JoinHandle<Result<i64>>,
    cover: MyPointer,
    stats: Stats,
}

async fn settle(
    f: &mut Option<InFlight>,
) -> std::result::Result<Result<i64>, tokio::task::JoinError> {
    match f {
        Some(x) => (&mut x.handle).await,
        None => std::future::pending().await,
    }
}

/// A binlog stream error, classified: a purge the pointer cannot survive is
/// the operator's (resync); the rest are retried.
pub fn stream_error(e: &mysql_async::Error, name: &str) -> Error {
    if let mysql_async::Error::Server(s) = e {
        let m = s.message.to_ascii_lowercase();
        if s.code == 1236 && m.contains("purged") {
            return Error::fatal(
                "binlog_purged",
                format!(
                    "{}: the server purged binlog the source has not read (it was stopped \
                     longer than binlog_expire_logs_seconds); POST \
                     /api/v1/connectors/{name}/resync to snapshot again",
                    s.message
                ),
            );
        }
        if s.code == 1236 {
            // Another replica with this server id, a binlog file gone
            // during a failover: the next start sorts it out.
            return Error::Mysql {
                code: Some(s.code),
                message: s.message.clone(),
                transient: true,
            };
        }
        if matches!(s.code, 1227 | 1044 | 1142) {
            return Error::fatal(
                "privileges",
                format!(
                    "{}: the source's user needs REPLICATION SLAVE, REPLICATION CLIENT and \
                     SELECT",
                    s.message
                ),
            );
        }
    }
    classify(e)
}

pub(super) struct Engine<'a> {
    src: &'a MySource,
    stop: &'a Stop,
    lease: &'a Lease,
    q: Conn,
    info: ServerInfo,
    stream: BinlogStream,
    pipe: Pipeline,
    held: Held<MyPointer>,
    in_flight: Option<InFlight>,
    last_event_at: Instant,
    last_idle_at: Instant,
    /// The stream read to the end of the binlog (a heartbeat came) and
    /// nothing has arrived since.
    caught_up: bool,
    last_commit_us: Option<i64>,
    chunk_rows: usize,
    chunk_rows_max: usize,
    silence: Duration,
}

impl<'a> Engine<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        src: &'a MySource,
        stop: &'a Stop,
        lease: &'a Lease,
        q: Conn,
        info: ServerInfo,
        stream: BinlogStream,
        pipe: Pipeline,
        held: Held<MyPointer>,
    ) -> Engine<'a> {
        let now = Instant::now();
        let hb = src.spec.heartbeat_seconds.max(1);
        Engine {
            src,
            stop,
            lease,
            q,
            info,
            stream,
            pipe,
            held,
            in_flight: None,
            last_event_at: now,
            last_idle_at: now,
            caught_up: false,
            last_commit_us: None,
            chunk_rows: (src.spec.snapshot_chunk_rows.max(1) as usize).min(FIRST_CHUNK_ROWS),
            chunk_rows_max: (src.spec.snapshot_chunk_rows.max(1) as usize).min(CHUNK_ROWS_CEILING),
            // Three heartbeats and a margin without a single event: the
            // connection is gone, whatever TCP says.
            silence: Duration::from_secs(hb * 3 + 10),
        }
    }

    pub(super) async fn run(mut self) -> Result<RunEnd> {
        let r = self.run_loop().await;
        self.finish(r).await
    }

    async fn run_loop(&mut self) -> Result<()> {
        self.phase();
        self.publish();
        loop {
            if self.stop.is_stopped() {
                return Ok(());
            }
            if self.lease.lost() {
                return Err(Error::Fenced(
                    "the source lease was taken over by another node".into(),
                ));
            }
            let now = Instant::now();

            // 1. A bundle, when one should go ("nothing buffered" is asked of
            //    the stream itself: an event ready now is read first).
            if self.in_flight.is_none() {
                if let Some(b) = self.pipe.take(now, false) {
                    self.send(b);
                } else if self.pipe.bundler.has_items() {
                    match self.stream.next().now_or_never() {
                        Some(ev) => {
                            self.on_stream(ev).await?;
                            continue;
                        }
                        None => {
                            if let Some(b) = self.pipe.take(now, true) {
                                self.send(b);
                            }
                        }
                    }
                }
            }

            // 2. A snapshot chunk, one at a time, not while a bundle's worth
            //    of events waits.
            if self.pipe.snap.is_some()
                && self.pipe.bundler.items() < self.pipe.limits().max_messages
            {
                if let Some((idx, after)) = self.pipe.next_chunk(now) {
                    if let Err(e) = self.read_chunk(idx, after).await {
                        self.pipe.abandon_chunk();
                        return Err(e);
                    }
                    continue;
                }
            }

            // 3. Wait for the next thing to do.
            let deadline = self.deadline(now);
            let paused = self.pipe.bundler.backlogged();
            let has_flight = self.in_flight.is_some();
            let in_flight = &mut self.in_flight;
            let stream = &mut self.stream;
            let stop = self.stop;
            tokio::select! {
                biased;
                _ = stop.wait() => {}
                r = settle(in_flight), if has_flight => self.on_committed(r)?,
                ev = stream.next(), if !paused => self.on_stream(ev).await?,
                _ = tokio::time::sleep_until(deadline) => self.on_tick()?,
            }
        }
    }

    fn deadline(&self, now: Instant) -> Instant {
        let mut d = now + Duration::from_secs(1);
        if self.in_flight.is_none() {
            if let Some(l) = self.pipe.bundler.deadline() {
                d = d.min(l);
            }
        }
        d.max(now)
    }

    fn send(&mut self, b: Bundle<super::pointer::MyPosition>) {
        let stats = Stats {
            txns: b.txns,
            messages: b.items.len() as u64,
            snapshot_rows: b.snapshot_rows,
            last_commit_us: b.last_commit_us,
        };
        let cover = self.held.doc.at(&b.cover);
        let commit = Commit::new(
            self.src.ctx.api.clone(),
            self.src.pointer_key.clone(),
            &b.items,
            cover.clone(),
            self.held.version,
        );
        drop(b);
        self.in_flight = Some(InFlight {
            handle: tokio::spawn(commit.run()),
            cover,
            stats,
        });
    }

    fn on_committed(
        &mut self,
        r: std::result::Result<Result<i64>, tokio::task::JoinError>,
    ) -> Result<()> {
        let Some(f) = self.in_flight.take() else {
            return Ok(());
        };
        let version = match r {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(Error::io(format!("the bundle task failed: {e}"))),
        };
        self.committed(f.cover, version, f.stats);
        Ok(())
    }

    fn committed(&mut self, cover: MyPointer, version: i64, s: Stats) {
        self.held = Held {
            doc: cover,
            version,
        };
        let m = &self.src.metrics;
        m.bundles.fetch_add(1, Ordering::Relaxed);
        m.messages.fetch_add(s.messages, Ordering::Relaxed);
        m.transactions.fetch_add(s.txns, Ordering::Relaxed);
        m.snapshot_rows
            .fetch_add(s.snapshot_rows, Ordering::Relaxed);
        if let Some(t) = s.last_commit_us {
            self.last_commit_us = Some(t);
            self.src.status.set(
                "lastCommitAt",
                Value::String(crate::values::iso_utc_micros(t)),
            );
        }
        self.update_lag();
        self.publish();
        self.phase();
    }

    fn update_lag(&self) {
        let lag = if self.caught_up && !self.pipe.bundler.has_items() && self.in_flight.is_none() {
            0
        } else {
            self.last_commit_us
                .map(|t| ((crate::status::now_us() - t) / 1_000_000).max(0))
                .unwrap_or(0)
        };
        self.src.metrics.lag_seconds.store(lag, Ordering::Relaxed);
        self.src.status.set("lagSeconds", json!(lag));
    }

    async fn on_stream(
        &mut self,
        ev: Option<std::result::Result<Event, mysql_async::Error>>,
    ) -> Result<()> {
        let now = Instant::now();
        self.last_event_at = now;
        let ev = match ev {
            None => return Err(Error::io("the server ended the binlog stream")),
            Some(Err(e)) => return Err(stream_error(&e, &self.src.ctx.name)),
            Some(Ok(ev)) => ev,
        };
        let h = ev.header();
        let log_pos = u64::from(h.log_pos());
        let data = ev
            .read_data()
            .map_err(|e| Error::io(format!("an unreadable binlog event: {e}")))?;
        let Some(data) = data else {
            // A type this driver does not know (HEARTBEAT_V2 is never asked
            // for): only its position.
            self.pipe.on_event_pos(log_pos);
            return Ok(());
        };
        self.caught_up = matches!(data, EventData::HeartbeatEvent);
        match data {
            EventData::RotateEvent(r) => {
                self.pipe.on_rotate(r.name().into_owned(), r.position());
            }
            EventData::HeartbeatEvent => {
                // The dump thread is at the end of the binlog: the file is
                // the event's body, the position its log_pos.
                let file = String::from_utf8_lossy(ev.data()).into_owned();
                if !file.is_empty() {
                    self.pipe.on_rotate(file, log_pos);
                } else {
                    self.pipe.on_event_pos(log_pos);
                }
                self.pipe.maybe_queue_chunk(now);
                self.update_lag();
            }
            EventData::GtidEvent(g) => {
                self.pipe.on_event_pos(log_pos);
                let ts = match g.immediate_commit_timestamp() {
                    0 => i64::from(h.timestamp()) * 1_000_000,
                    t => t as i64,
                };
                let tag = g.tag().map(|t| t.as_str().to_string());
                self.pipe
                    .on_gtid(Gtid::from_event(g.sid(), tag.as_deref(), g.gno()), ts)?;
            }
            EventData::AnonymousGtidEvent(_) => {
                return Err(Error::fatal(
                    "gtid_mode",
                    "the binlog holds a transaction without a GTID (written while gtid_mode \
                     was not ON): the source cannot place it; resync once gtid_mode is ON",
                ));
            }
            EventData::TransactionPayloadEvent(_) => {
                self.pipe.on_event_pos(log_pos);
                self.pipe.on_payload();
            }
            EventData::QueryEvent(q) => {
                self.pipe.on_event_pos(log_pos);
                let (schema, sql) = (q.schema().into_owned(), q.query().into_owned());
                self.pipe.on_query(&schema, &sql, now)?;
            }
            EventData::TableMapEvent(t) => {
                self.pipe.on_event_pos(log_pos);
                self.pipe.on_table_map(t.table_id(), &t)?;
            }
            EventData::RowsEvent(r) => {
                self.pipe.on_event_pos(log_pos);
                self.on_rows(&r, now)?;
            }
            EventData::XidEvent(_) | EventData::XaPrepareLogEvent(_) => {
                self.pipe.on_event_pos(log_pos);
                self.pipe.on_commit(now)?;
            }
            EventData::IncidentEvent(i) => {
                return Err(Error::fatal(
                    "binlog_incident",
                    format!(
                        "the server logged an incident in the binlog ({:?}): changes may be \
                         missing from it; check the server, then resync",
                        i
                    ),
                ));
            }
            _ => self.pipe.on_event_pos(log_pos),
        }
        Ok(())
    }

    fn on_rows(&mut self, r: &RowsEventData<'_>, now: Instant) -> Result<()> {
        let kind = match r {
            RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => {
                RowsKind::Write
            }
            RowsEventData::UpdateRowsEvent(_) | RowsEventData::UpdateRowsEventV1(_) => {
                RowsKind::Update
            }
            RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => {
                RowsKind::Delete
            }
            RowsEventData::PartialUpdateRowsEvent(_) => {
                return Err(Error::fatal(
                    "binlog_row_value_options",
                    "the binlog carries a partial JSON update (binlog_row_value_options = \
                     PARTIAL_JSON when it was written): the source needs whole rows",
                ))
            }
        };
        let table_id = r.table_id();
        let Some(tme) = self.stream.get_tme(table_id) else {
            return Err(Error::io(format!(
                "binlog: a rows event of table id {table_id} before its table map"
            )));
        };
        let rows = super::rows::decode(r, tme)?;
        self.pipe.on_rows(table_id, kind, rows, now)
    }

    /// One chunk: `lw`, the read in its own consistent snapshot, `hw`.
    async fn read_chunk(&mut self, idx: usize, after: Option<Vec<String>>) -> Result<()> {
        let t = self.pipe.tables[idx].clone();
        let limit = self.chunk_rows;
        let lw = binlog_status(&mut self.q, &self.info).await?.executed;
        self.q
            .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
            .await
            .map_err(|e| classify(&e))?;
        let read_at = crate::status::now_us();
        let sql = chunk_sql(&t, after.is_some());
        let params = chunk_params(&t, after.as_deref(), limit)?;
        let rows: std::result::Result<Vec<Row>, _> = self.q.exec(sql, params).await;
        let rows = match rows {
            Ok(r) => r,
            Err(e) => {
                let _ = self.q.query_drop("ROLLBACK").await;
                return Err(classify(&e));
            }
        };
        self.q
            .query_drop("COMMIT")
            .await
            .map_err(|e| classify(&e))?;
        let hw = binlog_status(&mut self.q, &self.info).await?.pos;
        let mut bytes = 0usize;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let vals = row.unwrap();
            let mut cells = Vec::with_capacity(vals.len());
            for (v, c) in vals.into_iter().zip(&t.cols) {
                bytes += match &v {
                    mysql_async::Value::Bytes(b) => b.len() + 8,
                    _ => 16,
                };
                cells.push(
                    Cell::from_value(c.kind, v)
                        .map_err(|e| Error::io(format!("{}.{}: {e}", t.name, c.name)))?,
                );
            }
            out.push(cells);
        }
        // Sized by bytes, not count: shrink to fit the budget, grow back
        // (doubling, up to the document's size) while chunks stay small.
        let budget = super::snapshot_chunk_max_bytes();
        if bytes > budget && !out.is_empty() {
            let per_row = (bytes / out.len()).max(1);
            self.chunk_rows = (budget / per_row).clamp(1, self.chunk_rows_max);
        } else if bytes < budget / 4 && out.len() == limit {
            self.chunk_rows = limit.saturating_mul(2).min(self.chunk_rows_max);
        }
        tracing::debug!(
            target: crate::LOG_TARGET,
            connector = %self.src.ctx.name,
            table = %t.name,
            rows = out.len(),
            hw = %hw,
            "snapshot chunk read"
        );
        let now = Instant::now();
        self.pipe
            .install_chunk(Chunk::new(idx, &t, limit, out, lw, hw, read_at), now);
        self.phase();
        Ok(())
    }

    fn on_tick(&mut self) -> Result<()> {
        let now = Instant::now();
        if now.saturating_duration_since(self.last_event_at) > self.silence {
            return Err(Error::io(format!(
                "nothing from the binlog stream for {:?} (not even a heartbeat)",
                self.silence
            )));
        }
        if self.in_flight.is_none()
            && now.saturating_duration_since(self.last_idle_at) >= IDLE_EVERY
        {
            self.last_idle_at = now;
            self.pipe.idle(&self.held.doc.pos, now);
        }
        self.update_lag();
        Ok(())
    }

    fn phase(&self) {
        self.src.status.set_phase(if self.pipe.snap.is_some() {
            "snapshot"
        } else {
            "streaming"
        });
    }

    fn publish(&self) {
        let st = &self.src.status;
        let m = &self.src.metrics;
        st.set("pointerGtids", json!(self.held.doc.pos.gtids.to_string()));
        st.set("epoch", json!(self.held.doc.epoch));
        st.set(
            "binlog",
            json!({"file": self.pipe.pos.file, "position": self.pipe.pos.pos}),
        );
        st.set(
            "transactions",
            json!(m.transactions.load(Ordering::Relaxed)),
        );
        st.set("messages", json!(m.messages.load(Ordering::Relaxed)));
        st.set("bundles", json!(m.bundles.load(Ordering::Relaxed)));
        st.set(
            "snapshot",
            match &self.held.doc.pos.snapshot {
                Some(s) => json!({
                    "tables": s.tables.len(),
                    "done": s.tables_done(),
                    "table": s.table,
                    "rows": s.rows,
                }),
                None => Value::Null,
            },
        );
        st.set("truncatesSkipped", json!(self.pipe.truncates_skipped));
        st.set("snapshotRowsDropped", json!(self.pipe.snapshot_dropped));
    }

    async fn finish(mut self, r: Result<()>) -> Result<RunEnd> {
        let bound = if r.is_ok() {
            Duration::from_millis(self.src.ctx.knobs.shutdown_grace_ms.clamp(1_000, 60_000))
        } else {
            ERROR_SETTLE
        };
        if let Some(mut f) = self.in_flight.take() {
            match tokio::time::timeout(bound, &mut f.handle).await {
                Ok(Ok(Ok(v))) => self.committed(f.cover, v, f.stats),
                Ok(Ok(Err(e))) => {
                    tracing::warn!(
                        target: crate::LOG_TARGET,
                        connector = %self.src.ctx.name,
                        error = %e,
                        "the last bundle did not commit"
                    );
                }
                Ok(Err(_)) => {}
                Err(_) => f.handle.abort(),
            }
        }
        let _ = tokio::time::timeout(Duration::from_secs(2), self.stream.close()).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), self.q.disconnect()).await;
        r.map(|()| RunEnd::Stopped)
    }
}
