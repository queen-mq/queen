//! The streaming loop (PLAN §4.4): the replication stream, the bundle in
//! flight, the snapshot's chunk reads and the timers, in one task.
//!
//! The loop owns the order of everything that matters: frames are decoded in
//! stream order into the [`Pipeline`]; ONE bundle is in flight at a time (a
//! spawned task, so the stream keeps being read and keepalives answered while
//! the broker commits); the next bundle is planned against the version the
//! previous one returned. PostgreSQL is told a flush position only after the
//! pointer that covers it committed (I2: persist first, confirm second).

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::FutureExt;
use serde_json::{json, Value};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_postgres::Client;

use crate::connector::RunEnd;
use crate::error::{Error, Result};
use crate::pg::connect::classify;
use crate::repl::pgoutput::Message;
use crate::repl::{Lsn, ReplicationStream, StreamEvent};
use crate::stop::Stop;

use super::bundle::{Bundle, Commit};
use super::events::TypeConv;
use super::lease::Lease;
use super::pipeline::{patch, Note, Pipeline};
use super::pointer::{Held, Pointer};
use super::snapshot::{chunk_sql, hb_sql, lw_sql, parse_chunk, Chunk};
use super::{slot, types, Source};

/// A status update at least this often, whatever happens (wal_sender_timeout
/// is 60 s by default).
const STATUS_EVERY: Duration = Duration::from_secs(10);
/// After a committed bundle, a status update at most this often.
const STATUS_MIN_GAP: Duration = Duration::from_secs(1);
/// Idle advancement at most this often (PLAN §4.4 "Idle").
const IDLE_EVERY: Duration = Duration::from_secs(10);
/// Slot lag read this often (PLAN §4.7).
const LAG_EVERY: Duration = Duration::from_secs(10);
/// How long an error exit waits for the bundle in flight to settle, so the
/// next start reads a pointer that is not about to move.
const ERROR_SETTLE: Duration = Duration::from_secs(5);

/// What a bundle counts for, kept while its events travel.
#[derive(Debug, Clone, Copy)]
struct Stats {
    txns: u64,
    messages: u64,
    snapshot_rows: u64,
    last_commit_us: Option<i64>,
}

struct InFlight {
    handle: JoinHandle<Result<i64>>,
    cover: Pointer,
    stats: Stats,
}

pub(super) struct Engine<'a> {
    src: &'a Source,
    stop: &'a Stop,
    lease: &'a Lease,
    pg: Client,
    stream: ReplicationStream,
    pipe: Pipeline,
    types: HashMap<u32, TypeConv>,
    held: Held,
    in_flight: Option<InFlight>,
    last_received: Lsn,
    last_status_at: Instant,
    status_due: bool,
    last_data_at: Instant,
    last_idle_at: Option<Instant>,
    next_lag_at: Instant,
    heartbeat: Duration,
    chunk_rows: usize,
}

/// The bundle task's answer, or pending forever when nothing is in flight.
async fn settle(
    f: &mut Option<InFlight>,
) -> std::result::Result<Result<i64>, tokio::task::JoinError> {
    match f {
        Some(x) => (&mut x.handle).await,
        None => std::future::pending().await,
    }
}

impl<'a> Engine<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        src: &'a Source,
        stop: &'a Stop,
        lease: &'a Lease,
        pg: Client,
        stream: ReplicationStream,
        pipe: Pipeline,
        types: HashMap<u32, TypeConv>,
        held: Held,
    ) -> Engine<'a> {
        let now = Instant::now();
        Engine {
            src,
            stop,
            lease,
            pg,
            stream,
            pipe,
            types,
            last_received: held.doc.lsn,
            held,
            in_flight: None,
            last_status_at: now,
            status_due: false,
            last_data_at: now,
            last_idle_at: None,
            next_lag_at: now,
            heartbeat: Duration::from_secs(src.spec.heartbeat_seconds.max(1)),
            chunk_rows: src.spec.snapshot_chunk_rows.max(1) as usize,
        }
    }

    /// Stream until stopped (`Ok(Stopped)`) or an error.
    pub(super) async fn run(mut self) -> Result<RunEnd> {
        let r = self.run_loop().await;
        self.finish(r).await
    }

    async fn run_loop(&mut self) -> Result<()> {
        self.phase();
        self.publish();
        // Tell the server where the pointer is at once: the slot may lag it.
        self.send_status().await?;
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

            // 1. A bundle, when one should go. "Nothing buffered" is asked of
            //    the stream itself: a frame that is ready now is read first.
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

            // 2. A snapshot chunk, when one may be read: one at a time, and
            //    not while a bundle's worth of events waits.
            if self.pipe.snap.is_some()
                && self.pipe.bundler.items() < self.pipe.bundler.limits().max_messages
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
                r = settle(in_flight), if has_flight => self.on_committed(r).await?,
                ev = stream.next(), if !paused => self.on_stream(ev).await?,
                _ = tokio::time::sleep_until(deadline) => self.on_tick().await?,
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
        if self.status_due {
            d = d.min(self.last_status_at + STATUS_MIN_GAP);
        }
        d = d
            .min(self.last_status_at + STATUS_EVERY)
            .min(self.last_data_at + self.heartbeat)
            .min(self.next_lag_at);
        d.max(now)
    }

    fn send(&mut self, b: Bundle) {
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

    async fn on_committed(
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
        if self.last_status_at.elapsed() >= STATUS_MIN_GAP {
            self.send_status().await?;
        }
        Ok(())
    }

    fn committed(&mut self, cover: Pointer, version: i64, s: Stats) {
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
        m.pointer_lsn.store(self.held.doc.lsn.0, Ordering::Relaxed);
        if let Some(t) = s.last_commit_us {
            self.src.status.set(
                "lastCommitAt",
                Value::String(crate::values::iso_utc_micros(t)),
            );
        }
        self.status_due = true;
        self.publish();
    }

    async fn on_stream(&mut self, ev: Result<Option<StreamEvent>>) -> Result<()> {
        let now = Instant::now();
        match ev? {
            None => Err(Error::io("the server ended the replication stream")),
            Some(StreamEvent::Keepalive {
                wal_end,
                reply_requested,
                ..
            }) => {
                self.last_received = self.last_received.max(wal_end);
                self.maybe_idle(wal_end, now);
                if reply_requested {
                    self.send_status().await?;
                }
                Ok(())
            }
            Some(StreamEvent::XLogData {
                wal_start,
                wal_end,
                message,
                ..
            }) => {
                self.last_received = self.last_received.max(wal_start).max(wal_end);
                self.last_data_at = now;
                self.on_message(message, now).await
            }
        }
    }

    async fn on_message(&mut self, m: Message, now: Instant) -> Result<()> {
        match m {
            Message::Begin {
                final_lsn,
                commit_time_us,
                xid,
            } => self.pipe.on_begin(final_lsn, commit_time_us, xid),
            Message::Commit { end_lsn, .. } => self.pipe.on_commit(end_lsn, now),
            Message::Relation(rel) => {
                let oids: Vec<u32> = rel.columns.iter().map(|c| c.type_oid).collect();
                types::resolve(&self.pg, &oids, &mut self.types).await?;
                self.pipe.on_relation(&rel, &self.types)
            }
            Message::Insert { relid, new } => self.pipe.on_insert(relid, &new, now),
            Message::Update { relid, old, new } => {
                // A key change with unchanged TOAST values: read them back
                // for the new key first (Pipeline::read_back says why).
                let new = match self.pipe.read_back(relid, old.as_ref(), &new) {
                    Some(rb) => {
                        let row = self.read_back_row(&rb.sql).await?;
                        patch(new, &rb, row)
                    }
                    None => new,
                };
                self.pipe.on_update(relid, old.as_ref(), &new, now)
            }
            Message::Delete { relid, old } => self.pipe.on_delete(relid, &old, now),
            Message::Truncate { options, relids } => {
                self.pipe.on_truncate(options, &relids, now)?;
                self.src
                    .status
                    .set("truncatesSkipped", json!(self.pipe.truncates_skipped));
                Ok(())
            }
            Message::LogicalMessage {
                transactional,
                lsn,
                prefix,
                content,
            } => {
                match self
                    .pipe
                    .on_message(transactional, lsn, &prefix, &content, now)
                {
                    Note::Heartbeat(at) => self.maybe_idle(at, now),
                    Note::High { finished, .. } => {
                        if finished {
                            tracing::info!(
                                target: crate::LOG_TARGET,
                                connector = %self.src.ctx.name,
                                "snapshot read to the end; its last chunk is on its way"
                            );
                        }
                        self.phase();
                    }
                    Note::Low | Note::Ignored => {}
                }
                Ok(())
            }
            Message::Type { .. } | Message::Origin { .. } | Message::Other { .. } => Ok(()),
        }
    }

    /// The first row of a read-back statement, as output text (`None`: no
    /// row).
    async fn read_back_row(&self, sql: &str) -> Result<Option<Vec<Option<String>>>> {
        let msgs = self.pg.simple_query(sql).await.map_err(|e| classify(&e))?;
        Ok(msgs.iter().find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect())
            }
            _ => None,
        }))
    }

    /// Idle advancement: the server says it sent everything before `wal_end`.
    fn maybe_idle(&mut self, wal_end: Lsn, now: Instant) {
        if self.in_flight.is_some() {
            return;
        }
        if self
            .last_idle_at
            .is_some_and(|t| now.saturating_duration_since(t) < IDLE_EVERY)
        {
            return;
        }
        if self.pipe.idle(wal_end, now) {
            self.last_idle_at = Some(now);
        }
    }

    /// One chunk: the low watermark, then the chunk and the high watermark
    /// in one round trip (PLAN §4.5, [`super::snapshot`]).
    async fn read_chunk(&mut self, idx: usize, after: Option<Vec<String>>) -> Result<()> {
        let t = self.pipe.tables[idx].clone();
        let Some(n) = self.pipe.snap.as_mut().map(|s| s.take_n()) else {
            return Ok(());
        };
        let epoch = self.pipe.epoch.clone();
        slot::emit(&self.pg, &lw_sql(&epoch, n)).await?;
        let read_at = crate::status::now_us();
        let msgs = self
            .pg
            .simple_query(&chunk_sql(&t, after.as_deref(), self.chunk_rows, &epoch, n))
            .await
            .map_err(|e| classify(&e))?;
        let ans = parse_chunk(&msgs, t.cols.len())?;
        tracing::debug!(
            target: crate::LOG_TARGET,
            connector = %self.src.ctx.name,
            table = %t.name,
            chunk = n,
            rows = ans.rows.len(),
            hw = %ans.hw_end,
            "snapshot chunk read"
        );
        self.pipe.install_chunk(Chunk::new(
            n,
            idx,
            &t,
            self.chunk_rows,
            ans.rows,
            ans.vis,
            read_at,
        ));
        Ok(())
    }

    async fn on_tick(&mut self) -> Result<()> {
        let now = Instant::now();
        if (self.status_due && now >= self.last_status_at + STATUS_MIN_GAP)
            || now >= self.last_status_at + STATUS_EVERY
        {
            self.send_status().await?;
        }
        if now >= self.last_data_at + self.heartbeat {
            slot::emit(&self.pg, &hb_sql(&self.pipe.epoch)).await?;
            self.last_data_at = now;
        }
        if now >= self.next_lag_at {
            self.next_lag_at = now + LAG_EVERY;
            self.read_lag().await?;
        }
        Ok(())
    }

    async fn read_lag(&mut self) -> Result<()> {
        if let Some((lag, wal_status, confirmed)) = slot::slot_lag(&self.pg, &self.src.slot).await?
        {
            self.src
                .metrics
                .slot_lag_bytes
                .store(lag, Ordering::Relaxed);
            let st = &self.src.status;
            st.set("slotLagBytes", json!(lag));
            st.set("walStatus", json!(wal_status));
            st.set("confirmedFlushLsn", json!(confirmed.map(|l| l.to_string())));
        }
        Ok(())
    }

    /// One Standby Status Update: write = what was received, flush = apply =
    /// the committed pointer (never more: I2).
    async fn send_status(&mut self) -> Result<()> {
        let flush = self.held.doc.lsn;
        let write = self.last_received.max(flush);
        self.stream.send_status(write, flush, flush, false).await?;
        self.last_status_at = Instant::now();
        self.status_due = false;
        Ok(())
    }

    fn phase(&self) {
        self.src.status.set_phase(if self.pipe.snap.is_some() {
            "snapshot"
        } else {
            "streaming"
        });
    }

    /// The status detail fields of PLAN §4.7 that change with the pointer.
    fn publish(&self) {
        let st = &self.src.status;
        let m = &self.src.metrics;
        st.set("pointerLsn", json!(self.held.doc.lsn.to_string()));
        st.set("epoch", json!(self.held.doc.epoch));
        st.set(
            "transactions",
            json!(m.transactions.load(Ordering::Relaxed)),
        );
        st.set("messages", json!(m.messages.load(Ordering::Relaxed)));
        st.set("bundles", json!(m.bundles.load(Ordering::Relaxed)));
        st.set(
            "snapshot",
            match &self.held.doc.snapshot {
                Some(s) => json!({
                    "tables": s.tables.len(),
                    "done": s.tables_done(),
                    "table": s.table,
                    "rows": s.rows,
                }),
                None => Value::Null,
            },
        );
    }

    /// The end of a run: let the bundle in flight finish (bounded), confirm
    /// what committed, close the stream.
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
        if r.is_ok() {
            let _ = self.send_status().await;
        }
        self.stream.close(Duration::from_secs(2)).await;
        r.map(|()| RunEnd::Stopped)
    }
}
