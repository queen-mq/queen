//! The MySQL source's pure core: binlog events in, units out. No I/O: the
//! engine feeds it the stream, the snapshot's chunks and the clock, so every
//! rule is testable without a server.
//!
//! A transaction in the binlog is `GTID`, then (`BEGIN`, table maps, row
//! events, `XID`) or a single statement (DDL, `TRUNCATE`). It becomes one
//! unit whose cover adds its GTID to the pointer's set; one that outgrows the
//! bundle limits is cut into parts, each covering the GTID set as it was plus
//! `inTxn` (the transaction's GTID and how many of its row changes are in).
//! The pipeline keeps the **tail**, the position once everything queued is
//! in Queen, and every cover is computed from it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use mysql_common::binlog::events::TableMapEvent;
use tokio::time::Instant;

use crate::config::TruncatePolicy;
use crate::error::{Error, Result};
use crate::source::bundle::{Bundle, Bundler, Limits, Unit};
use crate::source::events::Item;

use super::catalog::BinlogPos;
use super::events::{self, Change, Cols};
use super::gtid::Gtid;
use super::pointer::{InTxn, MyPosition};
use super::rows::Image;
use super::snapshot::{chunk_units, Chunk, SnapState, Touch};
use super::table::{shape, MyTable, Shape};
use super::values::{Cell, Kind};

/// Which row event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowsKind {
    Write,
    Update,
    Delete,
}

#[derive(Debug)]
struct Txn {
    gtid: Gtid,
    ts_us: i64,
    /// Row changes decoded so far (every row of every rows event, mapped or
    /// not, plus a TRUNCATE), in binlog order: the index of the next one,
    /// and the event's `seq`.
    raw: u64,
    /// Changes below this are in Queen already (a split transaction
    /// resumed).
    skip: u64,
    /// The whole transaction is in Queen (its GTID is in the set).
    covered: bool,
    /// `BEGIN` was seen: a later statement is inside the transaction, not
    /// the transaction.
    began: bool,
    pending: Vec<Item>,
    pending_bytes: usize,
    events: u64,
    touched: Vec<(usize, Option<String>)>,
}

#[derive(Debug)]
pub struct Pipeline {
    pub epoch: String,
    pub tables: Arc<Vec<MyTable>>,
    /// `(db, table)`, lowercase too, to the table index.
    names: HashMap<(String, String), usize>,
    shapes: HashMap<u64, Option<Shape>>,
    txn: Option<Txn>,
    pub bundler: Bundler<MyPosition>,
    pub tail: MyPosition,
    pub snap: Option<SnapState>,
    pending_tables: HashSet<usize>,
    on_truncate: TruncatePolicy,
    pub truncates_skipped: u64,
    /// Snapshot rows a stream change made stale (the change carried them).
    pub snapshot_dropped: u64,
    /// Where the stream is in the binlog (the end of the last event read).
    pub pos: BinlogPos,
    /// Inside a compressed transaction payload: its inner events carry no
    /// position of the file.
    in_payload: bool,
}

/// What a statement ([`Pipeline::on_query`]) was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Begin,
    Commit,
    /// `TRUNCATE [TABLE] [db.]t`.
    Truncate(Option<String>, String),
    Other,
}

/// What a statement says, cheaply: the binlog holds what the client sent.
pub fn statement(sql: &str) -> Statement {
    let s = sql.trim().trim_end_matches(';').trim();
    let lower = s.to_ascii_lowercase();
    match lower.as_str() {
        "begin" | "xa start" => return Statement::Begin,
        "commit" | "rollback" => return Statement::Commit,
        _ => {}
    }
    let Some(rest) = lower.strip_prefix("truncate") else {
        return Statement::Other;
    };
    if !rest.starts_with(char::is_whitespace) {
        return Statement::Other;
    }
    // Keep the original case of the name: offsets in `s` match `lower`
    // (ASCII lowering keeps lengths).
    let mut name = s[s.len() - rest.len()..].trim();
    if name.len() >= 5 && name[..5].eq_ignore_ascii_case("table") {
        let after = &name[5..];
        if after.starts_with(char::is_whitespace) || after.starts_with('`') {
            name = after.trim();
        }
    }
    let parts = split_name(name);
    match parts.as_slice() {
        [t] => Statement::Truncate(None, t.clone()),
        [d, t] => Statement::Truncate(Some(d.clone()), t.clone()),
        _ => Statement::Other,
    }
}

/// `` `a`.`b` `` / `a.b` / `b` → its parts, backticks undone.
fn split_name(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '`' if quoted && chars.peek() == Some(&'`') => {
                chars.next();
                cur.push('`');
            }
            '`' => quoted = !quoted,
            '.' if !quoted => out.push(std::mem::take(&mut cur)),
            c if !quoted && c.is_whitespace() => return Vec::new(),
            c => cur.push(c),
        }
    }
    if quoted || cur.is_empty() {
        return Vec::new();
    }
    out.push(cur);
    out
}

impl Pipeline {
    pub fn new(
        epoch: String,
        tables: Arc<Vec<MyTable>>,
        start: MyPosition,
        limits: Limits,
        on_truncate: TruncatePolicy,
    ) -> Pipeline {
        let mut names = HashMap::new();
        for (i, t) in tables.iter().enumerate() {
            names.insert((t.tn.db.clone(), t.tn.name.clone()), i);
            names
                .entry((t.tn.db.to_lowercase(), t.tn.name.to_lowercase()))
                .or_insert(i);
        }
        let snap = start.snapshot.clone().map(SnapState::new);
        let mut p = Pipeline {
            epoch,
            tables,
            names,
            shapes: HashMap::new(),
            txn: None,
            bundler: Bundler::new(limits),
            tail: start,
            snap,
            pending_tables: HashSet::new(),
            on_truncate,
            truncates_skipped: 0,
            snapshot_dropped: 0,
            pos: BinlogPos::default(),
            in_payload: false,
        };
        p.refresh_pending();
        p
    }

    fn refresh_pending(&mut self) {
        self.pending_tables.clear();
        if let Some(s) = &self.snap {
            let from = s.progress.tables_done();
            for name in s.progress.tables.iter().skip(from) {
                if let Some(i) = self.tables.iter().position(|t| t.name == *name) {
                    self.pending_tables.insert(i);
                }
            }
        }
    }

    fn table_of(&self, db: &str, name: &str) -> Option<usize> {
        self.names
            .get(&(db.to_string(), name.to_string()))
            .or_else(|| self.names.get(&(db.to_lowercase(), name.to_lowercase())))
            .copied()
    }

    pub fn in_txn(&self) -> bool {
        self.txn.is_some()
    }

    /// An event's end position in the current file (`log_pos`, 0 for an
    /// artificial event).
    pub fn on_event_pos(&mut self, log_pos: u64) {
        if log_pos > 0 && !self.in_payload {
            self.pos.pos = self.pos.pos.max(log_pos);
        }
    }

    /// A ROTATE: the stream goes on in `file` at `pos`.
    pub fn on_rotate(&mut self, file: String, pos: u64) {
        if file != self.pos.file {
            self.pos = BinlogPos { file, pos };
        } else {
            self.pos.pos = self.pos.pos.max(pos);
        }
    }

    /// A compressed transaction payload begins (its own end position was
    /// recorded); the events inside it follow.
    pub fn on_payload(&mut self) {
        self.in_payload = true;
    }

    pub fn on_gtid(&mut self, gtid: Gtid, ts_us: i64) -> Result<()> {
        if let Some(t) = &self.txn {
            return Err(Error::io(format!(
                "binlog: the GTID {gtid} begins inside the open transaction {}",
                t.gtid
            )));
        }
        let skip = match &self.tail.in_txn {
            Some(t) if t.gtid == gtid.to_string() => t.done,
            _ => 0,
        };
        self.txn = Some(Txn {
            covered: self.tail.gtids.contains(&gtid),
            gtid,
            ts_us,
            raw: 0,
            skip,
            began: false,
            pending: Vec::new(),
            pending_bytes: 0,
            events: 0,
            touched: Vec::new(),
        });
        Ok(())
    }

    pub fn on_table_map(&mut self, table_id: u64, tme: &TableMapEvent<'_>) -> Result<()> {
        let db = tme.database_name();
        let name = tme.table_name();
        match self.table_of(&db, &name) {
            Some(i) => {
                let s = shape(i, &self.tables[i], tme)?;
                self.shapes.insert(table_id, Some(s));
            }
            None => {
                self.shapes.insert(table_id, None);
            }
        }
        Ok(())
    }

    fn next_raw(&mut self, what: &str) -> Result<(u64, bool)> {
        let Some(txn) = &mut self.txn else {
            return Err(Error::io(format!("binlog: {what} outside a transaction")));
        };
        let raw = txn.raw;
        txn.raw += 1;
        Ok((raw, !txn.covered && raw >= txn.skip))
    }

    /// One rows event: its rows as `(before, after)` images.
    pub fn on_rows(
        &mut self,
        table_id: u64,
        kind: RowsKind,
        rows: Vec<(Option<Image>, Option<Image>)>,
        now: Instant,
    ) -> Result<()> {
        if self.txn.as_ref().is_some_and(|t| t.covered) {
            // Still counted: the raw index must not depend on what is mapped.
            if let Some(t) = &mut self.txn {
                t.raw += rows.len() as u64;
            }
            return Ok(());
        }
        let shape = match self.shapes.get(&table_id) {
            Some(Some(s)) => s.clone(),
            Some(None) => {
                if let Some(t) = &mut self.txn {
                    t.raw += rows.len() as u64;
                }
                return Ok(());
            }
            None => {
                return Err(Error::io(format!(
                    "binlog: a rows event of table id {table_id} before its table map"
                )))
            }
        };
        let idx = shape.table;
        let snapshotting = self.pending_tables.contains(&idx);
        let names: Vec<&str> = shape.cols.iter().map(|c| c.name.as_str()).collect();
        let kinds: Vec<Kind> = shape.cols.iter().map(|c| c.kind).collect();
        let cols = Cols {
            names: &names,
            kinds: &kinds,
        };
        for (before, after) in rows {
            let (raw, emit) = self.next_raw("a row change")?;
            if !emit && !snapshotting {
                continue;
            }
            let b = before.map(|r| cells(&shape, r)).transpose()?;
            let a = after.map(|r| cells(&shape, r)).transpose()?;
            let ch: Change = match (kind, &b, &a) {
                (RowsKind::Write, _, Some(a)) => {
                    events::insert(&cols, &shape.key, shape.part.as_deref(), a)
                }
                (RowsKind::Update, Some(b), Some(a)) => {
                    events::update(&cols, &shape.key, shape.part.as_deref(), b, a)
                }
                (RowsKind::Delete, Some(b), _) => {
                    events::delete(&cols, &shape.key, shape.part.as_deref(), b)
                }
                _ => {
                    return Err(Error::io(format!(
                        "binlog: a {kind:?} row of {} without its image",
                        self.tables[idx].name
                    )))
                }
            };
            if snapshotting {
                if let Some(t) = &mut self.txn {
                    t.touched
                        .extend(ch.touched.iter().map(|k| (idx, Some(k.clone()))));
                }
            }
            if !emit {
                continue;
            }
            let items = self.items(idx, ch, raw);
            self.push_change(items, raw, now);
        }
        Ok(())
    }

    fn items(&self, idx: usize, ch: Change, raw: u64) -> Vec<Item> {
        let Some(txn) = &self.txn else {
            return Vec::new();
        };
        let t = &self.tables[idx];
        ch.events
            .into_iter()
            .enumerate()
            .map(|(k, (partition, body))| Item {
                queue: t.queue.clone(),
                partition,
                txn_id: events::stream_id(&self.epoch, &txn.gtid, raw, k),
                payload: body.render(&t.name, Some(&txn.gtid), txn.ts_us, raw),
            })
            .collect()
    }

    /// Queue a change's events; a transaction past the bundle limits is cut
    /// BEFORE this change, so a part's `inTxn.done` says exactly which
    /// changes are in Queen.
    fn push_change(&mut self, items: Vec<Item>, raw: u64, now: Instant) {
        if items.is_empty() {
            return;
        }
        let limits = self.bundler.limits();
        let Some(txn) = &mut self.txn else {
            return;
        };
        let len: usize = items.iter().map(Item::wire_len).sum();
        if !txn.pending.is_empty()
            && (txn.pending.len() + items.len() > limits.max_messages
                || txn.pending_bytes + len > limits.max_bytes)
        {
            let cover = MyPosition {
                gtids: self.tail.gtids.clone(),
                in_txn: Some(InTxn {
                    gtid: txn.gtid.to_string(),
                    done: raw,
                }),
                snapshot: self.tail.snapshot.clone(),
            };
            let part = std::mem::take(&mut txn.pending);
            txn.pending_bytes = 0;
            self.bundler.push(Unit::new(part, cover.clone()), now);
            self.tail = cover;
        }
        txn.events += items.len() as u64;
        txn.pending_bytes += len;
        txn.pending.extend(items);
    }

    /// A statement event. Inside `BEGIN … COMMIT` it is part of the
    /// transaction; otherwise (DDL, `TRUNCATE`) it is the transaction.
    pub fn on_query(&mut self, schema: &str, sql: &str, now: Instant) -> Result<()> {
        let st = statement(sql);
        let Some(txn) = &mut self.txn else {
            // A statement with no GTID before it (an artificial event):
            // nothing to do.
            return Ok(());
        };
        match st {
            Statement::Begin => {
                txn.began = true;
                Ok(())
            }
            Statement::Commit => self.on_commit(now),
            _ if txn.began => Ok(()),
            Statement::Truncate(db, name) => {
                let db = db.unwrap_or_else(|| schema.to_string());
                self.on_truncate(&db, &name, now)?;
                self.on_commit(now)
            }
            Statement::Other => self.on_commit(now),
        }
    }

    fn on_truncate(&mut self, db: &str, name: &str, now: Instant) -> Result<()> {
        let (raw, emit) = self.next_raw("TRUNCATE")?;
        let Some(idx) = self.table_of(db, name) else {
            return Ok(());
        };
        if self.pending_tables.contains(&idx) {
            if let Some(t) = &mut self.txn {
                t.touched.push((idx, None));
            }
        }
        if !emit {
            return Ok(());
        }
        let t = &self.tables[idx];
        match self.on_truncate {
            TruncatePolicy::Skip => {
                self.truncates_skipped += 1;
                tracing::info!(
                    target: crate::LOG_TARGET,
                    table = %t.name,
                    "TRUNCATE not emitted (onTruncate: skip)"
                );
            }
            TruncatePolicy::Emit => {
                let Some(txn) = &self.txn else {
                    return Ok(());
                };
                let item = Item {
                    queue: t.queue.clone(),
                    partition: "all".into(),
                    txn_id: events::stream_id(&self.epoch, &txn.gtid, raw, 0),
                    payload: events::truncate_event(&[&t.name], &txn.gtid, txn.ts_us, raw),
                };
                self.push_change(vec![item], raw, now);
            }
        }
        Ok(())
    }

    /// The transaction's end (`XID`, `COMMIT`, an XA prepare, or the end of
    /// a one-statement transaction).
    pub fn on_commit(&mut self, now: Instant) -> Result<()> {
        self.in_payload = false;
        let Some(txn) = self.txn.take() else {
            return Err(Error::io("binlog: a commit outside a transaction"));
        };
        if txn.covered {
            return Ok(());
        }
        if let Some(s) = &mut self.snap {
            s.on_commit(Touch {
                gtid: txn.gtid.clone(),
                keys: txn.touched,
            });
        }
        let mut gtids = self.tail.gtids.clone();
        gtids.add(&txn.gtid);
        let cover = MyPosition {
            gtids,
            in_txn: None,
            snapshot: self.tail.snapshot.clone(),
        };
        let mut u = Unit::new(txn.pending, cover.clone());
        if txn.events > 0 || txn.skip > 0 {
            u.txns = 1;
            u.last_commit_us = Some(txn.ts_us);
        }
        self.bundler.push(u, now);
        self.tail = cover;
        self.maybe_queue_chunk(now);
        Ok(())
    }

    /// The snapshot's next chunk to read: `(table index, after)`, when a
    /// snapshot runs and no chunk is in flight. A table of the run no longer
    /// configured is skipped.
    pub fn next_chunk(&mut self, now: Instant) -> Option<(usize, Option<Vec<String>>)> {
        loop {
            let s = self.snap.as_mut()?;
            if s.chunk.is_some() {
                return None;
            }
            match self.tables.iter().position(|t| t.name == s.progress.table) {
                Some(i) => return Some((i, s.progress.after.clone())),
                None => {
                    let next = s.progress.next_table(s.progress.rows);
                    self.advance_snapshot(next, now);
                }
            }
        }
    }

    fn advance_snapshot(
        &mut self,
        next: Option<crate::source::pointer::SnapshotProgress>,
        now: Instant,
    ) {
        let cover = MyPosition {
            snapshot: next.clone(),
            ..self.tail.clone()
        };
        let mut u = Unit::new(Vec::new(), cover.clone());
        u.force = true;
        self.bundler.push(u, now);
        self.tail = cover;
        match next {
            Some(p) => {
                if let Some(s) = &mut self.snap {
                    s.progress = p;
                }
            }
            None => self.snap = None,
        }
        self.refresh_pending();
    }

    /// A chunk was read. It is queued at once when the stream is already
    /// past its `hw`.
    pub fn install_chunk(&mut self, c: Chunk, now: Instant) {
        if let Some(s) = &mut self.snap {
            s.install(c);
        }
        self.maybe_queue_chunk(now);
    }

    /// A chunk read failed: forget it.
    pub fn abandon_chunk(&mut self) {
        if let Some(s) = &mut self.snap {
            s.chunk = None;
        }
    }

    /// Queue the chunk in flight once the stream has read to its `hw`
    /// (outside a transaction). Returns the rows queued.
    pub fn maybe_queue_chunk(&mut self, now: Instant) -> Option<usize> {
        if self.txn.is_some() {
            return None;
        }
        let ready = self
            .snap
            .as_ref()
            .and_then(|s| s.chunk.as_ref())
            .is_some_and(|c| self.pos.reached(&c.hw));
        if !ready {
            return None;
        }
        let s = self.snap.as_mut()?;
        let chunk = s.chunk.take()?;
        self.snapshot_dropped += (chunk.rows.len() - chunk.surviving()) as u64;
        let t = &self.tables[chunk.table];
        let (units, end) = chunk_units(
            &chunk,
            t,
            &self.epoch,
            &self.tail,
            &s.progress,
            &self.bundler.limits(),
        );
        for u in units {
            self.tail = u.cover.clone();
            self.bundler.push(u, now);
        }
        match end {
            Some(p) => s.progress = p,
            None => self.snap = None,
        }
        self.refresh_pending();
        Some(chunk.surviving())
    }

    /// Idle advancement: the tail moved past the committed pointer with
    /// transactions of tables this source does not map; write it down, so a
    /// restart (or the binlog's expiry) does not reach back for them.
    pub fn idle(&mut self, committed: &MyPosition, now: Instant) -> bool {
        if self.txn.is_some()
            || self.bundler.has_items()
            || self.tail.in_txn.is_some()
            || self.tail == *committed
        {
            return false;
        }
        let mut u = Unit::new(Vec::new(), self.tail.clone());
        u.force = true;
        self.bundler.push(u, now);
        true
    }

    /// The next bundle, when one should go.
    pub fn take(&mut self, now: Instant, stream_idle: bool) -> Option<Bundle<MyPosition>> {
        if self.bundler.due(now) || (stream_idle && self.bundler.has_items()) {
            self.bundler.take()
        } else {
            None
        }
    }

    pub fn limits(&self) -> Limits {
        self.bundler.limits()
    }
}

/// A binlog row's values as cells of `shape`'s columns. A row image that
/// lacks columns was written under `binlog_row_image` other than FULL.
fn cells(shape: &Shape, row: Image) -> Result<Vec<Cell>> {
    if row.len() != shape.cols.len() {
        return Err(Error::fatal(
            "binlog_row_image",
            format!(
                "a binlog row carries {} of {} columns: it was written with \
                 binlog_row_image other than FULL; resync once every writer uses FULL",
                row.len(),
                shape.cols.len()
            ),
        ));
    }
    row.iter()
        .zip(&shape.cols)
        .map(|(v, c)| Cell::from_binlog(c.kind, v, &c.names))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_are_told_apart() {
        assert_eq!(statement("BEGIN"), Statement::Begin);
        assert_eq!(statement(" commit ;"), Statement::Commit);
        assert_eq!(
            statement("TRUNCATE TABLE `shop`.`orders`"),
            Statement::Truncate(Some("shop".into()), "orders".into())
        );
        assert_eq!(
            statement("truncate Orders"),
            Statement::Truncate(None, "Orders".into())
        );
        assert_eq!(
            statement("TRUNCATE `we``ird`"),
            Statement::Truncate(None, "we`ird".into())
        );
        assert_eq!(statement("TRUNCATEX t"), Statement::Other);
        assert_eq!(statement("ALTER TABLE t ADD c INT"), Statement::Other);
        assert_eq!(statement("truncate table a b"), Statement::Other);
    }
}
