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
    /// Tables this statement leaves without their name: renamed away and not
    /// renamed back in the same statement (an atomic swap keeps the name),
    /// or dropped. `DROP DATABASE d` is `(Some(d), None)`.
    Gone(Vec<(Option<String>, Option<String>)>),
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    /// A bare word, as written (keywords compare lowercase).
    Word(String),
    /// A backquoted name, unquoted.
    Quoted(String),
    Dot,
    Comma,
    Open,
    Close,
    Other,
}

impl Tok {
    fn is(&self, kw: &str) -> bool {
        matches!(self, Tok::Word(w) if w.eq_ignore_ascii_case(kw))
    }
}

/// The statement's tokens: comments and string literals skipped, names
/// unquoted. Enough to read the DDL that moves tables.
fn tokens(sql: &str) -> Vec<Tok> {
    let mut out = Vec::new();
    let mut it = sql.chars().peekable();
    while let Some(c) = it.next() {
        match c {
            c if c.is_whitespace() => {}
            '/' if it.peek() == Some(&'*') => {
                it.next();
                let mut prev = ' ';
                for c in it.by_ref() {
                    if prev == '*' && c == '/' {
                        break;
                    }
                    prev = c;
                }
            }
            '#' => {
                for c in it.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
            }
            '-' if it.peek() == Some(&'-') => {
                for c in it.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
            }
            '\'' | '"' => {
                let q = c;
                while let Some(c) = it.next() {
                    if c == '\\' {
                        it.next();
                    } else if c == q {
                        if it.peek() == Some(&q) {
                            it.next();
                        } else {
                            break;
                        }
                    }
                }
                out.push(Tok::Other);
            }
            '`' => {
                let mut name = String::new();
                while let Some(c) = it.next() {
                    if c == '`' {
                        if it.peek() == Some(&'`') {
                            it.next();
                            name.push('`');
                        } else {
                            break;
                        }
                    } else {
                        name.push(c);
                    }
                }
                out.push(Tok::Quoted(name));
            }
            '.' => out.push(Tok::Dot),
            ',' => out.push(Tok::Comma),
            '(' => out.push(Tok::Open),
            ')' => out.push(Tok::Close),
            ';' => break,
            c if c.is_alphanumeric() || c == '_' || c == '$' => {
                let mut w = String::from(c);
                while let Some(&n) = it.peek() {
                    if n.is_alphanumeric() || n == '_' || n == '$' {
                        w.push(n);
                        it.next();
                    } else {
                        break;
                    }
                }
                out.push(Tok::Word(w));
            }
            _ => out.push(Tok::Other),
        }
    }
    out
}

/// A table name at `t[i..]`: `name` or `db.name`, and the index after it.
fn table_ref(t: &[Tok], i: usize) -> Option<((Option<String>, String), usize)> {
    let name = |k: usize| match t.get(k) {
        Some(Tok::Word(w)) | Some(Tok::Quoted(w)) => Some(w.clone()),
        _ => None,
    };
    let a = name(i)?;
    if t.get(i + 1) == Some(&Tok::Dot) {
        let b = name(i + 2)?;
        return Some(((Some(a), b), i + 3));
    }
    Some(((None, a), i + 1))
}

/// What a statement says, cheaply: the binlog holds what the client sent.
pub fn statement(sql: &str) -> Statement {
    let t = tokens(sql);
    let first = |kw: &str| t.first().is_some_and(|x| x.is(kw));
    if t.len() == 1 && (first("begin") || first("commit") || first("rollback")) {
        return if first("begin") {
            Statement::Begin
        } else {
            Statement::Commit
        };
    }
    if first("xa") && t.get(1).is_some_and(|x| x.is("start") || x.is("begin")) {
        return Statement::Begin;
    }
    if first("truncate") {
        let at = if t.get(1).is_some_and(|x| x.is("table")) {
            2
        } else {
            1
        };
        return match table_ref(&t, at) {
            Some(((db, name), end)) if end == t.len() => Statement::Truncate(db, name),
            _ => Statement::Other,
        };
    }
    if first("rename") && t.get(1).is_some_and(|x| x.is("table") || x.is("tables")) {
        // RENAME TABLE a TO b, c TO d: what is renamed away and not back.
        let mut moves = Vec::new();
        let mut i = 2;
        loop {
            let Some((from, j)) = table_ref(&t, i) else {
                return Statement::Other;
            };
            if !t.get(j).is_some_and(|x| x.is("to")) {
                return Statement::Other;
            }
            let Some((to, k)) = table_ref(&t, j + 1) else {
                return Statement::Other;
            };
            moves.push((from, to));
            if t.get(k) == Some(&Tok::Comma) {
                i = k + 1;
            } else {
                break;
            }
        }
        return gone_after_moves(&moves);
    }
    if first("alter") {
        // ALTER [ONLINE] [IGNORE] TABLE t …, RENAME [TO|AS] u, …
        let Some(at) = t.iter().take(4).position(|x| x.is("table")) else {
            return Statement::Other;
        };
        let Some((from, mut i)) = table_ref(&t, at + 1) else {
            return Statement::Other;
        };
        let mut depth = 0i32;
        let mut clause_start = true;
        while i < t.len() {
            match &t[i] {
                Tok::Open => depth += 1,
                Tok::Close => depth -= 1,
                Tok::Comma if depth == 0 => {
                    clause_start = true;
                    i += 1;
                    continue;
                }
                x if clause_start && depth == 0 && x.is("rename") => {
                    let mut j = i + 1;
                    if t.get(j)
                        .is_some_and(|x| x.is("column") || x.is("index") || x.is("key"))
                    {
                        clause_start = false;
                        i += 1;
                        continue;
                    }
                    if t.get(j).is_some_and(|x| x.is("to") || x.is("as")) {
                        j += 1;
                    }
                    if let Some((to, _)) = table_ref(&t, j) {
                        return gone_after_moves(&[(from, to)]);
                    }
                }
                _ => {}
            }
            clause_start = false;
            i += 1;
        }
        return Statement::Other;
    }
    if first("drop") {
        let mut i = 1;
        if t.get(i).is_some_and(|x| x.is("temporary")) {
            // A temporary table never reaches the source.
            return Statement::Other;
        }
        if t.get(i).is_some_and(|x| x.is("database") || x.is("schema")) {
            i += 1;
            if t.get(i).is_some_and(|x| x.is("if")) && t.get(i + 1).is_some_and(|x| x.is("exists"))
            {
                i += 2;
            }
            return match table_ref(&t, i) {
                Some(((None, db), _)) => Statement::Gone(vec![(Some(db), None)]),
                _ => Statement::Other,
            };
        }
        if !t.get(i).is_some_and(|x| x.is("table") || x.is("tables")) {
            return Statement::Other;
        }
        i += 1;
        if t.get(i).is_some_and(|x| x.is("if")) && t.get(i + 1).is_some_and(|x| x.is("exists")) {
            i += 2;
        }
        let mut gone = Vec::new();
        while let Some(((db, name), j)) = table_ref(&t, i) {
            gone.push((db, Some(name)));
            if t.get(j) == Some(&Tok::Comma) {
                i = j + 1;
            } else {
                break;
            }
        }
        return if gone.is_empty() {
            Statement::Other
        } else {
            Statement::Gone(gone)
        };
    }
    Statement::Other
}

type Name = (Option<String>, String);

/// Renames applied in order: the names that end up without a table.
fn gone_after_moves(moves: &[(Name, Name)]) -> Statement {
    let mut present: Vec<Name> = Vec::new();
    let mut moved: Vec<Name> = Vec::new();
    for (from, to) in moves {
        present.retain(|n| n != from);
        if !moved.contains(from) {
            moved.push(from.clone());
        }
        if !present.contains(to) {
            present.push(to.clone());
        }
    }
    let gone: Vec<(Option<String>, Option<String>)> = moved
        .into_iter()
        .filter(|n| !present.contains(n))
        .map(|(db, name)| (db, Some(name)))
        .collect();
    if gone.is_empty() {
        Statement::Other
    } else {
        Statement::Gone(gone)
    }
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
            Statement::Gone(names) => {
                if !txn.covered {
                    self.check_gone(schema, &names, sql)?;
                }
                self.on_commit(now)
            }
            Statement::Other => self.on_commit(now),
        }
    }

    /// A statement left configured tables without their name: the source
    /// would read nothing more from them and say nothing. It stops instead,
    /// naming the statement, until the document follows the change.
    fn check_gone(
        &self,
        schema: &str,
        names: &[(Option<String>, Option<String>)],
        sql: &str,
    ) -> Result<()> {
        let mut hit: Vec<&str> = Vec::new();
        for (db, name) in names {
            let db = db.as_deref().unwrap_or(schema);
            match name {
                Some(n) => {
                    if let Some(i) = self.table_of(db, n) {
                        hit.push(&self.tables[i].name);
                    }
                }
                None => hit.extend(
                    self.tables
                        .iter()
                        .filter(|t| t.tn.db == db || t.tn.db.eq_ignore_ascii_case(db))
                        .map(|t| t.name.as_str()),
                ),
            }
        }
        if hit.is_empty() {
            return Ok(());
        }
        let mut stmt: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        if stmt.len() > 200 {
            let cut = (0..=200)
                .rev()
                .find(|&i| stmt.is_char_boundary(i))
                .unwrap_or(0);
            stmt.truncate(cut);
            stmt.push('…');
        }
        Err(Error::fatal(
            "table_gone",
            format!(
                "the binlog renames or drops {} (`{stmt}`): the source would read nothing \
                 more from it; name the table it became in source.tables, remove it, or \
                 POST …/resync once a table of that name is back",
                hit.join(", ")
            ),
        ))
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
        let (units, end) = chunk_units(
            &chunk,
            &chunk.t,
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
        assert_eq!(statement("XA START 'x1'"), Statement::Begin);
        assert_eq!(statement("ROLLBACK TO SAVEPOINT s"), Statement::Other);
    }

    fn gone(v: &[(Option<&str>, Option<&str>)]) -> Statement {
        Statement::Gone(
            v.iter()
                .map(|(d, n)| (d.map(str::to_string), n.map(str::to_string)))
                .collect(),
        )
    }

    #[test]
    fn renames_and_drops_that_leave_a_name_empty() {
        assert_eq!(
            statement("RENAME TABLE `orders` TO `orders_old`"),
            gone(&[(None, Some("orders"))])
        );
        // An atomic swap (gh-ost, pt-online-schema-change) keeps the name.
        assert_eq!(
            statement("RENAME TABLE orders TO _orders_del, _orders_gho TO orders"),
            gone(&[(None, Some("_orders_gho"))])
        );
        assert_eq!(
            statement("rename table shop.a to shop.b, shop.b to shop.c"),
            gone(&[(Some("shop"), Some("a")), (Some("shop"), Some("b"))])
        );
        assert_eq!(
            statement("ALTER TABLE shop.orders ADD COLUMN x INT DEFAULT (1), RENAME TO archive"),
            gone(&[(Some("shop"), Some("orders"))])
        );
        assert_eq!(
            statement("ALTER TABLE orders RENAME COLUMN a TO b"),
            Statement::Other
        );
        assert_eq!(
            statement("ALTER TABLE orders RENAME INDEX i TO j"),
            Statement::Other
        );
        assert_eq!(
            statement("DROP TABLE IF EXISTS `a`,`shop`.`b` /* generated by server */"),
            gone(&[(None, Some("a")), (Some("shop"), Some("b"))])
        );
        assert_eq!(statement("DROP TEMPORARY TABLE t"), Statement::Other);
        assert_eq!(
            statement("DROP DATABASE IF EXISTS shop"),
            gone(&[(Some("shop"), None)])
        );
        assert_eq!(statement("DROP INDEX i ON t"), Statement::Other);
        assert_eq!(
            statement("/* a comment */ DROP -- why\n TABLE t # more\n"),
            gone(&[(None, Some("t"))])
        );
    }
}
