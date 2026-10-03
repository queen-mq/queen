//! The engine's pure core: pgoutput messages in, units out (PLAN §4.4–4.6).
//! No I/O here — the engine feeds it the stream, the snapshot reads and the
//! clock, and sends what [`Pipeline::take`] hands out — so every rule of the
//! stream is testable without a database.
//!
//! The pipeline keeps the **tail**: the pointer position once everything it
//! queued is in Queen. Every unit's cover is computed from it, so positions
//! only move forward and a bundle (a prefix of the queue) always writes a
//! position its contents justify.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::time::Instant;

use crate::config::TruncatePolicy;
use crate::error::{Error, Result};
use crate::pg::catalog::quote_ident;
use crate::repl::pgoutput::{Datum, OldTuple, Relation, Tuple};
use crate::repl::Lsn;

use super::bundle::{Bundle, Bundler, Limits, Unit};
use super::events::{
    key_code, stream_txn_id, truncate_event, vals_of, Change, Eff, Head, Item, Shape, TypeConv,
};
use super::pointer::{InTxn, Position, SnapshotProgress};
use super::snapshot::{self, chunk_units, Chunk, SnapState, Touch};
use super::table::{self, TableState};

/// The source transaction being decoded.
#[derive(Debug)]
struct Txn {
    xid: u32,
    commit_lsn: Lsn,
    ts_us: i64,
    /// Raw changes decoded so far — every pgoutput Insert, Update, Delete and
    /// Truncate of the transaction, mapped or not, in decode order — which
    /// is the index of the next one. It depends on the WAL alone, never on
    /// the configuration, so `inTxn.done` (a raw index) means the same thing
    /// after a restart under another `partitionBy`, `onTruncate` or table
    /// set. It is also the event's `seq`.
    raw: u64,
    /// Changes with a raw index below this are already in Queen (a split
    /// transaction resumed after a restart, `inTxn.done`).
    skip: u64,
    /// The whole transaction is already in Queen (its commit is before the
    /// position): decode it, emit nothing.
    covered: bool,
    pending: Vec<Item>,
    pending_bytes: usize,
    events: u64,
    touched: Vec<(usize, Option<String>, Eff)>,
}

/// What a logical message meant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Note {
    /// Not ours, or a watermark of no chunk in flight.
    Ignored,
    /// Our heartbeat, at this LSN: everything before it was received.
    Heartbeat(Lsn),
    /// The low watermark of the chunk in flight.
    Low,
    /// The high watermark: the chunk was queued (`rows` events).
    High { rows: usize, finished: bool },
}

/// A read-back for a key change ([`Pipeline::read_back`]): the statement,
/// and the tuple positions its columns go to, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadBack {
    pub sql: String,
    pub positions: Vec<usize>,
}

/// The tuple with a read-back's answer put in (`row`: the values in
/// `positions` order, `None` when the row is gone — the columns then stay
/// unchanged).
pub fn patch(mut new: Tuple, rb: &ReadBack, row: Option<Vec<Option<String>>>) -> Tuple {
    if let Some(row) = row {
        for (j, &p) in rb.positions.iter().enumerate() {
            if let Some(d) = new.0.get_mut(p) {
                *d = match row.get(j).cloned().flatten() {
                    Some(v) => Datum::Text(bytes::Bytes::from(v)),
                    None => Datum::Null,
                };
            }
        }
    }
    new
}

#[derive(Debug)]
pub struct Pipeline {
    pub epoch: String,
    prefix: String,
    pub tables: Arc<Vec<TableState>>,
    shapes: HashMap<u32, Shape>,
    unmapped: HashSet<u32>,
    txn: Option<Txn>,
    pub bundler: Bundler,
    pub tail: Position,
    pub snap: Option<SnapState>,
    /// Indices of the tables the snapshot has not finished.
    pending_tables: HashSet<usize>,
    on_truncate: TruncatePolicy,
    pub truncates_skipped: u64,
}

impl Pipeline {
    pub fn new(
        epoch: String,
        tables: Arc<Vec<TableState>>,
        start: Position,
        limits: Limits,
        on_truncate: TruncatePolicy,
        first_chunk_n: u64,
    ) -> Pipeline {
        let snap = start
            .snapshot
            .clone()
            .map(|p| SnapState::new(p, first_chunk_n));
        let mut p = Pipeline {
            prefix: snapshot::prefix(&epoch),
            epoch,
            tables,
            shapes: HashMap::new(),
            unmapped: HashSet::new(),
            txn: None,
            bundler: Bundler::new(limits),
            tail: start,
            snap,
            pending_tables: HashSet::new(),
            on_truncate,
            truncates_skipped: 0,
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

    pub fn in_txn(&self) -> bool {
        self.txn.is_some()
    }

    pub fn on_relation(&mut self, rel: &Relation, conv: &HashMap<u32, TypeConv>) -> Result<()> {
        let found = self
            .tables
            .iter()
            .position(|t| t.oid == rel.id)
            .or_else(|| {
                self.tables
                    .iter()
                    .position(|t| t.table.schema == rel.namespace && t.table.name == rel.name)
            });
        match found {
            Some(i) => {
                let s = table::shape(i, &self.tables[i], rel, conv)?;
                self.unmapped.remove(&rel.id);
                self.shapes.insert(rel.id, s);
            }
            None => {
                self.shapes.remove(&rel.id);
                self.unmapped.insert(rel.id);
            }
        }
        Ok(())
    }

    pub fn on_begin(&mut self, final_lsn: Lsn, ts_us: i64, xid: u32) -> Result<()> {
        if self.txn.is_some() {
            return Err(Error::io("pgoutput: BEGIN inside a transaction"));
        }
        let skip = match &self.tail.in_txn {
            Some(t) if t.commit_lsn == final_lsn => t.done,
            _ => 0,
        };
        self.txn = Some(Txn {
            xid,
            commit_lsn: final_lsn,
            ts_us,
            raw: 0,
            skip,
            covered: final_lsn < self.tail.lsn,
            pending: Vec::new(),
            pending_bytes: 0,
            events: 0,
            touched: Vec::new(),
        });
        Ok(())
    }

    pub fn on_insert(&mut self, relid: u32, new: &Tuple, now: Instant) -> Result<()> {
        self.change(relid, now, |s| s.insert(new))
    }

    pub fn on_update(
        &mut self,
        relid: u32,
        old: Option<&OldTuple>,
        new: &Tuple,
        now: Instant,
    ) -> Result<()> {
        self.change(relid, now, |s| s.update(old, new))
    }

    pub fn on_delete(&mut self, relid: u32, old: &OldTuple, now: Instant) -> Result<()> {
        self.change(relid, now, |s| s.delete(old))
    }

    /// The next raw change: its index, and whether it is to be emitted (not
    /// covered, not already in Queen from before a restart).
    fn next_raw(&mut self, what: &str) -> Result<(u64, bool)> {
        let Some(txn) = &mut self.txn else {
            return Err(Error::io(format!("pgoutput: {what} outside a transaction")));
        };
        let raw = txn.raw;
        txn.raw += 1;
        Ok((raw, !txn.covered && raw >= txn.skip))
    }

    fn change(&mut self, relid: u32, now: Instant, f: impl FnOnce(&Shape) -> Change) -> Result<()> {
        let (raw, emit) = self.next_raw("a change")?;
        if self.txn.as_ref().is_some_and(|t| t.covered) {
            return Ok(());
        }
        let Some(shape) = self.shapes.get(&relid) else {
            if self.unmapped.contains(&relid) {
                return Ok(());
            }
            return Err(Error::io(format!(
                "pgoutput: a change of relation {relid} before its Relation message"
            )));
        };
        let idx = shape.table;
        let snapshotting = self.pending_tables.contains(&idx);
        if !emit && !snapshotting {
            return Ok(());
        }
        let ch = f(shape);
        if snapshotting {
            if let Some(txn) = &mut self.txn {
                txn.touched
                    .extend(ch.touched.into_iter().map(|(k, e)| (idx, Some(k), e)));
            }
        }
        if !emit {
            return Ok(());
        }
        let items: Vec<Item> = ch
            .events
            .into_iter()
            .enumerate()
            .filter_map(|(k, (partition, body))| {
                self.item(idx, partition, raw, k, |h| body.render(h))
            })
            .collect();
        self.push_change(items, raw, now);
        Ok(())
    }

    /// A key change whose new tuple left columns unchanged (TOAST, not under
    /// `REPLICA IDENTITY FULL`, whose old row fills them): the `c` for the
    /// new key would carry no value for them, and the consumer of the new
    /// key has no row to keep them from. The engine reads them back for the
    /// NEW key with this statement (simple query protocol: the same output
    /// text pgoutput sends) and puts them into the tuple before decoding it.
    /// `None` when no read-back is needed (or the change will not be
    /// emitted).
    ///
    /// The value read back is the row's value NOW, which can be newer than
    /// this change: the final state is exact, an intermediate state may show
    /// a later value for those columns. When the row is gone the columns
    /// stay `unchanged` (a later delete follows in the stream).
    pub fn read_back(&self, relid: u32, old: Option<&OldTuple>, new: &Tuple) -> Option<ReadBack> {
        let txn = self.txn.as_ref()?;
        if txn.covered || txn.raw < txn.skip {
            return None;
        }
        let shape = self.shapes.get(&relid)?;
        let Some(OldTuple::Key(old)) = old else {
            return None;
        };
        let positions: Vec<usize> = new
            .0
            .iter()
            .enumerate()
            .filter(|(i, d)| matches!(d, Datum::Unchanged) && !shape.key.contains(i))
            .map(|(i, _)| i)
            .filter(|&i| i < shape.cols.len())
            .collect();
        if positions.is_empty() {
            return None;
        }
        let n = vals_of(new, shape.cols.len());
        if key_code(&n, &shape.key) == key_code(&vals_of(old, shape.cols.len()), &shape.key) {
            return None;
        }
        let t = &self.tables[shape.table];
        let mut keys = Vec::with_capacity(shape.key.len());
        let mut lits = Vec::with_capacity(shape.key.len());
        for &k in &shape.key {
            let name = &shape.cols[k].name;
            let p = t.cols.iter().position(|c| c.name == *name)?;
            keys.push(quote_ident(name));
            lits.push(format!(
                "{}::{}",
                snapshot::literal(n.get(k)?.text()?),
                t.type_names[p]
            ));
        }
        let cols: Vec<String> = positions
            .iter()
            .map(|&i| quote_ident(&shape.cols[i].name))
            .collect();
        Some(ReadBack {
            sql: format!(
                "SELECT {} FROM {} WHERE ({}) = ({})",
                cols.join(", "),
                t.table.quoted(),
                keys.join(", "),
                lits.join(", ")
            ),
            positions,
        })
    }

    pub fn on_truncate(&mut self, options: u8, relids: &[u32], now: Instant) -> Result<()> {
        let (raw, emit) = self.next_raw("TRUNCATE")?;
        if self.txn.as_ref().is_some_and(|t| t.covered) {
            return Ok(());
        }
        let mut idxs: Vec<usize> = relids
            .iter()
            .filter_map(|r| self.shapes.get(r).map(|s| s.table))
            .collect();
        idxs.sort_unstable();
        idxs.dedup();
        if idxs.is_empty() {
            return Ok(());
        }
        if let Some(txn) = &mut self.txn {
            for &i in &idxs {
                if self.pending_tables.contains(&i) {
                    txn.touched.push((i, None, Eff::Delete));
                }
            }
        }
        if !emit {
            return Ok(());
        }
        match self.on_truncate {
            TruncatePolicy::Skip => {
                self.truncates_skipped += 1;
                let names: Vec<&str> = idxs.iter().map(|&i| self.tables[i].name.as_str()).collect();
                tracing::info!(
                    target: crate::LOG_TARGET,
                    tables = ?names,
                    "TRUNCATE not emitted (onTruncate: skip)"
                );
            }
            TruncatePolicy::Emit => {
                // One `t` per queue, partition `all` (emit requires `single`
                // on every table), listing that queue's tables.
                let mut queues: Vec<Arc<str>> = Vec::new();
                for &i in &idxs {
                    if !queues.contains(&self.tables[i].queue) {
                        queues.push(self.tables[i].queue.clone());
                    }
                }
                let mut items = Vec::with_capacity(queues.len());
                for (k, q) in queues.iter().enumerate() {
                    let names: Vec<&str> = idxs
                        .iter()
                        .filter(|&&i| self.tables[i].queue == *q)
                        .map(|&i| self.tables[i].name.as_str())
                        .collect();
                    let first = idxs
                        .iter()
                        .copied()
                        .find(|&i| self.tables[i].queue == *q)
                        .unwrap_or(0);
                    if let Some(it) = self.item(first, "all".to_string(), raw, k, |h| {
                        truncate_event(h, &names, options & 1 != 0, options & 2 != 0)
                    }) {
                        items.push(it);
                    }
                }
                self.push_change(items, raw, now);
            }
        }
        Ok(())
    }

    /// The `k`-th event of raw change `raw`: `seq` is the raw index (a key
    /// move's `d` and `c` share it), the id `pg:<epoch>:<commit>:<raw>`, and
    /// `…:<raw>.<k>` for the second and later events of one change.
    fn item(
        &self,
        table: usize,
        partition: String,
        raw: u64,
        k: usize,
        render: impl FnOnce(&Head<'_>) -> String,
    ) -> Option<Item> {
        let t = &self.tables[table];
        let txn = self.txn.as_ref()?;
        let payload = render(&Head {
            op: "c",
            table: &t.name,
            lsn: txn.commit_lsn,
            xid: Some(txn.xid),
            ts_us: txn.ts_us,
            seq: raw,
        });
        Some(Item {
            queue: t.queue.clone(),
            partition,
            txn_id: stream_txn_id(&self.epoch, txn.commit_lsn, raw, k),
            payload,
        })
    }

    /// Queue the events of raw change `raw`. A transaction that outgrew the
    /// bundle limits is cut into a chunk BEFORE this change — never inside
    /// it, so a chunk's `inTxn.done` (the raw index it ends at) says exactly
    /// which changes are in Queen.
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
            let cover = Position {
                lsn: self.tail.lsn,
                in_txn: Some(InTxn {
                    commit_lsn: txn.commit_lsn,
                    done: raw,
                }),
                snapshot: self.tail.snapshot.clone(),
            };
            let chunk = std::mem::take(&mut txn.pending);
            txn.pending_bytes = 0;
            self.bundler.push(Unit::new(chunk, cover.clone()), now);
            self.tail = cover;
        }
        txn.events += items.len() as u64;
        txn.pending_bytes += len;
        txn.pending.extend(items);
    }

    pub fn on_commit(&mut self, end_lsn: Lsn, now: Instant) -> Result<()> {
        let Some(txn) = self.txn.take() else {
            return Err(Error::io("pgoutput: COMMIT outside a transaction"));
        };
        if txn.covered || end_lsn <= self.tail.lsn {
            return Ok(());
        }
        if let Some(s) = &mut self.snap {
            s.on_commit(Touch {
                xid: txn.xid,
                keys: txn.touched,
            });
        }
        let in_txn = match &self.tail.in_txn {
            Some(t) if t.commit_lsn > txn.commit_lsn => Some(t.clone()),
            _ => None,
        };
        let cover = Position {
            lsn: end_lsn,
            in_txn,
            snapshot: self.tail.snapshot.clone(),
        };
        let mut u = Unit::new(txn.pending, cover.clone());
        if txn.events > 0 || txn.skip > 0 {
            u.txns = 1;
            u.last_commit_us = Some(txn.ts_us);
        }
        self.bundler.push(u, now);
        self.tail = cover;
        Ok(())
    }

    /// A logical message. Ours are non-transactional, prefixed
    /// `queen:<epoch>`; everything else is somebody else's.
    pub fn on_message(
        &mut self,
        transactional: bool,
        lsn: Lsn,
        prefix: &str,
        content: &[u8],
        now: Instant,
    ) -> Note {
        if transactional || prefix != self.prefix || self.txn.is_some() {
            return Note::Ignored;
        }
        if content == b"hb" {
            return Note::Heartbeat(lsn);
        }
        let Some((high, n)) = snapshot::parse_watermark(content) else {
            return Note::Ignored;
        };
        let Some(s) = &mut self.snap else {
            return Note::Ignored;
        };
        if !high {
            let had = s.chunk.as_ref().is_some_and(|c| c.n == n && !c.window_open);
            s.on_lw(n);
            return if had { Note::Low } else { Note::Ignored };
        }
        let Some(chunk) = s.on_hw(n) else {
            return Note::Ignored;
        };
        let hw = lsn.max(self.tail.lsn);
        let t = &self.tables[chunk.table];
        let (units, end) = chunk_units(
            &chunk,
            t,
            &self.epoch,
            hw,
            &s.progress,
            &self.bundler.limits(),
        );
        let rows = chunk.surviving();
        for u in units {
            self.tail = u.cover.clone();
            self.bundler.push(u, now);
        }
        let finished = end.is_none();
        match end {
            Some(p) => s.progress = p,
            None => self.snap = None,
        }
        self.refresh_pending();
        Note::High { rows, finished }
    }

    /// The snapshot's next chunk: `(table index, after)` when one may be read
    /// now (a snapshot runs, no chunk in flight). A table of the run that is
    /// no longer configured is skipped (its progress queued).
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

    fn advance_snapshot(&mut self, next: Option<SnapshotProgress>, now: Instant) {
        let cover = Position {
            lsn: self.tail.lsn,
            in_txn: self.tail.in_txn.clone(),
            snapshot: next.clone(),
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

    /// A chunk was read (between its watermarks' emission).
    pub fn install_chunk(&mut self, c: Chunk) {
        if let Some(s) = &mut self.snap {
            s.install(c);
        }
    }

    /// A chunk read failed after its number was taken: forget it, so its
    /// watermarks are ignored and the next read starts over.
    pub fn abandon_chunk(&mut self) {
        if let Some(s) = &mut self.snap {
            s.chunk = None;
        }
    }

    /// Idle advancement (PLAN §4.4 "Idle"): the server says everything before
    /// `wal_end` has been sent. When nothing is open, queued or split, the
    /// position may jump there — a unit with no events, forced out.
    pub fn idle(&mut self, wal_end: Lsn, now: Instant) -> bool {
        if self.txn.is_some()
            || !self.bundler.is_empty()
            || self.tail.in_txn.is_some()
            || wal_end <= self.tail.lsn
        {
            return false;
        }
        let cover = Position {
            lsn: wal_end,
            in_txn: None,
            snapshot: self.tail.snapshot.clone(),
        };
        let mut u = Unit::new(Vec::new(), cover.clone());
        u.force = true;
        self.bundler.push(u, now);
        self.tail = cover;
        true
    }

    /// The next bundle, when one should go: the bundler says so, or the
    /// stream has nothing more right now (`stream_idle`) and events wait.
    pub fn take(&mut self, now: Instant, stream_idle: bool) -> Option<Bundle> {
        if self.bundler.due(now) || (stream_idle && self.bundler.has_items()) {
            self.bundler.take()
        } else {
            None
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::{PartitionBy, PartitionMode, TableSpec};
    use crate::pg::catalog::{ColumnInfo, TableInfo, TableName};
    use crate::repl::pgoutput::{Datum, RelColumn};
    use crate::source::snapshot::Visibility;
    use bytes::Bytes;
    use std::time::Duration;

    pub(crate) fn t(s: &str) -> Datum {
        Datum::Text(Bytes::copy_from_slice(s.as_bytes()))
    }

    fn info(oid: u32, name: &str) -> TableInfo {
        let col = |n: &str, attnum: i16, oid: u32, ty: &str| ColumnInfo {
            name: n.into(),
            attnum,
            type_oid: oid,
            type_name: ty.into(),
            not_null: true,
            generated: false,
        };
        TableInfo {
            oid,
            name: TableName {
                schema: "public".into(),
                name: name.into(),
            },
            columns: vec![col("id", 1, 20, "bigint"), col("v", 2, 25, "text")],
            primary_key: vec!["id".into()],
            replica_identity: b'd',
            identity_index: vec![],
        }
    }

    pub(crate) fn tables(single: bool) -> Arc<Vec<TableState>> {
        let mk = |oid: u32, name: &str, queue: &str| {
            table::resolve(
                &TableSpec {
                    table: format!("public.{name}"),
                    queue: queue.into(),
                    partition_by: if single {
                        PartitionBy::Mode(PartitionMode::Single)
                    } else {
                        PartitionBy::Default
                    },
                },
                &info(oid, name),
                None,
                &HashMap::new(),
            )
            .unwrap()
        };
        Arc::new(vec![mk(100, "a", "qa"), mk(200, "b", "qb")])
    }

    pub(crate) fn rel(oid: u32, name: &str) -> Relation {
        Relation {
            id: oid,
            namespace: "public".into(),
            name: name.into(),
            replica_identity: b'd',
            columns: vec![
                RelColumn {
                    key: true,
                    name: "id".into(),
                    type_oid: 20,
                    type_mod: -1,
                },
                RelColumn {
                    key: false,
                    name: "v".into(),
                    type_oid: 25,
                    type_mod: -1,
                },
            ],
        }
    }

    fn limits(m: usize) -> Limits {
        Limits {
            max_messages: m,
            max_bytes: 1 << 20,
            linger: Duration::from_millis(20),
        }
    }

    fn pipe(start: Position, m: usize) -> Pipeline {
        let mut p = Pipeline::new(
            "0badf00d".into(),
            tables(false),
            start,
            limits(m),
            TruncatePolicy::Skip,
            1000,
        );
        p.on_relation(&rel(100, "a"), &HashMap::new()).unwrap();
        p.on_relation(&rel(200, "b"), &HashMap::new()).unwrap();
        p
    }

    fn at(lsn: u64) -> Position {
        Position {
            lsn: Lsn(lsn),
            in_txn: None,
            snapshot: None,
        }
    }

    /// One transaction: BEGIN(final), n inserts into `a`, COMMIT(end).
    pub(crate) fn txn(
        p: &mut Pipeline,
        commit: u64,
        end: u64,
        ids: std::ops::Range<u64>,
        now: Instant,
    ) {
        p.on_begin(Lsn(commit), 1_790_935_200_000_000, 7).unwrap();
        for i in ids {
            p.on_insert(100, &Tuple(vec![t(&i.to_string()), t("x")]), now)
                .unwrap();
        }
        p.on_commit(Lsn(end), now).unwrap();
    }

    fn ids(b: &Bundle) -> Vec<String> {
        b.items.iter().map(|i| i.txn_id.clone()).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn whole_transactions_in_commit_order_with_their_end_lsn() {
        let now = Instant::now();
        let mut p = pipe(at(0x100), 100);
        txn(&mut p, 0x200, 0x210, 0..2, now);
        txn(&mut p, 0x300, 0x310, 2..3, now);
        assert!(
            p.take(now, false).is_none(),
            "nothing due: below limits, linger not passed"
        );
        let b = p.take(now, true).expect("the stream is idle: flush");
        assert_eq!(
            ids(&b),
            vec![
                "pg:0badf00d:0000000000000200:0",
                "pg:0badf00d:0000000000000200:1",
                "pg:0badf00d:0000000000000300:0"
            ]
        );
        assert_eq!(b.cover, at(0x310));
        assert_eq!(b.txns, 2);
        assert_eq!(b.items[0].queue.as_ref(), "qa");
        assert_eq!(b.items[0].partition, "0");
        let ev: serde_json::Value = serde_json::from_str(&b.items[1].payload).unwrap();
        assert_eq!(ev["op"], "c");
        assert_eq!(ev["lsn"], "0/200");
        assert_eq!(ev["xid"], 7);
        assert_eq!(ev["seq"], 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_transaction_already_in_queen_is_dropped() {
        let now = Instant::now();
        let mut p = pipe(at(0x310), 100);
        txn(&mut p, 0x200, 0x210, 0..2, now);
        txn(&mut p, 0x300, 0x310, 2..3, now);
        assert!(
            p.bundler.is_empty(),
            "end LSN <= the pointer: already in Queen"
        );
        txn(&mut p, 0x400, 0x410, 3..4, now);
        assert_eq!(p.take(now, true).unwrap().items.len(), 1);
    }

    /// PLAN §4.4 "Split transactions": chunks as changes arrive, `inTxn` on
    /// every chunk but the last, the position unchanged until the end.
    #[tokio::test(start_paused = true)]
    async fn a_big_transaction_is_split_into_chunks_with_in_txn() {
        let now = Instant::now();
        let mut p = pipe(at(0x100), 10);
        txn(&mut p, 0x900, 0x910, 0..25, now);
        let b1 = p.take(now, false).unwrap();
        assert_eq!(b1.items.len(), 10);
        assert_eq!(
            b1.cover,
            Position {
                lsn: Lsn(0x100),
                in_txn: Some(InTxn {
                    commit_lsn: Lsn(0x900),
                    done: 10
                }),
                snapshot: None
            }
        );
        let b2 = p.take(now, false).unwrap();
        assert_eq!(b2.cover.in_txn.as_ref().unwrap().done, 20);
        let b3 = p.take(now, true).unwrap();
        assert_eq!(b3.items.len(), 5);
        assert_eq!(b3.cover, at(0x910));
        assert_eq!(b3.txns, 1);
        assert_eq!(ids(&b3)[0], "pg:0badf00d:0000000000000900:20");
    }

    /// The restart of a split transaction: decoded again from its start, its
    /// first `done` events dropped, the ids continuing where they were.
    #[tokio::test(start_paused = true)]
    async fn a_resumed_split_transaction_skips_what_is_done() {
        let now = Instant::now();
        let start = Position {
            lsn: Lsn(0x100),
            in_txn: Some(InTxn {
                commit_lsn: Lsn(0x900),
                done: 20,
            }),
            snapshot: None,
        };
        let mut p = pipe(start, 10);
        // An idle keepalive while the split transaction is pending must not
        // move the position past it.
        assert!(!p.idle(Lsn(0x2000), now));
        txn(&mut p, 0x900, 0x910, 0..25, now);
        let b = p.take(now, true).unwrap();
        assert_eq!(ids(&b).len(), 5);
        assert_eq!(ids(&b)[0], "pg:0badf00d:0000000000000900:20");
        assert_eq!(b.cover, at(0x910));
    }

    /// A split transaction stopped mid-way and resumed under ANOTHER
    /// `partitionBy`: `inTxn.done` counts raw changes, so every change is in
    /// Queen exactly once — those before the stop as the old configuration
    /// rendered them, the rest as the new one does — and no id repeats.
    #[tokio::test(start_paused = true)]
    async fn a_split_transaction_resumed_under_another_partition_by_lands_every_change_once() {
        let now = Instant::now();
        // 20 changes: even ones move their key (a `d` and a `c` partitioned
        // by key, one `u` under `single`), odd ones insert.
        let feed = |p: &mut Pipeline| {
            p.on_begin(Lsn(0x900), 1_790_935_200_000_000, 7).unwrap();
            for i in 0..20u64 {
                if i % 2 == 0 {
                    let old = OldTuple::Key(Tuple(vec![t(&(1000 + i).to_string()), Datum::Null]));
                    p.on_update(
                        100,
                        Some(&old),
                        &Tuple(vec![t(&i.to_string()), t("x")]),
                        now,
                    )
                    .unwrap();
                } else {
                    p.on_insert(100, &Tuple(vec![t(&i.to_string()), t("x")]), now)
                        .unwrap();
                }
            }
            p.on_commit(Lsn(0x910), now).unwrap();
        };
        let mut by_key = pipe(at(0x100), 7);
        feed(&mut by_key);
        let first = by_key.take(now, false).unwrap();
        let done = first.cover.in_txn.clone().unwrap().done;
        assert_eq!(
            (done, first.items.len()),
            (4, 6),
            "changes 0..4 are 6 events; change 4's pair would pass 7 and is never split"
        );
        assert_eq!(
            first.items[0].partition, "1000",
            "the `d` to the old key's partition"
        );
        assert_eq!(first.items[1].partition, "0", "the `c` to the new one");
        assert_eq!(first.items[1].txn_id, "pg:0badf00d:0000000000000900:0.1");

        // The stop: only `first` is in Queen. The restart runs `single`.
        let mut single = Pipeline::new(
            "0badf00d".into(),
            tables(true),
            first.cover.clone(),
            limits(7),
            TruncatePolicy::Skip,
            1,
        );
        single.on_relation(&rel(100, "a"), &HashMap::new()).unwrap();
        single.on_relation(&rel(200, "b"), &HashMap::new()).unwrap();
        feed(&mut single);
        let mut rest = Vec::new();
        while let Some(b) = single.take(now, true) {
            rest.push(b);
        }
        assert_eq!(rest.last().unwrap().cover, at(0x910));
        let seq = |i: &Item| {
            serde_json::from_str::<serde_json::Value>(&i.payload).unwrap()["seq"]
                .as_u64()
                .unwrap()
        };
        let mut before: Vec<u64> = first.items.iter().map(seq).collect();
        before.dedup();
        let after: Vec<&Item> = rest.iter().flat_map(|b| b.items.iter()).collect();
        assert_eq!(before, (0..done).collect::<Vec<_>>());
        assert_eq!(
            after.iter().map(|i| seq(i)).collect::<Vec<_>>(),
            (done..20).collect::<Vec<_>>(),
            "every remaining change once, none of the first chunk's again"
        );
        assert!(after.iter().all(|i| i.partition == "all"));
        let ids: HashSet<&str> = first
            .items
            .iter()
            .chain(after.iter().copied())
            .map(|i| i.txn_id.as_str())
            .collect();
        assert_eq!(ids.len(), first.items.len() + after.len(), "no id repeats");
    }

    /// A key change whose new tuple left `v` unchanged: read `v` back for the
    /// NEW key; a change of another kind, under FULL, or not to be emitted
    /// needs none.
    #[tokio::test(start_paused = true)]
    async fn a_key_change_with_unchanged_toast_reads_it_back() {
        let now = Instant::now();
        let start = Position {
            lsn: Lsn(0x100),
            in_txn: Some(InTxn {
                commit_lsn: Lsn(0x900),
                done: 1,
            }),
            snapshot: None,
        };
        let mut p = pipe(start, 100);
        let new = Tuple(vec![t("3"), Datum::Unchanged]);
        let old_key = OldTuple::Key(Tuple(vec![t("2"), Datum::Null]));
        assert!(
            p.read_back(100, Some(&old_key), &new).is_none(),
            "no transaction open"
        );
        p.on_begin(Lsn(0x900), 0, 1).unwrap();
        assert!(
            p.read_back(100, Some(&old_key), &new).is_none(),
            "change 0 is already in Queen (inTxn.done 1)"
        );
        p.on_insert(100, &Tuple(vec![t("9"), t("x")]), now).unwrap();
        let rb = p.read_back(100, Some(&old_key), &new).unwrap();
        assert_eq!(
            rb.sql,
            "SELECT \"v\" FROM \"public\".\"a\" WHERE (\"id\") = (E'3'::bigint)"
        );
        assert_eq!(rb.positions, vec![1]);
        let filled = patch(new.clone(), &rb, Some(vec![Some("big value".into())]));
        assert_eq!(filled.0[1], t("big value"));
        assert_eq!(patch(new.clone(), &rb, Some(vec![None])).0[1], Datum::Null);
        assert_eq!(
            patch(new.clone(), &rb, None).0[1],
            Datum::Unchanged,
            "the row is gone: stays unchanged (a delete follows)"
        );
        // Not a key change, nothing unchanged, a FULL old row: no read-back.
        let same = OldTuple::Key(Tuple(vec![t("3"), Datum::Null]));
        assert!(p.read_back(100, Some(&same), &new).is_none());
        assert!(p.read_back(100, None, &new).is_none());
        assert!(p
            .read_back(100, Some(&old_key), &Tuple(vec![t("3"), t("x")]))
            .is_none());
        let full = OldTuple::Full(Tuple(vec![t("2"), t("old")]));
        assert!(p.read_back(100, Some(&full), &new).is_none());
        // The filled tuple decodes to a `c` that carries v.
        p.on_update(100, Some(&old_key), &filled, now).unwrap();
        p.on_commit(Lsn(0x910), now).unwrap();
        let b = p.take(now, true).unwrap();
        let c: serde_json::Value = serde_json::from_str(&b.items.last().unwrap().payload).unwrap();
        assert_eq!(c["op"], "c");
        assert_eq!(c["after"]["v"], "big value");
        assert!(c.get("unchanged").is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn idle_advancement_only_when_nothing_is_open_or_queued() {
        let now = Instant::now();
        let mut p = pipe(at(0x100), 10);
        assert!(!p.idle(Lsn(0x100), now), "not beyond the position");
        p.on_begin(Lsn(0x200), 0, 1).unwrap();
        assert!(!p.idle(Lsn(0x300), now), "a transaction is open");
        p.on_insert(100, &Tuple(vec![t("1"), t("x")]), now).unwrap();
        p.on_commit(Lsn(0x210), now).unwrap();
        assert!(!p.idle(Lsn(0x300), now), "events are queued");
        p.take(now, true).unwrap();
        assert!(p.idle(Lsn(0x300), now));
        let b = p.take(now, false).expect("an idle advance is forced out");
        assert!(b.items.is_empty());
        assert_eq!(b.cover, at(0x300));
    }

    #[tokio::test(start_paused = true)]
    async fn unmapped_relations_cost_nothing_but_move_the_position() {
        let now = Instant::now();
        let mut p = pipe(at(0x100), 10);
        let mut other = rel(999, "other");
        other.namespace = "x".into();
        p.on_relation(&other, &HashMap::new()).unwrap();
        p.on_begin(Lsn(0x200), 0, 1).unwrap();
        p.on_insert(999, &Tuple(vec![t("1"), t("x")]), now).unwrap();
        p.on_commit(Lsn(0x210), now).unwrap();
        assert!(!p.bundler.has_items());
        assert!(
            p.take(now, true).is_none(),
            "no events: no bundle of its own"
        );
        txn(&mut p, 0x300, 0x310, 0..1, now);
        let b = p.take(now, true).unwrap();
        assert_eq!(b.cover, at(0x310));
        // A change of a relation never described is a protocol error.
        p.on_begin(Lsn(0x400), 0, 1).unwrap();
        assert!(p.on_insert(4242, &Tuple(vec![]), now).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn truncate_is_skipped_or_emitted_once_per_queue() {
        let now = Instant::now();
        let mut p = pipe(at(0x100), 10);
        p.on_begin(Lsn(0x200), 0, 1).unwrap();
        p.on_truncate(3, &[100, 200], now).unwrap();
        p.on_commit(Lsn(0x210), now).unwrap();
        assert_eq!(p.truncates_skipped, 1);
        assert!(!p.bundler.has_items());

        let mut e = Pipeline::new(
            "0badf00d".into(),
            tables(true),
            at(0x100),
            limits(10),
            TruncatePolicy::Emit,
            1,
        );
        e.on_relation(&rel(100, "a"), &HashMap::new()).unwrap();
        e.on_relation(&rel(200, "b"), &HashMap::new()).unwrap();
        e.on_begin(Lsn(0x200), 1_790_935_200_000_000, 9).unwrap();
        e.on_insert(100, &Tuple(vec![t("1"), t("x")]), now).unwrap();
        e.on_truncate(1, &[100, 200], now).unwrap();
        e.on_commit(Lsn(0x210), now).unwrap();
        let b = e.take(now, true).unwrap();
        assert_eq!(b.items.len(), 3, "the insert, then one `t` per queue");
        assert_eq!(b.items[1].partition, "all");
        assert_eq!(
            b.items[1].payload,
            r#"{"op":"t","tables":["public.a"],"cascade":true,"restartIdentity":false,"lsn":"0/200","xid":9,"ts":"2026-10-02T10:00:00.000000Z","seq":1}"#
        );
        assert_eq!(b.items[2].queue.as_ref(), "qb");
    }

    fn snap_start(lsn: u64) -> Position {
        Position {
            lsn: Lsn(lsn),
            in_txn: None,
            snapshot: SnapshotProgress::start(vec!["public.a".into(), "public.b".into()]),
        }
    }

    /// The DBLog cycle through the pipeline: transactions before `hw` go
    /// first, the chunk's surviving rows after them, the position at `hw`.
    #[tokio::test(start_paused = true)]
    async fn a_chunk_lands_after_the_transactions_before_its_high_watermark() {
        let now = Instant::now();
        let mut p = pipe(snap_start(0x100), 100);
        let (idx, after) = p.next_chunk(now).unwrap();
        assert_eq!((idx, after), (0, None));
        let n = p.snap.as_mut().unwrap().take_n();
        let rows: Vec<Vec<Option<String>>> = (1..=3)
            .map(|i| vec![Some(i.to_string()), Some("old".to_string())])
            .collect();
        p.install_chunk(Chunk::new(
            n,
            0,
            &p.tables[0].clone(),
            10,
            rows,
            Visibility::parse("50:50:").unwrap(),
            1_790_935_200_000_000,
        ));
        assert!(p.next_chunk(now).is_none(), "one chunk at a time");
        let pfx = snapshot::prefix("0badf00d");
        assert_eq!(
            p.on_message(false, Lsn(0x150), &pfx, format!("lw:{n}").as_bytes(), now),
            Note::Low
        );
        // In the window: key 2 changes.
        p.on_begin(Lsn(0x200), 0, 49).unwrap();
        p.on_update(100, None, &Tuple(vec![t("2"), t("new")]), now)
            .unwrap();
        p.on_commit(Lsn(0x210), now).unwrap();
        assert_eq!(
            p.on_message(
                false,
                Lsn(0x300),
                "queen:other",
                format!("hw:{n}").as_bytes(),
                now
            ),
            Note::Ignored,
            "another epoch's watermark"
        );
        assert_eq!(
            p.on_message(false, Lsn(0x300), &pfx, format!("hw:{n}").as_bytes(), now),
            Note::High {
                rows: 2,
                finished: false
            }
        );
        let b = p.take(now, true).unwrap();
        let ops: Vec<String> = b
            .items
            .iter()
            .map(|i| {
                let v: serde_json::Value = serde_json::from_str(&i.payload).unwrap();
                format!("{}{}", v["op"].as_str().unwrap(), v["key"]["id"])
            })
            .collect();
        assert_eq!(ops, vec!["u2", "r1", "r3"]);
        assert_eq!(b.cover.lsn, Lsn(0x300));
        let s = b.cover.snapshot.clone().unwrap();
        assert_eq!(s.table, "public.b", "a short chunk finished table a");
        assert_eq!(s.rows, 2);
        assert_eq!(p.next_chunk(now).unwrap().0, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_heartbeat_and_foreign_messages() {
        let now = Instant::now();
        let mut p = pipe(at(0x100), 100);
        let pfx = snapshot::prefix("0badf00d");
        assert_eq!(
            p.on_message(false, Lsn(0x500), &pfx, b"hb", now),
            Note::Heartbeat(Lsn(0x500))
        );
        assert_eq!(
            p.on_message(true, Lsn(0x500), &pfx, b"hb", now),
            Note::Ignored
        );
        assert_eq!(
            p.on_message(false, Lsn(0x500), "app", b"hb", now),
            Note::Ignored
        );
        assert_eq!(
            p.on_message(false, Lsn(0x500), &pfx, b"hw:1", now),
            Note::Ignored,
            "no snapshot running"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_snapshot_table_no_longer_configured_is_skipped() {
        let now = Instant::now();
        let start = Position {
            lsn: Lsn(0x100),
            in_txn: None,
            snapshot: SnapshotProgress::start(vec!["public.gone".into(), "public.b".into()]),
        };
        let mut p = pipe(start, 100);
        assert_eq!(p.next_chunk(now).unwrap().0, 1);
        let b = p.take(now, false).expect("the skip is written");
        assert_eq!(b.cover.snapshot.unwrap().table, "public.b");
    }
}
