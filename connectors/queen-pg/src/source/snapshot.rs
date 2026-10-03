//! The snapshot: DBLog watermark chunks while the stream runs (PLAN §4.5).
//!
//! Each chunk is read in three steps on the regular connection — a low
//! watermark `lw:<n>` (a non-transactional logical message), the chunk, a high
//! watermark `hw:<n>` — and the stream decides at `hw:n` which rows of the
//! chunk are still news: a key that a stream transaction before `hw` changed
//! is dropped from the chunk, because that transaction's event (pushed before
//! the chunk) carries the same or a newer state.
//!
//! **Which transactions drop keys.** The plan's rule is "every change between
//! `lw:n` and `hw:n`". That misses one case: a transaction whose commit record
//! is BEFORE `lw` in the WAL but which is not yet visible when the chunk's
//! snapshot is taken — it inserted its commit record and is still flushing
//! the WAL or waiting for a synchronous standby. Its event comes before the
//! chunk in the queue, the chunk holds the row as it was before it, and the
//! stale row would be the last word on that key. So the chunk is read inside
//! a `REPEATABLE READ` transaction that also returns `pg_current_snapshot()`,
//! and a stream transaction drops its keys when it is in the window OR is not
//! visible to that snapshot (its xid in `xip`, or `>= xmax`). Transactions
//! decoded before the chunk was read are remembered (`history`) until a
//! snapshot sees them: once a committed transaction is visible it is visible
//! to every later snapshot, so the history only holds what committed after
//! the last chunk's snapshot.
//!
//! **Fills.** Dropping a chunk row assumes the stream events that dropped
//! it carry the row. An update that leaves a TOASTed column unchanged does
//! not: pgoutput sends no value for it, and with the chunk row gone nothing
//! would ever carry it to a consumer that did not have the row yet. So every
//! dropped row folds its drop-causing changes, in order, into a [`Walk`]; a
//! row that still exists after them and has columns that every one of its
//! updates since its last insert left unchanged (M) gets a FILL event at
//! `hw`: `u` with the key and M's values from the chunk row, every other
//! non-key column `unchanged`. Exact: a column no drop-causing change
//! carried was changed by none of them, the chunk's snapshot sees every
//! change before them, and changes after `hw` come after the fill — so the
//! chunk's value is the value at `hw`.
//!
//! **Text.** The chunk is read with the SIMPLE query protocol, whose text
//! format is each type's output function — what pgoutput sends. `col::text`
//! is not that for every type (`bpchar::text` strips the padding,
//! `inet::text` adds `/32`), so the plan's `::text` casts would break the
//! byte-identity of `r` and `c` events. Key bounds are inlined as escaped
//! literals cast to the column's `format_type`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;

use tokio_postgres::SimpleQueryMessage;

use crate::error::{Error, Result};
use crate::pg::catalog::quote_ident;
use crate::repl::Lsn;

use super::bundle::{Limits, Unit};
use super::events::{
    key_code, object_all, object_at, snapshot_fill_id, snapshot_txn_id, Eff, Head, Item, Val,
};
use super::pointer::{Position, SnapshotProgress};
use super::table::TableState;

/// `pg_current_snapshot()`: which transactions a snapshot sees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Visibility {
    pub xmin: u64,
    pub xmax: u64,
    pub xip: HashSet<u64>,
}

impl Visibility {
    /// `xmin:xmax:xip,xip,…` (the `pg_snapshot` text form).
    pub fn parse(s: &str) -> Result<Visibility> {
        let bad = || Error::io(format!("unreadable pg_current_snapshot(): {s:?}"));
        let mut it = s.trim().splitn(3, ':');
        let xmin = it.next().and_then(|v| v.parse().ok()).ok_or_else(bad)?;
        let xmax = it.next().and_then(|v| v.parse().ok()).ok_or_else(bad)?;
        let mut xip = HashSet::new();
        for x in it.next().unwrap_or("").split(',').filter(|x| !x.is_empty()) {
            xip.insert(x.parse::<u64>().map_err(|_| bad())?);
        }
        Ok(Visibility { xmin, xmax, xip })
    }

    /// Whether this snapshot sees a COMMITTED transaction (every transaction
    /// the stream delivers is committed).
    pub fn sees(&self, xid: u32) -> bool {
        let x = self.widen(xid);
        if x < self.xmin {
            return true;
        }
        if x >= self.xmax {
            return false;
        }
        !self.xip.contains(&x)
    }

    /// The 64-bit xid nearest `xmax` whose low 32 bits are `xid`: the stream
    /// carries 32-bit xids, the snapshot epoch-qualified ones, and a live
    /// transaction is always within 2^31 of the snapshot.
    fn widen(&self, xid: u32) -> u64 {
        const HALF: u64 = 1 << 31;
        const FULL: u64 = 1 << 32;
        let r = self.xmax;
        let c = (r & !0xFFFF_FFFF) | u64::from(xid);
        if c > r.saturating_add(HALF) && c >= FULL {
            c - FULL
        } else if c.saturating_add(HALF) < r {
            c + FULL
        } else {
            c
        }
    }
}

/// What one committed stream transaction touched in the tables still to be
/// snapshotted, in change order: `(table, Some(key code), effect)`, or
/// `(table, None, Delete)` for a TRUNCATE (every key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Touch {
    pub xid: u32,
    pub keys: Vec<(usize, Option<String>, Eff)>,
}

/// A dropped chunk row's drop-causing changes, folded in order: does the row
/// exist after them, and which columns did every update since its last
/// insert leave unchanged (M, the columns a fill must carry).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Walk {
    exists: bool,
    /// An insert or a key change into the key since the last delete: every
    /// column's value is in the stream (or is not this chunk row's to give).
    carried_all: bool,
    /// The columns unchanged in EVERY update of the segment (`None`: no
    /// update yet). A column one update carried is known from then on, so
    /// intersecting is exact: M = ∪unchanged \ ∪carried = ∩unchanged.
    m: Option<BTreeSet<String>>,
}

impl Walk {
    pub fn apply(&mut self, e: &Eff) {
        match e {
            Eff::Put | Eff::MoveIn => {
                self.exists = true;
                self.carried_all = true;
                self.m = None;
            }
            Eff::Delete => *self = Walk::default(),
            Eff::Update(u) => {
                self.exists = true;
                if self.carried_all {
                    return;
                }
                let u: BTreeSet<String> = u.iter().cloned().collect();
                self.m = Some(match self.m.take() {
                    None => u,
                    Some(prev) => prev.intersection(&u).cloned().collect(),
                });
            }
        }
    }

    /// The columns to fill from the chunk row, when a fill is due.
    pub fn fill(&self) -> Option<&BTreeSet<String>> {
        if self.exists && !self.carried_all {
            self.m.as_ref().filter(|m| !m.is_empty())
        } else {
            None
        }
    }
}

/// One chunk between its read and its high watermark.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub n: u64,
    pub table: usize,
    pub limit: usize,
    /// Text per column, in key order, as read (dropped rows keep theirs: a
    /// fill takes values from them).
    pub rows: Vec<Vec<Option<String>>>,
    /// Rows a stream change dropped.
    pub dropped: Vec<bool>,
    walks: HashMap<usize, Walk>,
    keys: HashMap<String, usize>,
    /// The key of the last row READ (whether or not it survives): the next
    /// chunk starts after it.
    pub last_key: Option<Vec<String>>,
    pub vis: Visibility,
    pub window_open: bool,
    pub read_at_us: i64,
}

impl Chunk {
    pub fn new(
        n: u64,
        table: usize,
        t: &TableState,
        limit: usize,
        rows: Vec<Vec<Option<String>>>,
        vis: Visibility,
        read_at_us: i64,
    ) -> Chunk {
        let mut keys = HashMap::with_capacity(rows.len());
        let mut last_key = None;
        for (i, r) in rows.iter().enumerate() {
            let vals: Vec<Val<'_>> = r
                .iter()
                .map(|v| v.as_deref().map(Val::Text).unwrap_or(Val::Null))
                .collect();
            keys.insert(key_code(&vals, &t.key_pos), i);
            last_key = Some(
                t.key_pos
                    .iter()
                    .map(|&p| r.get(p).cloned().flatten().unwrap_or_default())
                    .collect(),
            );
        }
        Chunk {
            n,
            table,
            limit,
            dropped: vec![false; rows.len()],
            rows,
            walks: HashMap::new(),
            keys,
            last_key,
            vis,
            window_open: false,
            read_at_us,
        }
    }

    /// Fewer rows than asked for: the table ends with this chunk.
    pub fn is_last(&self) -> bool {
        self.rows.len() < self.limit
    }

    pub fn surviving(&self) -> usize {
        self.dropped.iter().filter(|d| !**d).count()
    }

    /// The columns row `i` must be filled with, when it was dropped and a
    /// fill is due.
    pub fn fill_of(&self, i: usize) -> Option<&BTreeSet<String>> {
        if !self.dropped.get(i).copied().unwrap_or(false) {
            return None;
        }
        self.walks.get(&i).and_then(Walk::fill)
    }

    fn drop_touched(&mut self, t: &Touch) {
        for (table, key, eff) in &t.keys {
            if *table != self.table {
                continue;
            }
            match key {
                None => {
                    for i in 0..self.rows.len() {
                        self.dropped[i] = true;
                        self.walks.entry(i).or_default().apply(&Eff::Delete);
                    }
                }
                Some(k) => {
                    if let Some(&i) = self.keys.get(k) {
                        self.dropped[i] = true;
                        self.walks.entry(i).or_default().apply(eff);
                    }
                }
            }
        }
    }
}

/// The snapshot's state while it runs.
#[derive(Debug)]
pub struct SnapState {
    /// The progress after everything queued (the tail, not the committed).
    pub progress: SnapshotProgress,
    pub chunk: Option<Chunk>,
    history: Vec<Touch>,
    last_vis: Option<Visibility>,
    next_n: u64,
}

impl SnapState {
    /// `first_n` should differ between lives of the engine (random), so a
    /// watermark redelivered from an earlier life never matches a chunk of
    /// this one.
    pub fn new(progress: SnapshotProgress, first_n: u64) -> SnapState {
        SnapState {
            progress,
            chunk: None,
            history: Vec::new(),
            last_vis: None,
            next_n: first_n,
        }
    }

    /// The number of the next chunk.
    pub fn take_n(&mut self) -> u64 {
        let n = self.next_n;
        self.next_n = self.next_n.wrapping_add(1);
        n
    }

    /// A committed stream transaction (only tables still to be snapshotted).
    pub fn on_commit(&mut self, t: Touch) {
        if t.keys.is_empty() {
            return;
        }
        match &mut self.chunk {
            Some(c) => {
                let invisible = !c.vis.sees(t.xid);
                if c.window_open || invisible {
                    c.drop_touched(&t);
                }
                // Not seen by this chunk's snapshot: maybe not by the next
                // one either.
                if invisible {
                    self.history.push(t);
                }
            }
            None => {
                if !self.last_vis.as_ref().is_some_and(|v| v.sees(t.xid)) {
                    self.history.push(t);
                }
            }
        }
    }

    /// A chunk was read: drop what the remembered transactions it does not see
    /// touched, forget the ones it sees.
    pub fn install(&mut self, mut c: Chunk) {
        self.history.retain(|t| {
            if c.vis.sees(t.xid) {
                false
            } else {
                c.drop_touched(t);
                true
            }
        });
        self.last_vis = Some(c.vis.clone());
        self.chunk = Some(c);
    }

    pub fn history_len(&self) -> usize {
        self.history.len()
    }

    /// `lw:<n>` arrived in the stream.
    pub fn on_lw(&mut self, n: u64) {
        if let Some(c) = &mut self.chunk {
            if c.n == n {
                c.window_open = true;
            }
        }
    }

    /// `hw:<n>` arrived: the chunk in flight when it is `n`'s, else nothing (a
    /// watermark redelivered after a restart, or a chunk given up).
    pub fn on_hw(&mut self, n: u64) -> Option<Chunk> {
        if self.chunk.as_ref().is_some_and(|c| c.n == n) {
            self.chunk.take()
        } else {
            None
        }
    }
}

/// `n` of `lw:<n>` / `hw:<n>`.
pub fn parse_watermark(content: &[u8]) -> Option<(bool, u64)> {
    let s = std::str::from_utf8(content).ok()?;
    if let Some(n) = s.strip_prefix("lw:") {
        return n.parse().ok().map(|n| (false, n));
    }
    if let Some(n) = s.strip_prefix("hw:") {
        return n.parse().ok().map(|n| (true, n));
    }
    None
}

/// The prefix of this source's logical messages.
pub fn prefix(epoch: &str) -> String {
    format!("queen:{epoch}")
}

/// The low watermark statement.
pub fn lw_sql(epoch: &str, n: u64) -> String {
    format!(
        "SELECT pg_logical_emit_message(false, 'queen:{epoch}', 'lw:{n}')::text",
        epoch = sanitize(epoch)
    )
}

/// The heartbeat statement (flushed, so it reaches the walsender at once even
/// with `synchronous_commit = off`).
pub fn hb_sql(epoch: &str) -> String {
    format!(
        "SELECT pg_logical_emit_message(false, 'queen:{}', 'hb', true)::text",
        sanitize(epoch)
    )
}

/// The epoch is 8 hex characters; anything else never reaches SQL.
fn sanitize(epoch: &str) -> String {
    epoch.chars().filter(|c| c.is_ascii_hexdigit()).collect()
}

/// `E'…'` with backslashes and quotes escaped: correct whatever
/// `standard_conforming_strings` says.
pub fn literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 3);
    out.push_str("E'");
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("''"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

/// The chunk read and the high watermark, ONE simple query (five statements):
/// `BEGIN REPEATABLE READ`, the snapshot, the rows, `COMMIT`, `hw:<n>`.
pub fn chunk_sql(
    t: &TableState,
    after: Option<&[String]>,
    limit: usize,
    epoch: &str,
    n: u64,
) -> String {
    let mut q = String::with_capacity(256);
    q.push_str(
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SELECT pg_current_snapshot()::text; SELECT ",
    );
    for (i, c) in t.cols.iter().enumerate() {
        if i > 0 {
            q.push_str(", ");
        }
        q.push_str(&quote_ident(&c.name));
    }
    q.push_str(" FROM ");
    q.push_str(&t.table.quoted());
    let keys: Vec<String> = t
        .key_pos
        .iter()
        .map(|&p| quote_ident(&t.cols[p].name))
        .collect();
    if let Some(after) = after {
        q.push_str(" WHERE (");
        q.push_str(&keys.join(", "));
        q.push_str(") > (");
        for (i, (&p, v)) in t.key_pos.iter().zip(after.iter()).enumerate() {
            if i > 0 {
                q.push_str(", ");
            }
            let _ = write!(q, "{}::{}", literal(v), t.type_names[p]);
        }
        q.push(')');
    }
    let _ = write!(
        q,
        " ORDER BY {} LIMIT {limit}; COMMIT; SELECT pg_logical_emit_message(false, 'queen:{}', \
         'hw:{n}', true)::text",
        keys.join(", "),
        sanitize(epoch)
    );
    q
}

/// What the chunk query answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkAnswer {
    pub vis: Visibility,
    pub rows: Vec<Vec<Option<String>>>,
    pub hw_end: Lsn,
}

/// Split the five statements' answers.
pub fn parse_chunk(msgs: &[SimpleQueryMessage], ncols: usize) -> Result<ChunkAnswer> {
    let mut stmt = 0usize;
    let mut vis = None;
    let mut rows = Vec::new();
    let mut hw = None;
    for m in msgs {
        match m {
            SimpleQueryMessage::CommandComplete(_) => stmt += 1,
            SimpleQueryMessage::Row(r) => match stmt {
                1 => vis = Some(Visibility::parse(r.get(0).unwrap_or(""))?),
                2 => {
                    if r.len() != ncols {
                        return Err(Error::io(format!(
                            "the snapshot chunk has {} columns, {ncols} expected",
                            r.len()
                        )));
                    }
                    rows.push((0..ncols).map(|i| r.get(i).map(str::to_string)).collect());
                }
                4 => {
                    hw = r
                        .get(0)
                        .and_then(|s| s.parse::<Lsn>().ok())
                        .or(Some(Lsn::ZERO))
                }
                _ => {}
            },
            _ => {}
        }
    }
    match (vis, hw) {
        (Some(vis), Some(hw_end)) => Ok(ChunkAnswer { vis, rows, hw_end }),
        _ => Err(Error::io(
            "the snapshot chunk query did not answer every statement".to_string(),
        )),
    }
}

/// A finished chunk as units: its surviving rows as `r` events and its
/// fills (dropped rows a [`Walk`] says need one) as `u` events, in key order,
/// cut where a part would pass the bundle limits; each part's cover says how
/// far the table got (the last part: the last key READ, or the next table
/// when the chunk was the table's last). A chunk with nothing to push is one
/// position-only unit, forced so the progress is not redone after a restart.
///
/// Ids: `pg:<epoch>:s:<hw16>:<seq>` for a row, `pg:<epoch>:s:<hw16>:f<seq>`
/// for a fill, `seq` counting both in key order.
pub fn chunk_units(
    c: &Chunk,
    t: &TableState,
    epoch: &str,
    hw: Lsn,
    progress: &SnapshotProgress,
    limits: &Limits,
) -> (Vec<Unit>, Option<SnapshotProgress>) {
    let head = Head {
        op: "r",
        table: &t.name,
        lsn: hw,
        xid: None,
        ts_us: c.read_at_us,
        seq: 0,
    };
    let mut units = Vec::new();
    let mut part: Vec<Item> = Vec::new();
    let mut part_bytes = 0usize;
    let mut part_rows = 0u64;
    let mut part_last_key: Option<Vec<String>> = None;
    let mut rows_total = progress.rows;
    let cover = |after: Option<Vec<String>>, rows: u64| Position {
        lsn: hw,
        in_txn: None,
        snapshot: Some(SnapshotProgress {
            tables: progress.tables.clone(),
            table: progress.table.clone(),
            after,
            rows,
        }),
    };
    let mut seq = 0u64;
    for (i, r) in c.rows.iter().enumerate() {
        let fill = if c.dropped[i] {
            match c.fill_of(i) {
                Some(m) => Some(m),
                None => continue,
            }
        } else {
            None
        };
        let vals: Vec<Val<'_>> = r
            .iter()
            .map(|v| v.as_deref().map(Val::Text).unwrap_or(Val::Null))
            .collect();
        let key = object_at(&t.cols, &vals, &t.key_pos, None);
        let (payload, txn_id) = match fill {
            None => (
                super::events::event(
                    &Head { seq, ..head },
                    &key,
                    Some(&object_all(&t.cols, &vals, None)),
                    None,
                    &[],
                ),
                snapshot_txn_id(epoch, hw, seq),
            ),
            Some(m) => {
                let keep = |p: usize| t.key_pos.contains(&p) || m.contains(&t.cols[p].name);
                let pos: Vec<usize> = (0..t.cols.len()).filter(|&p| keep(p)).collect();
                let unchanged: Vec<&str> = (0..t.cols.len())
                    .filter(|&p| !keep(p))
                    .map(|p| t.cols[p].name.as_str())
                    .collect();
                (
                    super::events::event(
                        &Head {
                            op: "u",
                            seq,
                            ..head
                        },
                        &key,
                        Some(&object_at(&t.cols, &vals, &pos, None)),
                        None,
                        &unchanged,
                    ),
                    snapshot_fill_id(epoch, hw, seq),
                )
            }
        };
        seq += 1;
        let item = Item {
            queue: t.queue.clone(),
            partition: super::events::partition_of(&vals, t.part_pos.as_deref()),
            txn_id,
            payload,
        };
        let len = item.wire_len();
        if !part.is_empty()
            && (part.len() + 1 > limits.max_messages || part_bytes + len > limits.max_bytes)
        {
            rows_total += part_rows;
            let mut u = Unit::new(
                std::mem::take(&mut part),
                cover(part_last_key.take(), rows_total),
            );
            u.snapshot_rows = part_rows;
            units.push(u);
            part_bytes = 0;
            part_rows = 0;
        }
        part_bytes += len;
        if fill.is_none() {
            part_rows += 1;
        }
        part.push(item);
        part_last_key = Some(
            t.key_pos
                .iter()
                .map(|&p| r.get(p).cloned().flatten().unwrap_or_default())
                .collect(),
        );
    }
    rows_total += part_rows;
    let end = if c.is_last() {
        progress.next_table(rows_total)
    } else {
        Some(SnapshotProgress {
            tables: progress.tables.clone(),
            table: progress.table.clone(),
            after: c.last_key.clone(),
            rows: rows_total,
        })
    };
    let mut last = Unit::new(
        part,
        Position {
            lsn: hw,
            in_txn: None,
            snapshot: end.clone(),
        },
    );
    last.snapshot_rows = part_rows;
    last.force = last.items.is_empty();
    units.push(last);
    (units, end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{PartitionBy, TableSpec};
    use crate::pg::catalog::{ColumnInfo, TableInfo, TableName};
    use std::time::Duration;

    fn table() -> TableState {
        let col = |n: &str, attnum: i16, oid: u32, ty: &str| ColumnInfo {
            name: n.into(),
            attnum,
            type_oid: oid,
            type_name: ty.into(),
            not_null: true,
            generated: false,
        };
        let info = TableInfo {
            oid: 1,
            name: TableName {
                schema: "public".into(),
                name: "t".into(),
            },
            columns: vec![
                col("a", 1, 23, "integer"),
                col("b", 2, 25, "text"),
                col("v", 3, 25, "text"),
            ],
            primary_key: vec!["a".into(), "b".into()],
            replica_identity: b'd',
            identity_index: vec![],
        };
        super::super::table::resolve(
            &TableSpec {
                table: "public.t".into(),
                queue: "q".into(),
                partition_by: PartitionBy::Default,
            },
            &info,
            None,
            &HashMap::new(),
        )
        .unwrap()
    }

    fn vis(s: &str) -> Visibility {
        Visibility::parse(s).unwrap()
    }

    fn row(a: &str, b: &str, v: &str) -> Vec<Option<String>> {
        vec![Some(a.into()), Some(b.into()), Some(v.into())]
    }

    #[test]
    fn snapshots_parse_and_judge_visibility() {
        let v = vis("100:110:102,105");
        assert!(v.sees(99) && v.sees(100) && v.sees(101) && v.sees(109));
        assert!(!v.sees(102) && !v.sees(105), "in progress");
        assert!(!v.sees(110) && !v.sees(4000), "not yet assigned");
        assert_eq!(
            vis("5:5:"),
            Visibility {
                xmin: 5,
                xmax: 5,
                xip: HashSet::new()
            }
        );
        assert!(Visibility::parse("x").is_err());
        // Across a wraparound: the snapshot is epoch-qualified, the stream is
        // not.
        let e1 = 1u64 << 32;
        let w = vis(&format!("{}:{}:", e1 - 10, e1 + 10));
        assert!(w.sees(u32::MAX - 20), "an old xid of the previous epoch");
        assert!(w.sees(5), "a completed xid of the new epoch");
        assert!(!w.sees(10), "xmax itself");
    }

    #[test]
    fn the_chunk_query_reads_output_text_and_bounds_by_the_key() {
        let t = table();
        let q = chunk_sql(&t, None, 100, "0badf00d", 7);
        assert_eq!(
            q,
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SELECT pg_current_snapshot()::text; \
             SELECT \"a\", \"b\", \"v\" FROM \"public\".\"t\" ORDER BY \"a\", \"b\" LIMIT 100; \
             COMMIT; SELECT pg_logical_emit_message(false, 'queen:0badf00d', 'hw:7', true)::text"
        );
        let after = vec!["42".to_string(), "it's \\ x".to_string()];
        let q = chunk_sql(&t, Some(&after), 100, "0badf00d", 8);
        assert!(
            q.contains(
                "WHERE (\"a\", \"b\") > (E'42'::integer, E'it''s \\\\ x'::text) ORDER BY \"a\", \"b\""
            ),
            "{q}"
        );
        assert_eq!(
            lw_sql("0badf00d", 9),
            "SELECT pg_logical_emit_message(false, 'queen:0badf00d', 'lw:9')::text"
        );
        assert!(hb_sql("0bad'f00d").contains("'queen:0badf00d', 'hb', true"));
        assert_eq!(parse_watermark(b"lw:12"), Some((false, 12)));
        assert_eq!(parse_watermark(b"hw:3"), Some((true, 3)));
        assert_eq!(parse_watermark(b"hb"), None);
    }

    fn chunk(n: u64, rows: Vec<Vec<Option<String>>>, limit: usize, v: &str) -> Chunk {
        Chunk::new(n, 0, &table(), limit, rows, vis(v), 1_790_935_200_000_000)
    }

    /// A transaction that updated `keys` (no TOAST column left unchanged).
    fn touch(xid: u32, keys: &[&str]) -> Touch {
        Touch {
            xid,
            keys: keys
                .iter()
                .map(|k| (0usize, Some(k.to_string()), Eff::Update(Vec::new())))
                .collect(),
        }
    }

    #[test]
    fn a_window_change_drops_its_key_and_another_watermark_is_ignored() {
        let mut s = SnapState::new(SnapshotProgress::start(vec!["public.t".into()]).unwrap(), 7);
        s.install(chunk(
            7,
            vec![row("1", "a", "x"), row("2", "b", "y")],
            10,
            "100:100:",
        ));
        s.on_commit(touch(90, &["1\0a"]));
        assert_eq!(
            s.chunk.as_ref().unwrap().surviving(),
            2,
            "before lw and visible: kept"
        );
        s.on_lw(6);
        s.on_commit(touch(91, &["1\0a"]));
        assert_eq!(
            s.chunk.as_ref().unwrap().surviving(),
            2,
            "lw:6 is not this chunk's"
        );
        s.on_lw(7);
        s.on_commit(touch(95, &["1\0a", "9\0z"]));
        assert_eq!(s.chunk.as_ref().unwrap().surviving(), 1);
        assert!(
            s.on_hw(6).is_none(),
            "a watermark of another chunk is ignored"
        );
        let c = s.on_hw(7).unwrap();
        assert!(c.dropped[0]);
        assert!(!c.dropped[1]);
        assert_eq!(c.fill_of(0), None, "every column carried: no fill");
        assert_eq!(c.last_key, Some(vec!["2".to_string(), "b".to_string()]));
        assert!(s.chunk.is_none());
    }

    /// The case the window misses: a transaction committed in the WAL before
    /// `lw` that the chunk's snapshot did not see. Decoded BEFORE the chunk
    /// was read (history) or after (directly): either way its key goes.
    #[test]
    fn a_transaction_the_chunk_snapshot_did_not_see_drops_its_key() {
        let mut s = SnapState::new(SnapshotProgress::start(vec!["public.t".into()]).unwrap(), 1);
        // Decoded before the chunk exists.
        s.on_commit(touch(105, &["1\0a"]));
        s.on_commit(touch(90, &["2\0b"]));
        assert_eq!(s.history_len(), 2);
        // The chunk's snapshot: 105 in progress, 90 done.
        s.install(chunk(
            1,
            vec![row("1", "a", "old"), row("2", "b", "y"), row("3", "c", "z")],
            10,
            "95:110:105",
        ));
        let c = s.chunk.as_ref().unwrap();
        assert!(c.dropped[0], "105 was invisible: its key is dropped");
        assert!(!c.dropped[1], "90 is in the chunk already");
        assert_eq!(
            s.history_len(),
            1,
            "90 is forgotten, 105 kept for the next chunk"
        );
        // Decoded after the read, before lw, invisible (>= xmax): dropped too.
        s.on_commit(touch(111, &["3\0c"]));
        assert_eq!(s.chunk.as_ref().unwrap().surviving(), 1);
        let c = s.on_hw(1).unwrap();
        assert_eq!(c.surviving(), 1);
        // The next chunk sees 105 and 111: both forgotten.
        s.install(chunk(2, vec![row("4", "d", "w")], 10, "120:120:"));
        assert_eq!(s.history_len(), 0);
    }

    fn upd(u: &[&str]) -> Eff {
        Eff::Update(u.iter().map(|c| c.to_string()).collect())
    }

    fn walk(effs: &[Eff]) -> Option<Vec<String>> {
        let mut w = Walk::default();
        for e in effs {
            w.apply(e);
        }
        w.fill().map(|m| m.iter().cloned().collect())
    }

    /// M = the columns every update since the row's last insert left
    /// unchanged; an insert carries all; a delete clears; a key change INTO
    /// the key is another row's (never filled from this chunk row).
    #[test]
    fn the_fill_columns_of_a_dropped_row() {
        let big = Some(vec!["big".to_string()]);
        assert_eq!(
            walk(&[upd(&["big"])]),
            big,
            "its only change left big unchanged"
        );
        assert_eq!(
            walk(&[upd(&["big"]), upd(&[])]),
            None,
            "the second carried big"
        );
        assert_eq!(
            walk(&[upd(&[]), upd(&["big"])]),
            None,
            "carried before, unchanged since"
        );
        assert_eq!(
            walk(&[upd(&["big", "notes"]), upd(&["big"])]),
            big,
            "notes was carried by the second"
        );
        assert_eq!(
            walk(&[upd(&["big", "notes"]), upd(&["notes", "big"])]),
            Some(vec!["big".to_string(), "notes".to_string()])
        );
        assert_eq!(
            walk(&[Eff::Put, upd(&["big"])]),
            None,
            "an insert carries all"
        );
        assert_eq!(walk(&[upd(&["big"]), Eff::Delete]), None, "gone at hw");
        assert_eq!(walk(&[Eff::Delete, Eff::Put]), None);
        assert_eq!(
            walk(&[upd(&["big"]), Eff::Delete, Eff::Put, upd(&["big"])]),
            None,
            "re-inserted: the insert carried big"
        );
        assert_eq!(
            walk(&[Eff::MoveIn, upd(&["big"])]),
            None,
            "another row moved here"
        );
        assert_eq!(walk(&[upd(&[])]), None, "nothing left unchanged");
        assert_eq!(walk(&[]), None);
    }

    fn toast_table() -> TableState {
        let col = |n: &str, attnum: i16, oid: u32, ty: &str| ColumnInfo {
            name: n.into(),
            attnum,
            type_oid: oid,
            type_name: ty.into(),
            not_null: false,
            generated: false,
        };
        let info = TableInfo {
            oid: 2,
            name: TableName {
                schema: "public".into(),
                name: "docs".into(),
            },
            columns: vec![
                col("id", 1, 20, "bigint"),
                col("n", 2, 23, "integer"),
                col("big", 3, 25, "text"),
            ],
            primary_key: vec!["id".into()],
            replica_identity: b'd',
            identity_index: vec![],
        };
        super::super::table::resolve(
            &TableSpec {
                table: "public.docs".into(),
                queue: "q".into(),
                partition_by: PartitionBy::Default,
            },
            &info,
            None,
            &HashMap::new(),
        )
        .unwrap()
    }

    fn doc_row(id: &str, n: &str, big: &str) -> Vec<Option<String>> {
        vec![Some(id.into()), Some(n.into()), Some(big.into())]
    }

    /// The loss the fills close: a row dropped by a window update and by an
    /// invisible transaction decoded before the read, both leaving `big`
    /// unchanged, comes back as a `u` with the key and `big` from the chunk,
    /// `n` unchanged (the stream carried it), in the chunk's own unit.
    #[test]
    fn a_row_dropped_by_updates_that_left_toast_unchanged_is_filled() {
        let t = toast_table();
        let mut s = SnapState::new(
            SnapshotProgress::start(vec!["public.docs".into()]).unwrap(),
            4,
        );
        // Decoded before the read, invisible to it (in progress at S).
        s.on_commit(Touch {
            xid: 105,
            keys: vec![(0, Some("1".into()), upd(&["big"]))],
        });
        let rows = vec![
            doc_row("1", "0", "B1"),
            doc_row("2", "0", "B2"),
            doc_row("3", "0", "B3"),
        ];
        s.install(Chunk::new(
            4,
            0,
            &t,
            10,
            rows,
            vis("100:110:105"),
            1_790_935_200_000_000,
        ));
        s.on_lw(4);
        // In the window: key 2's update leaves big unchanged; key 3's
        // carries everything.
        s.on_commit(Touch {
            xid: 100,
            keys: vec![
                (0, Some("2".into()), upd(&["big"])),
                (0, Some("3".into()), upd(&[])),
            ],
        });
        let c = s.on_hw(4).unwrap();
        assert_eq!(c.surviving(), 0);
        assert_eq!(c.fill_of(0).map(|m| m.len()), Some(1));
        assert_eq!(c.fill_of(2), None);
        let limits = Limits {
            max_messages: 100,
            max_bytes: 1 << 20,
            linger: Duration::ZERO,
        };
        let hw: Lsn = "0/3000".parse().unwrap();
        let p = SnapshotProgress::start(vec!["public.docs".into()]).unwrap();
        let (units, end) = chunk_units(&c, &t, "0badf00d", hw, &p, &limits);
        assert_eq!(
            units.len(),
            1,
            "the fills ride in the chunk's unit, with the pointer"
        );
        let items = &units[0].items;
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].txn_id, "pg:0badf00d:s:0000000000003000:f0");
        assert_eq!(items[0].partition, "1");
        assert_eq!(
            items[0].payload,
            r#"{"op":"u","table":"public.docs","key":{"id":1},"after":{"id":1,"big":"B1"},"before":null,"unchanged":["n"],"lsn":"0/3000","ts":"2026-10-02T10:00:00.000000Z","seq":0}"#
        );
        assert_eq!(items[1].txn_id, "pg:0badf00d:s:0000000000003000:f1");
        assert_eq!(units[0].snapshot_rows, 0, "fills are not snapshot rows");
        assert_eq!(
            end, None,
            "a short chunk of the only table ends the snapshot"
        );
        assert_eq!(units[0].cover.snapshot, None);
    }

    #[test]
    fn a_truncate_in_the_window_drops_the_whole_chunk() {
        let mut s = SnapState::new(SnapshotProgress::start(vec!["public.t".into()]).unwrap(), 1);
        s.install(chunk(
            1,
            vec![row("1", "a", "x"), row("2", "b", "y")],
            10,
            "100:100:",
        ));
        s.on_lw(1);
        s.on_commit(Touch {
            xid: 99,
            keys: vec![(0, None, Eff::Delete)],
        });
        assert_eq!(s.on_hw(1).unwrap().surviving(), 0);
    }

    #[test]
    fn a_chunk_becomes_r_events_in_parts_with_progress() {
        let t = table();
        let p = SnapshotProgress {
            tables: vec!["public.t".into(), "public.u".into()],
            table: "public.t".into(),
            after: None,
            rows: 10,
        };
        let limits = Limits {
            max_messages: 2,
            max_bytes: 1 << 20,
            linger: Duration::ZERO,
        };
        let mut c = chunk(
            3,
            vec![
                row("1", "a", "x"),
                row("2", "b", "y"),
                row("3", "c", "z"),
                row("4", "d", "w"),
            ],
            4,
            "1:1:",
        );
        c.dropped[3] = true; // dropped by the stream
        let hw: Lsn = "0/2000".parse().unwrap();
        let (units, end) = chunk_units(&c, &t, "0badf00d", hw, &p, &limits);
        assert_eq!(units.len(), 2);
        assert_eq!(units[0].items.len(), 2);
        let s0 = units[0].cover.snapshot.clone().unwrap();
        assert_eq!(s0.after, Some(vec!["2".to_string(), "b".to_string()]));
        assert_eq!(s0.rows, 12);
        assert_eq!(units[0].cover.lsn, hw);
        assert_eq!(units[1].items.len(), 1);
        let s1 = units[1].cover.snapshot.clone().unwrap();
        assert_eq!(
            s1.after,
            Some(vec!["4".to_string(), "d".to_string()]),
            "the last key READ, past the dropped row"
        );
        assert_eq!(s1.rows, 13);
        assert_eq!(end, Some(s1));
        let first = &units[0].items[0];
        assert_eq!(first.txn_id, "pg:0badf00d:s:0000000000002000:0");
        assert_eq!(first.partition, "1|a");
        assert_eq!(
            first.payload,
            r#"{"op":"r","table":"public.t","key":{"a":1,"b":"a"},"after":{"a":1,"b":"a","v":"x"},"before":null,"lsn":"0/2000","ts":"2026-10-02T10:00:00.000000Z","seq":0}"#
        );
        assert_eq!(units[1].items[0].txn_id, "pg:0badf00d:s:0000000000002000:2");

        // A short chunk ends the table: the next one starts from its first key.
        let c = chunk(4, vec![row("9", "z", "q")], 4, "1:1:");
        let (units, end) = chunk_units(&c, &t, "0badf00d", hw, &p, &limits);
        assert_eq!(units.len(), 1);
        let e = end.unwrap();
        assert_eq!(
            (e.table.as_str(), e.after.clone(), e.rows),
            ("public.u", None, 11)
        );
        // ... and the last table's last chunk ends the snapshot.
        let last = SnapshotProgress {
            table: "public.u".into(),
            ..p.clone()
        };
        let mut c = chunk(5, vec![row("9", "z", "q")], 4, "1:1:");
        c.dropped[0] = true;
        let (units, end) = chunk_units(&c, &t, "0badf00d", hw, &last, &limits);
        assert_eq!(end, None);
        assert!(
            units[0].items.is_empty() && units[0].force,
            "the progress is still written"
        );
        assert_eq!(units[0].cover.snapshot, None);
    }
}
