//! The MySQL snapshot: DBLog chunks while the binlog streams, with read-only
//! watermarks.
//!
//! The Postgres source writes its watermarks into the WAL; this one cannot
//! write (the source's user only reads), so a chunk is bracketed by what the
//! server says about itself instead:
//!
//! 1. `lw` = the executed GTID set (`SHOW BINARY LOG STATUS`);
//! 2. the chunk, read in its own consistent-snapshot transaction;
//! 3. `hw` = the binlog position after the read.
//!
//! **Which transactions drop keys.** MySQL commits a transaction in the
//! engine BEFORE it adds its GTID to the executed set (binlog.cc,
//! `process_commit_stage_queue` and `finish_commit` in 8.4), so a GTID in
//! `lw` is a transaction the chunk's read view sees. Every other transaction
//! may or may not be in the chunk: one the stream delivers before the chunk
//! is queued drops the keys it touched from the chunk, whether it came
//! before the chunk was read (remembered in `history` until a later `lw`
//! holds it) or after. Dropping is always safe here: `binlog_row_image=FULL`
//! puts the whole row in every event, so the event that dropped a row
//! carries a state at least as new (there is no Postgres-style fill).
//!
//! **When the chunk is queued.** Once the stream has read to `hw`: every
//! transaction the read view saw is then behind it, so nothing queued after
//! the chunk is older than it. (Queued earlier would still be right in the
//! end state, every later change following; waiting keeps a consumer from
//! seeing a key go back in time.) An idle database is at `hw` already.

use std::collections::HashMap;

use mysql_async::Value;

use crate::error::Result;
use crate::source::bundle::{Limits, Unit};
use crate::source::events::Item;
use crate::source::pointer::SnapshotProgress;

use super::catalog::{quote, BinlogPos};
use super::events::{snapshot_id, Body, Cols};
use super::gtid::{Gtid, GtidSet};
use super::pointer::MyPosition;
use super::table::MyTable;
use super::values::{param_value, placeholder, Cell};

/// What one transaction touched in the tables still to be snapshotted:
/// `(table, Some(key code))`, or `(table, None)` for a TRUNCATE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Touch {
    pub gtid: Gtid,
    pub keys: Vec<(usize, Option<String>)>,
}

/// One chunk between its read and its queueing.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub table: usize,
    /// The table as the chunk read it (its columns re-read for the chunk,
    /// so an ALTER during the snapshot is in the rows it renders).
    pub t: MyTable,
    pub limit: usize,
    /// In the table's column order.
    pub rows: Vec<Vec<Cell>>,
    pub dropped: Vec<bool>,
    keys: HashMap<String, usize>,
    /// The key (parameter texts) of the last row READ, dropped or not: the
    /// next chunk starts after it.
    pub last_key: Option<Vec<String>>,
    pub lw: GtidSet,
    pub hw: BinlogPos,
    pub read_at_us: i64,
}

fn cols_of(t: &MyTable) -> (Vec<&str>, Vec<super::values::Kind>) {
    (
        t.cols.iter().map(|c| c.name.as_str()).collect(),
        t.cols.iter().map(|c| c.kind).collect(),
    )
}

/// The key of `row` as chunk-bound parameter texts.
pub fn key_params(t: &MyTable, row: &[Cell]) -> Vec<String> {
    t.key_pos
        .iter()
        .map(|&p| {
            row.get(p)
                .and_then(|c| c.param_text(t.cols[p].kind))
                .unwrap_or_default()
        })
        .collect()
}

impl Chunk {
    pub fn new(
        table: usize,
        t: &MyTable,
        limit: usize,
        rows: Vec<Vec<Cell>>,
        lw: GtidSet,
        hw: BinlogPos,
        read_at_us: i64,
    ) -> Chunk {
        let (names, kinds) = cols_of(t);
        let cols = Cols {
            names: &names,
            kinds: &kinds,
        };
        let mut keys = HashMap::with_capacity(rows.len());
        for (i, r) in rows.iter().enumerate() {
            keys.insert(cols.key_code(r, &t.key_pos), i);
        }
        let last_key = rows.last().map(|r| key_params(t, r));
        Chunk {
            table,
            t: t.clone(),
            limit,
            dropped: vec![false; rows.len()],
            rows,
            keys,
            last_key,
            lw,
            hw,
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

    fn drop_touched(&mut self, t: &Touch) {
        for (table, key) in &t.keys {
            if *table != self.table {
                continue;
            }
            match key {
                None => self.dropped.iter_mut().for_each(|d| *d = true),
                Some(k) => {
                    if let Some(&i) = self.keys.get(k) {
                        self.dropped[i] = true;
                    }
                }
            }
        }
    }
}

/// The snapshot while it runs.
#[derive(Debug)]
pub struct SnapState {
    /// The progress after everything queued (the tail's).
    pub progress: SnapshotProgress,
    pub chunk: Option<Chunk>,
    history: Vec<Touch>,
}

impl SnapState {
    pub fn new(progress: SnapshotProgress) -> SnapState {
        SnapState {
            progress,
            chunk: None,
            history: Vec::new(),
        }
    }

    /// A transaction the stream delivered (touching tables still to read).
    pub fn on_commit(&mut self, t: Touch) {
        if t.keys.is_empty() {
            return;
        }
        if let Some(c) = &mut self.chunk {
            if !c.lw.contains(&t.gtid) {
                c.drop_touched(&t);
            }
        }
        self.history.push(t);
    }

    /// A chunk was read: what the remembered transactions its `lw` lacks
    /// touched is dropped; the ones `lw` holds are seen by every later read.
    pub fn install(&mut self, mut c: Chunk) {
        self.history.retain(|t| {
            if c.lw.contains(&t.gtid) {
                false
            } else {
                c.drop_touched(t);
                true
            }
        });
        self.chunk = Some(c);
    }

    pub fn history_len(&self) -> usize {
        self.history.len()
    }
}

/// The chunk statement: the table's columns after `after` (when there is a
/// previous chunk) in key order, `LIMIT ?`. A composite key's bound is
/// spelled out (`k1 > ? OR (k1 = ? AND k2 > ?)`), which the range optimizer
/// reads as index ranges.
pub fn chunk_sql(t: &MyTable, with_after: bool) -> String {
    let cols: Vec<String> = t.cols.iter().map(|c| quote(&c.name)).collect();
    let keys: Vec<String> = t.key_pos.iter().map(|&p| quote(&t.cols[p].name)).collect();
    let mut sql = format!("SELECT {} FROM {}", cols.join(", "), t.tn.quoted());
    if with_after {
        let ph: Vec<String> = t
            .key_pos
            .iter()
            .map(|&p| placeholder(t.cols[p].kind, &t.cols[p].column_type))
            .collect();
        let mut ors = Vec::with_capacity(keys.len());
        for j in 0..keys.len() {
            let mut and: Vec<String> = (0..j).map(|i| format!("{} = {}", keys[i], ph[i])).collect();
            and.push(format!("{} > {}", keys[j], ph[j]));
            ors.push(if and.len() == 1 {
                and.pop().unwrap_or_default()
            } else {
                format!("({})", and.join(" AND "))
            });
        }
        sql.push_str(" WHERE ");
        sql.push_str(&ors.join(" OR "));
    }
    sql.push_str(" ORDER BY ");
    sql.push_str(&keys.join(", "));
    sql.push_str(" LIMIT ?");
    sql
}

/// The statement's parameters, in [`chunk_sql`]'s order.
pub fn chunk_params(t: &MyTable, after: Option<&[String]>, limit: usize) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    if let Some(a) = after {
        let vals: Vec<Value> = t
            .key_pos
            .iter()
            .zip(a)
            .map(|(&p, text)| param_value(t.cols[p].kind, text))
            .collect::<Result<_>>()?;
        for j in 0..vals.len() {
            out.extend(vals[..=j].iter().cloned());
        }
    }
    out.push(Value::UInt(limit as u64));
    Ok(out)
}

/// The surviving rows of `chunk` as `r` events in units that fit
/// `limits`, each moving the snapshot's progress; the last unit carries the
/// progress after the chunk (the next table when the table ended, `None`
/// when the whole snapshot did). Returns the units and that progress.
pub fn chunk_units(
    chunk: &Chunk,
    t: &MyTable,
    epoch: &str,
    tail: &MyPosition,
    progress: &SnapshotProgress,
    limits: &Limits,
) -> (Vec<Unit<MyPosition>>, Option<SnapshotProgress>) {
    let (names, kinds) = cols_of(t);
    let cols = Cols {
        names: &names,
        kinds: &kinds,
    };
    let mut units = Vec::new();
    let mut rows_done = progress.rows;
    let mut items: Vec<Item> = Vec::new();
    let mut bytes = 0usize;
    let mut unit_rows = 0u64;
    let cover = |after: Option<Vec<String>>, rows: u64| MyPosition {
        snapshot: Some(SnapshotProgress {
            tables: progress.tables.clone(),
            table: progress.table.clone(),
            after,
            rows,
        }),
        ..tail.clone()
    };
    for (i, row) in chunk.rows.iter().enumerate() {
        if chunk.dropped[i] {
            continue;
        }
        let body = Body {
            op: "r",
            key: cols.object_at(row, &t.key_pos),
            after: Some(cols.object_all(row)),
            before: None,
        };
        let item = Item {
            queue: t.queue.clone(),
            partition: cols.partition(row, t.part_pos.as_deref()),
            txn_id: snapshot_id(epoch, rows_done),
            payload: body.render(&t.name, None, chunk.read_at_us, rows_done),
        };
        let len = item.wire_len();
        if !items.is_empty()
            && (items.len() + 1 > limits.max_messages || bytes + len > limits.max_bytes)
        {
            // Close a unit at the last row it holds: a restart reads on
            // from there.
            let after = chunk.rows[..i].last().map(|r| key_params(t, r));
            let mut u = Unit::new(std::mem::take(&mut items), cover(after, rows_done));
            u.snapshot_rows = unit_rows;
            units.push(u);
            bytes = 0;
            unit_rows = 0;
        }
        bytes += len;
        items.push(item);
        unit_rows += 1;
        rows_done += 1;
    }
    let end = if chunk.is_last() {
        progress.next_table(rows_done)
    } else {
        Some(SnapshotProgress {
            tables: progress.tables.clone(),
            table: progress.table.clone(),
            after: chunk.last_key.clone().or_else(|| progress.after.clone()),
            rows: rows_done,
        })
    };
    let mut last = Unit::new(
        items,
        MyPosition {
            snapshot: end.clone(),
            ..tail.clone()
        },
    );
    last.snapshot_rows = unit_rows;
    last.force = true;
    units.push(last);
    (units, end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{PartitionBy, TableSpec};
    use crate::mysql::catalog::{ColumnInfo, MyTableName, TableInfo};
    use crate::mysql::table::resolve;
    use crate::mysql::values::{Charset, Kind};
    use std::time::Duration;

    const U: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

    pub(crate) fn table(composite: bool) -> MyTable {
        let c = |n: &str, k: Kind, ty: &str| ColumnInfo {
            name: n.into(),
            kind: k,
            column_type: ty.into(),
        };
        let mut columns = vec![
            c("id", Kind::Int, "bigint"),
            c("v", Kind::Text(Charset::Utf8), "varchar(20)"),
        ];
        let mut pk = vec!["id".to_string()];
        if composite {
            columns.insert(1, c("at", Kind::DateTime(0), "datetime"));
            pk.push("at".into());
        }
        resolve(
            &TableSpec {
                table: "shop.t".into(),
                queue: "q".into(),
                partition_by: PartitionBy::Default,
            },
            &TableInfo {
                name: MyTableName {
                    db: "shop".into(),
                    name: "t".into(),
                },
                columns,
                primary_key: pk,
            },
        )
        .unwrap()
    }

    fn g(n: u64) -> Gtid {
        Gtid {
            uuid: U.into(),
            tag: String::new(),
            gno: n,
        }
    }

    fn rows(ids: &[i64]) -> Vec<Vec<Cell>> {
        ids.iter()
            .map(|&i| vec![Cell::Int(i), Cell::Str(format!("v{i}"))])
            .collect()
    }

    fn chunk(lw: &str, ids: &[i64], limit: usize) -> Chunk {
        Chunk::new(
            0,
            &table(false),
            limit,
            rows(ids),
            GtidSet::parse(lw).unwrap(),
            BinlogPos {
                file: "binlog.000001".into(),
                pos: 500,
            },
            0,
        )
    }

    #[test]
    fn statements_bound_composite_keys_with_typed_casts() {
        let t = table(true);
        assert_eq!(
            chunk_sql(&t, false),
            "SELECT `id`, `at`, `v` FROM `shop`.`t` ORDER BY `id`, `at` LIMIT ?"
        );
        assert_eq!(
            chunk_sql(&t, true),
            "SELECT `id`, `at`, `v` FROM `shop`.`t` WHERE `id` > ? OR (`id` = ? AND `at` > \
             CAST(? AS DATETIME(6))) ORDER BY `id`, `at` LIMIT ?"
        );
        let p = chunk_params(
            &t,
            Some(&["7".into(), "2026-01-02 03:04:05.000000".into()]),
            100,
        )
        .unwrap();
        assert_eq!(p.len(), 4);
        assert_eq!(p[0], Value::Int(7));
        assert_eq!(p[3], Value::UInt(100));
    }

    #[test]
    fn transactions_outside_lw_drop_keys_before_and_after_the_read() {
        let mut s = SnapState::new(SnapshotProgress::start(vec!["shop.t".into()]).unwrap());
        // Delivered before the chunk was read: one the read saw (in lw),
        // one it may not have.
        s.on_commit(Touch {
            gtid: g(5),
            keys: vec![(0, Some("1".into()))],
        });
        s.on_commit(Touch {
            gtid: g(6),
            keys: vec![(0, Some("2".into()))],
        });
        s.install(chunk(&format!("{U}:1-5"), &[1, 2, 3, 4], 10));
        assert_eq!(s.history_len(), 1, "g5 is in lw: forgotten");
        // After the read: in lw (seen by the read), and not.
        s.on_commit(Touch {
            gtid: g(4),
            keys: vec![(0, Some("3".into()))],
        });
        s.on_commit(Touch {
            gtid: g(7),
            keys: vec![(0, Some("4".into()))],
        });
        let c = s.chunk.as_ref().unwrap();
        assert_eq!(c.dropped, vec![false, true, false, true]);
        assert_eq!(c.surviving(), 2);
    }

    #[test]
    fn a_truncate_drops_the_whole_chunk() {
        let mut s = SnapState::new(SnapshotProgress::start(vec!["shop.t".into()]).unwrap());
        s.install(chunk(&format!("{U}:1-5"), &[1, 2], 10));
        s.on_commit(Touch {
            gtid: g(9),
            keys: vec![(0, None)],
        });
        assert_eq!(s.chunk.as_ref().unwrap().surviving(), 0);
    }

    #[test]
    fn units_split_by_the_limits_and_carry_the_progress() {
        let t = table(false);
        let c = chunk(&format!("{U}:1-5"), &[1, 2, 3, 4, 5], 5);
        let progress = SnapshotProgress::start(vec!["shop.t".into(), "shop.u".into()]).unwrap();
        let limits = Limits {
            max_messages: 2,
            max_bytes: 1 << 20,
            linger: Duration::ZERO,
        };
        let (units, end) = chunk_units(&c, &t, "e", &MyPosition::default(), &progress, &limits);
        assert_eq!(units.len(), 3);
        let sizes: Vec<usize> = units.iter().map(|u| u.items.len()).collect();
        assert_eq!(sizes, vec![2, 2, 1]);
        let after: Vec<Option<Vec<String>>> = units
            .iter()
            .map(|u| u.cover.snapshot.as_ref().and_then(|s| s.after.clone()))
            .collect();
        assert_eq!(
            after,
            vec![
                Some(vec!["2".into()]),
                Some(vec!["4".into()]),
                Some(vec!["5".into()])
            ]
        );
        assert_eq!(units[0].items[0].txn_id, "my:e:r:0");
        assert!(units[0].items[1].payload.contains("\"op\":\"r\""));
        // Five rows of a five-row limit: not the last chunk yet.
        assert_eq!(end.as_ref().unwrap().table, "shop.t");
        assert_eq!(end.as_ref().unwrap().rows, 5);
        // A short chunk ends the table: the progress moves to the next one.
        let short = chunk(&format!("{U}:1-5"), &[6], 5);
        let (_, end) = chunk_units(&short, &t, "e", &MyPosition::default(), &progress, &limits);
        assert_eq!(end.unwrap().table, "shop.u");
    }
}
