//! The message payload (PLAN §4.6), the partition a change goes to, and the
//! message ids (PLAN §4.4).
//!
//! An event is built as JSON TEXT, never through `serde_json::Value`: column
//! values come from [`crate::values`], which copies a bigint or a numeric as
//! the exact digits the server printed, and a `Value` round trip would turn
//! them into f64. The same functions render a stream row (pgoutput text
//! datums) and a snapshot row (the simple query protocol's text, which is the
//! type's output function — what pgoutput sends), so an `r` event and a `c`
//! event for the same row are byte-identical in `key` and `after`.

use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::config::{PartitionBy, PartitionMode};
use crate::repl::pgoutput::{Datum, OldTuple, Tuple};
use crate::repl::Lsn;
use crate::values::{append_json_string, append_json_with_element};

/// A partition name longer than this many bytes becomes `~` + 32 hex chars of
/// its SHA-256: the broker's name budget is shared with the tenant and the
/// queue, and a key can be any length.
pub const MAX_PARTITION_BYTES: usize = 128;

/// How a column's text becomes JSON: the type the converter sees, and the
/// element type of an array type the converter cannot know (a non-builtin
/// array: `enum[]`, `domain[]`). A domain is resolved to its base type, so a
/// domain over `int4` is a number in the stream AND in the snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TypeConv {
    pub oid: u32,
    pub elem: Option<u32>,
}

impl TypeConv {
    pub fn builtin(oid: u32) -> TypeConv {
        TypeConv { oid, elem: None }
    }
}

/// One column as the event renders it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Col {
    pub name: String,
    pub conv: TypeConv,
}

/// One value of a row: what pgoutput or a snapshot row says about a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Val<'a> {
    Null,
    /// Unchanged TOAST: the value was not sent.
    Unchanged,
    Text(&'a str),
}

impl<'a> Val<'a> {
    pub fn of(d: &'a Datum) -> Val<'a> {
        match d {
            Datum::Null => Val::Null,
            Datum::Unchanged => Val::Unchanged,
            // pgoutput sends text in the connection's client encoding (UTF8).
            // A value that is not UTF-8 cannot be a JSON string as is; it is
            // rendered as null rather than corrupting the payload, which never
            // happens with UTF8 on both ends.
            Datum::Text(b) => std::str::from_utf8(b).map(Val::Text).unwrap_or(Val::Null),
            // Never requested (proto v1, text mode).
            Datum::Binary(_) => Val::Null,
        }
    }

    pub fn text(self) -> Option<&'a str> {
        match self {
            Val::Text(s) => Some(s),
            _ => None,
        }
    }
}

/// The values of a pgoutput tuple, one per relation column (missing trailing
/// columns, which a well-formed tuple never has, read as NULL).
pub fn vals_of(t: &Tuple, n: usize) -> Vec<Val<'_>> {
    (0..n)
        .map(|i| t.0.get(i).map(Val::of).unwrap_or(Val::Null))
        .collect()
}

/// How a table's rows map to partitions, resolved against its columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionRule {
    /// The replica identity key's values (the default).
    Key,
    /// Explicit columns.
    Columns(Vec<String>),
    /// One partition, `all`: total commit order in the queue.
    Single,
}

impl PartitionRule {
    pub fn from_config(p: &PartitionBy) -> PartitionRule {
        match p {
            PartitionBy::Mode(PartitionMode::Single) => PartitionRule::Single,
            PartitionBy::Columns(c) => PartitionRule::Columns(c.clone()),
            PartitionBy::Mode(PartitionMode::Key) | PartitionBy::Default => PartitionRule::Key,
        }
    }

    /// The column names the partition is rendered from (`None`: `single`).
    pub fn columns<'a>(&'a self, key: &'a [String]) -> Option<&'a [String]> {
        match self {
            PartitionRule::Key => Some(key),
            PartitionRule::Columns(c) => Some(c),
            PartitionRule::Single => None,
        }
    }
}

/// PLAN §4.6 "Partitions": one column → its text; several → joined with `|`,
/// `|` and `\` escaped with `\`; NULL → `\N`; longer than 128 bytes → `~` +
/// the first 32 hex chars of the rendering's SHA-256.
pub fn render_partition(values: &[Option<&str>]) -> String {
    let rendered = match values {
        [one] => one.unwrap_or("\\N").to_string(),
        many => {
            let mut s = String::new();
            for (i, v) in many.iter().enumerate() {
                if i > 0 {
                    s.push('|');
                }
                match v {
                    None => s.push_str("\\N"),
                    Some(t) => {
                        for c in t.chars() {
                            if c == '|' || c == '\\' {
                                s.push('\\');
                            }
                            s.push(c);
                        }
                    }
                }
            }
            s
        }
    };
    if rendered.len() > MAX_PARTITION_BYTES {
        let digest = Sha256::digest(rendered.as_bytes());
        format!("~{}", &hex::encode(digest)[..32])
    } else {
        rendered
    }
}

/// The partition of `vals` (positions `part`, `None` = `single`). An
/// unchanged-TOAST partition value — possible only for a column outside the
/// replica identity of a `FULL` table, which the decoder fills from the old
/// row first — renders as NULL.
pub fn partition_of(vals: &[Val<'_>], part: Option<&[usize]>) -> String {
    match part {
        None => "all".to_string(),
        Some(pos) => {
            let v: Vec<Option<&str>> = pos
                .iter()
                .map(|&i| vals.get(i).and_then(|v| v.text()))
                .collect();
            render_partition(&v)
        }
    }
}

/// The text of the key columns joined by NUL (which no PostgreSQL text value
/// contains): the injective map key the snapshot filters chunks by.
pub fn key_code(vals: &[Val<'_>], key: &[usize]) -> String {
    let mut s = String::new();
    for (i, &p) in key.iter().enumerate() {
        if i > 0 {
            s.push('\0');
        }
        if let Some(t) = vals.get(p).and_then(|v| v.text()) {
            s.push_str(t);
        }
    }
    s
}

/// `pg:<epoch>:<commit LSN, 16 hex>:<seq>` — a stream event, `seq` being
/// the raw change index in the source transaction; `…:<seq>.<k>` for the
/// `k`-th (k >= 1) event of one change (a key move's `c`, a truncate's second
/// queue). Built from the WAL alone, so ids never collide across a restart
/// under another configuration.
pub fn stream_txn_id(epoch: &str, commit_lsn: Lsn, seq: u64, k: usize) -> String {
    if k == 0 {
        format!("pg:{epoch}:{}:{seq}", commit_lsn.to_hex16())
    } else {
        format!("pg:{epoch}:{}:{seq}.{k}", commit_lsn.to_hex16())
    }
}

/// `pg:<epoch>:s:<high watermark LSN, 16 hex>:<seq>` — a snapshot row.
pub fn snapshot_txn_id(epoch: &str, hw: Lsn, seq: u64) -> String {
    format!("pg:{epoch}:s:{}:{seq}", hw.to_hex16())
}

/// `pg:<epoch>:s:<high watermark LSN, 16 hex>:f<seq>` — a snapshot FILL (the
/// unchanged-TOAST values of a row the chunk filter dropped,
/// [`super::snapshot::Walk`]).
pub fn snapshot_fill_id(epoch: &str, hw: Lsn, seq: u64) -> String {
    format!("pg:{epoch}:s:{}:f{seq}", hw.to_hex16())
}

/// One push of a bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item {
    pub queue: Arc<str>,
    pub partition: String,
    pub txn_id: String,
    /// The event, JSON text.
    pub payload: String,
}

impl Item {
    /// Bytes this item adds to a bundle's body.
    pub fn wire_len(&self) -> usize {
        self.queue.len() + self.partition.len() + self.txn_id.len() + self.payload.len() + 64
    }

    /// `{"queue":…,"partition":…,"payload":…,"transactionId":…}`.
    pub fn append_json(&self, out: &mut String) {
        out.push_str("{\"queue\":");
        append_json_string(&self.queue, out);
        out.push_str(",\"partition\":");
        append_json_string(&self.partition, out);
        out.push_str(",\"payload\":");
        out.push_str(&self.payload);
        out.push_str(",\"transactionId\":");
        append_json_string(&self.txn_id, out);
        out.push('}');
    }
}

/// Append one value (non-null text) as JSON.
fn append_value(conv: TypeConv, text: &str, out: &mut String) {
    append_json_with_element(conv.oid, conv.elem, text, out);
}

/// A JSON object of the columns at `pos`, in that order. Unchanged values are
/// left out (their positions are pushed to `unchanged` when given).
pub fn object_at(
    cols: &[Col],
    vals: &[Val<'_>],
    pos: &[usize],
    mut unchanged: Option<&mut Vec<usize>>,
) -> String {
    let mut out = String::with_capacity(16 * pos.len() + 2);
    out.push('{');
    let mut first = true;
    for &i in pos {
        let (Some(col), Some(v)) = (cols.get(i), vals.get(i)) else {
            continue;
        };
        match v {
            Val::Unchanged => {
                if let Some(u) = unchanged.as_deref_mut() {
                    u.push(i);
                }
                continue;
            }
            Val::Null | Val::Text(_) => {}
        }
        if !first {
            out.push(',');
        }
        first = false;
        append_json_string(&col.name, &mut out);
        out.push(':');
        match v {
            Val::Text(t) => append_value(col.conv, t, &mut out),
            _ => out.push_str("null"),
        }
    }
    out.push('}');
    out
}

/// A JSON object of every column, in column order.
pub fn object_all(cols: &[Col], vals: &[Val<'_>], unchanged: Option<&mut Vec<usize>>) -> String {
    let all: Vec<usize> = (0..cols.len()).collect();
    object_at(cols, vals, &all, unchanged)
}

/// The fields every event shares.
#[derive(Debug, Clone, Copy)]
pub struct Head<'a> {
    /// `c` `u` `d` `r` `t`.
    pub op: &'static str,
    pub table: &'a str,
    pub lsn: Lsn,
    /// Absent for `r`.
    pub xid: Option<u32>,
    pub ts_us: i64,
    pub seq: u64,
}

/// The event JSON (PLAN §4.6): `op`, `table`, `key`, `after`, `before`,
/// `unchanged` (omitted when empty), `lsn`, `xid` (absent for `r`), `ts`,
/// `seq`. `key`/`after`/`before` are JSON texts (`None` = `null`).
pub fn event(
    h: &Head<'_>,
    key: &str,
    after: Option<&str>,
    before: Option<&str>,
    unchanged: &[&str],
) -> String {
    let mut out = String::with_capacity(
        96 + h.table.len() + key.len() + after.map_or(4, str::len) + before.map_or(4, str::len),
    );
    out.push_str("{\"op\":\"");
    out.push_str(h.op);
    out.push_str("\",\"table\":");
    append_json_string(h.table, &mut out);
    out.push_str(",\"key\":");
    out.push_str(key);
    out.push_str(",\"after\":");
    out.push_str(after.unwrap_or("null"));
    out.push_str(",\"before\":");
    out.push_str(before.unwrap_or("null"));
    if !unchanged.is_empty() {
        out.push_str(",\"unchanged\":[");
        for (i, u) in unchanged.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            append_json_string(u, &mut out);
        }
        out.push(']');
    }
    tail(h, &mut out);
    out
}

/// `t` (PLAN §4.6): the tables, the two options, and the shared tail.
pub fn truncate_event(h: &Head<'_>, tables: &[&str], cascade: bool, restart: bool) -> String {
    let mut out = String::from("{\"op\":\"t\",\"tables\":[");
    for (i, t) in tables.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        append_json_string(t, &mut out);
    }
    out.push_str("],\"cascade\":");
    out.push_str(if cascade { "true" } else { "false" });
    out.push_str(",\"restartIdentity\":");
    out.push_str(if restart { "true" } else { "false" });
    tail(h, &mut out);
    out
}

fn tail(h: &Head<'_>, out: &mut String) {
    use std::fmt::Write as _;
    out.push_str(",\"lsn\":\"");
    let _ = write!(out, "{}", h.lsn);
    out.push('"');
    if let Some(x) = h.xid {
        let _ = write!(out, ",\"xid\":{x}");
    }
    out.push_str(",\"ts\":");
    append_json_string(&crate::values::iso_utc_micros(h.ts_us), out);
    let _ = write!(out, ",\"seq\":{}}}", h.seq);
}

/// A relation as the decoder sees it: its table, its columns with their
/// converters, and the positions of the key and of the partition columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shape {
    pub table: usize,
    pub cols: Vec<Col>,
    /// Key columns, in key order.
    pub key: Vec<usize>,
    /// Partition columns; `None` = `single`.
    pub part: Option<Vec<usize>>,
    /// `REPLICA IDENTITY FULL`: the old row is complete.
    pub full: bool,
}

/// What one change did to one key, as the snapshot's chunk filter needs it
/// (PLAN §4.5): whether the row exists after it, and which of its columns it
/// sent without a value (unchanged TOAST). A chunk row dropped because of
/// such changes is filled back from the chunk for exactly those columns
/// ([`super::snapshot::Walk`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Eff {
    /// An insert: every column carried.
    Put,
    /// A key change INTO this key: the row is another row moved here, so no
    /// value of it may come from this key's chunk row (which, if any, was a
    /// different row).
    MoveIn,
    /// An update of this key, with the columns it left unchanged.
    Update(Vec<String>),
    /// A delete, a key change AWAY from this key, or a truncate.
    Delete,
}

/// What one change becomes: zero, one or two events (a partition move is a
/// `d` to the old partition and a `c` to the new one).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub events: Vec<(String, EventBody)>,
    /// The keys this change touched (new and old) and what it did to each,
    /// for the snapshot's chunk filter.
    pub touched: Vec<(String, Eff)>,
}

/// An event before its head (lsn, seq) is known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBody {
    pub op: &'static str,
    pub key: String,
    pub after: Option<String>,
    pub before: Option<String>,
    pub unchanged: Vec<String>,
}

impl EventBody {
    pub fn render(&self, h: &Head<'_>) -> String {
        let u: Vec<&str> = self.unchanged.iter().map(String::as_str).collect();
        event(
            &Head { op: self.op, ..*h },
            &self.key,
            self.after.as_deref(),
            self.before.as_deref(),
            &u,
        )
    }
}

fn names(cols: &[Col], pos: &[usize]) -> Vec<String> {
    pos.iter()
        .filter_map(|&i| cols.get(i).map(|c| c.name.clone()))
        .collect()
}

impl Shape {
    /// An INSERT.
    pub fn insert(&self, new: &Tuple) -> Change {
        let n = vals_of(new, self.cols.len());
        let mut unchanged = Vec::new();
        let after = object_all(&self.cols, &n, Some(&mut unchanged));
        Change {
            events: vec![(
                partition_of(&n, self.part.as_deref()),
                EventBody {
                    op: "c",
                    key: object_at(&self.cols, &n, &self.key, None),
                    after: Some(after),
                    before: None,
                    unchanged: names(&self.cols, &unchanged),
                },
            )],
            touched: vec![(key_code(&n, &self.key), Eff::Put)],
        }
    }

    /// An UPDATE. With `REPLICA IDENTITY FULL` the old row is complete, so an
    /// unchanged-TOAST value of the new row is filled from it (it is the same
    /// value by definition) and `unchanged` stays empty.
    pub fn update(&self, old: Option<&OldTuple>, new: &Tuple) -> Change {
        let mut n = vals_of(new, self.cols.len());
        let o: Option<Vec<Val<'_>>> = old.map(|t| match t {
            OldTuple::Key(t) | OldTuple::Full(t) => vals_of(t, self.cols.len()),
        });
        let old_full = matches!(old, Some(OldTuple::Full(_)));
        if let Some(o) = &o {
            for (i, v) in n.iter_mut().enumerate() {
                if *v == Val::Unchanged {
                    // A FULL old row has every value; a key-only old row has
                    // the key columns (an unchanged key value is the old one).
                    let fill = old_full || self.key.contains(&i);
                    if fill && matches!(o[i], Val::Text(_) | Val::Null) {
                        *v = o[i];
                    }
                }
            }
        }
        let mut unchanged = Vec::new();
        let after = object_all(&self.cols, &n, Some(&mut unchanged));
        let new_key = object_at(&self.cols, &n, &self.key, None);
        let new_part = partition_of(&n, self.part.as_deref());
        let before = o.as_ref().map(|o| {
            if old_full {
                object_all(&self.cols, o, None)
            } else {
                object_at(&self.cols, o, &self.key, None)
            }
        });
        let unchanged = names(&self.cols, &unchanged);
        let new_code = key_code(&n, &self.key);
        let touched = match o.as_ref().map(|o| key_code(o, &self.key)) {
            Some(old_code) if old_code != new_code => {
                vec![(new_code, Eff::MoveIn), (old_code, Eff::Delete)]
            }
            _ => vec![(new_code, Eff::Update(unchanged.clone()))],
        };
        if let Some(o) = &o {
            let old_part = partition_of(o, self.part.as_deref());
            if old_part != new_part {
                return Change {
                    events: vec![
                        (
                            old_part,
                            EventBody {
                                op: "d",
                                key: object_at(&self.cols, o, &self.key, None),
                                after: None,
                                before,
                                unchanged: Vec::new(),
                            },
                        ),
                        (
                            new_part,
                            EventBody {
                                op: "c",
                                key: new_key,
                                after: Some(after),
                                before: None,
                                unchanged,
                            },
                        ),
                    ],
                    touched,
                };
            }
        }
        Change {
            events: vec![(
                new_part,
                EventBody {
                    op: "u",
                    key: new_key,
                    after: Some(after),
                    before,
                    unchanged,
                },
            )],
            touched,
        }
    }

    /// A DELETE: `key` is the old key, `before` the old key or the old row.
    pub fn delete(&self, old: &OldTuple) -> Change {
        let (t, full) = match old {
            OldTuple::Key(t) => (t, false),
            OldTuple::Full(t) => (t, true),
        };
        let o = vals_of(t, self.cols.len());
        let key = object_at(&self.cols, &o, &self.key, None);
        let before = if full {
            object_all(&self.cols, &o, None)
        } else {
            key.clone()
        };
        Change {
            events: vec![(
                partition_of(&o, self.part.as_deref()),
                EventBody {
                    op: "d",
                    key,
                    after: None,
                    before: Some(before),
                    unchanged: Vec::new(),
                },
            )],
            touched: vec![(key_code(&o, &self.key), Eff::Delete)],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::oid;
    use bytes::Bytes;

    fn t(s: &str) -> Datum {
        Datum::Text(Bytes::copy_from_slice(s.as_bytes()))
    }

    fn orders() -> Shape {
        Shape {
            table: 0,
            cols: vec![
                Col {
                    name: "id".into(),
                    conv: TypeConv::builtin(oid::INT8),
                },
                Col {
                    name: "status".into(),
                    conv: TypeConv::builtin(oid::TEXT),
                },
                Col {
                    name: "total".into(),
                    conv: TypeConv::builtin(oid::NUMERIC),
                },
                Col {
                    name: "notes".into(),
                    conv: TypeConv::builtin(oid::TEXT),
                },
            ],
            key: vec![0],
            part: Some(vec![0]),
            full: false,
        }
    }

    fn head(seq: u64) -> Head<'static> {
        Head {
            op: "c",
            table: "public.orders",
            lsn: "0/170A2C8".parse().unwrap(),
            xid: Some(7781),
            ts_us: 1_790_935_200_123_456, // 2026-10-02T10:00:00.123456Z
            seq,
        }
    }

    /// PLAN §4.6's example, byte for byte (the field order included).
    #[test]
    fn an_update_with_unchanged_toast_renders_the_plan_example() {
        let s = orders();
        let ch = s.update(
            None,
            &Tuple(vec![t("42"), t("paid"), t("99.90"), Datum::Unchanged]),
        );
        assert_eq!(ch.events.len(), 1);
        let (part, body) = &ch.events[0];
        assert_eq!(part, "42");
        assert_eq!(
            body.render(&head(3)),
            r#"{"op":"u","table":"public.orders","key":{"id":42},"after":{"id":42,"status":"paid","total":99.90},"before":null,"unchanged":["notes"],"lsn":"0/170A2C8","xid":7781,"ts":"2026-10-02T10:00:00.123456Z","seq":3}"#
        );
        assert_eq!(
            ch.touched,
            vec![("42".to_string(), Eff::Update(vec!["notes".to_string()]))]
        );
    }

    #[test]
    fn inserts_and_deletes_carry_the_key_and_the_old_key() {
        let s = orders();
        let ch = s.insert(&Tuple(vec![
            t("1"),
            Datum::Null,
            t("12345678901234567890.5"),
            t("x"),
        ]));
        let (part, body) = &ch.events[0];
        assert_eq!(part, "1");
        assert_eq!(
            body.render(&head(0)),
            r#"{"op":"c","table":"public.orders","key":{"id":1},"after":{"id":1,"status":null,"total":12345678901234567890.5,"notes":"x"},"before":null,"lsn":"0/170A2C8","xid":7781,"ts":"2026-10-02T10:00:00.123456Z","seq":0}"#,
            "a numeric keeps its exact digits"
        );
        let ch = s.delete(&OldTuple::Key(Tuple(vec![
            t("1"),
            Datum::Null,
            Datum::Null,
            Datum::Null,
        ])));
        let (part, body) = &ch.events[0];
        assert_eq!(part, "1");
        assert_eq!(body.op, "d");
        assert_eq!(body.key, r#"{"id":1}"#);
        assert_eq!(body.after, None);
        assert_eq!(body.before.as_deref(), Some(r#"{"id":1}"#));
    }

    /// A key change under the default identity: pgoutput sends the old key, the
    /// partition moves, so it is a `d` to the old partition and a `c` to the
    /// new one, in that order.
    #[test]
    fn a_key_change_moves_partitions_with_a_delete_then_a_create() {
        let s = orders();
        let ch = s.update(
            Some(&OldTuple::Key(Tuple(vec![
                t("1"),
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ]))),
            &Tuple(vec![t("2"), t("new"), t("1"), t("n")]),
        );
        assert_eq!(ch.events.len(), 2);
        assert_eq!(ch.events[0].0, "1");
        assert_eq!(ch.events[0].1.op, "d");
        assert_eq!(ch.events[0].1.key, r#"{"id":1}"#);
        assert_eq!(ch.events[0].1.before.as_deref(), Some(r#"{"id":1}"#));
        assert_eq!(ch.events[1].0, "2");
        assert_eq!(ch.events[1].1.op, "c");
        assert_eq!(ch.events[1].1.key, r#"{"id":2}"#);
        assert_eq!(
            ch.touched,
            vec![
                ("2".to_string(), Eff::MoveIn),
                ("1".to_string(), Eff::Delete)
            ]
        );

        // With `single`, the same key change stays one `u` with the old key
        // in `before`.
        let mut single = orders();
        single.part = None;
        let ch = single.update(
            Some(&OldTuple::Key(Tuple(vec![
                t("1"),
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ]))),
            &Tuple(vec![t("2"), t("new"), t("1"), t("n")]),
        );
        assert_eq!(ch.events.len(), 1);
        assert_eq!(ch.events[0].0, "all");
        assert_eq!(ch.events[0].1.op, "u");
        assert_eq!(ch.events[0].1.before.as_deref(), Some(r#"{"id":1}"#));
    }

    /// REPLICA IDENTITY FULL: `before` is the old row, and an unchanged TOAST
    /// value is taken from it instead of being listed.
    #[test]
    fn a_full_identity_update_fills_unchanged_values_from_the_old_row() {
        let mut s = orders();
        s.full = true;
        let old = OldTuple::Full(Tuple(vec![t("7"), t("open"), t("1.5"), t("big")]));
        let ch = s.update(
            Some(&old),
            &Tuple(vec![t("7"), t("paid"), t("1.5"), Datum::Unchanged]),
        );
        let (_, b) = &ch.events[0];
        assert_eq!(b.op, "u");
        assert!(b.unchanged.is_empty());
        assert_eq!(
            b.after.as_deref(),
            Some(r#"{"id":7,"status":"paid","total":1.5,"notes":"big"}"#)
        );
        assert_eq!(
            b.before.as_deref(),
            Some(r#"{"id":7,"status":"open","total":1.5,"notes":"big"}"#)
        );
        let ch = s.delete(&old);
        assert_eq!(
            ch.events[0].1.before.as_deref(),
            Some(r#"{"id":7,"status":"open","total":1.5,"notes":"big"}"#)
        );
        assert_eq!(ch.events[0].1.key, r#"{"id":7}"#);

        // Partitioning by a non-key column under FULL: the old row says where
        // the row was.
        s.part = Some(vec![1]);
        let ch = s.update(
            Some(&old),
            &Tuple(vec![t("7"), t("paid"), t("1.5"), Datum::Unchanged]),
        );
        assert_eq!(ch.events.len(), 2);
        assert_eq!((ch.events[0].0.as_str(), ch.events[0].1.op), ("open", "d"));
        assert_eq!((ch.events[1].0.as_str(), ch.events[1].1.op), ("paid", "c"));
    }

    #[test]
    fn partitions_render_per_the_plan() {
        assert_eq!(render_partition(&[Some("42")]), "42");
        assert_eq!(render_partition(&[None]), "\\N");
        assert_eq!(
            render_partition(&[Some("a|b")]),
            "a|b",
            "one column is not escaped"
        );
        assert_eq!(
            render_partition(&[Some("a|b"), Some("c\\d"), None]),
            "a\\|b|c\\\\d|\\N"
        );
        assert_eq!(render_partition(&[Some(""), Some("")]), "|");
        let long = "x".repeat(129);
        let r = render_partition(&[Some(&long)]);
        assert_eq!(r.len(), 33);
        assert!(r.starts_with('~'));
        let want = hex::encode(Sha256::digest(long.as_bytes()));
        assert_eq!(&r[1..], &want[..32]);
        let edge = "y".repeat(128);
        assert_eq!(render_partition(&[Some(&edge)]), edge, "128 bytes is kept");
        // The hash is over the RENDERED name (escapes and separators included).
        let a = "a".repeat(100);
        let b = "b|".repeat(20);
        let r = render_partition(&[Some(&a), Some(&b)]);
        let rendered = format!("{a}|{}", "b\\|".repeat(20));
        assert_eq!(
            &r[1..],
            &hex::encode(Sha256::digest(rendered.as_bytes()))[..32]
        );
        assert_eq!(partition_of(&[Val::Text("1")], None), "all");
    }

    #[test]
    fn message_ids_and_items_have_the_wire_shape() {
        let l: Lsn = "0/170A2C8".parse().unwrap();
        assert_eq!(
            stream_txn_id("4f1c2a9b", l, 0, 0),
            "pg:4f1c2a9b:000000000170A2C8:0"
        );
        assert_eq!(
            stream_txn_id("4f1c2a9b", l, 3, 1),
            "pg:4f1c2a9b:000000000170A2C8:3.1",
            "the second event of one change"
        );
        assert_eq!(
            snapshot_txn_id("4f1c2a9b", l, 7),
            "pg:4f1c2a9b:s:000000000170A2C8:7"
        );
        let it = Item {
            queue: Arc::from("orders"),
            partition: "4\"2".into(),
            txn_id: "pg:x:1:0".into(),
            payload: r#"{"op":"c","n":12345678901234567890}"#.into(),
        };
        let mut s = String::new();
        it.append_json(&mut s);
        assert_eq!(
            s,
            r#"{"queue":"orders","partition":"4\"2","payload":{"op":"c","n":12345678901234567890},"transactionId":"pg:x:1:0"}"#
        );
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["partition"], "4\"2");
    }

    #[test]
    fn truncate_events_and_composite_keys() {
        let h = Head { op: "t", ..head(5) };
        assert_eq!(
            truncate_event(&h, &["public.a", "public.b"], true, false),
            r#"{"op":"t","tables":["public.a","public.b"],"cascade":true,"restartIdentity":false,"lsn":"0/170A2C8","xid":7781,"ts":"2026-10-02T10:00:00.123456Z","seq":5}"#
        );
        // A two-column key, key order differs from column order.
        let s = Shape {
            table: 0,
            cols: vec![
                Col {
                    name: "b".into(),
                    conv: TypeConv::builtin(oid::TEXT),
                },
                Col {
                    name: "a".into(),
                    conv: TypeConv::builtin(oid::INT4),
                },
            ],
            key: vec![1, 0],
            part: Some(vec![1, 0]),
            full: false,
        };
        let ch = s.insert(&Tuple(vec![t("x|y"), t("5")]));
        assert_eq!(ch.events[0].0, "5|x\\|y");
        assert_eq!(ch.events[0].1.key, r#"{"a":5,"b":"x|y"}"#);
        assert_eq!(ch.touched, vec![("5\0x|y".to_string(), Eff::Put)]);
    }

    /// Snapshot rows (simple-protocol text) and stream rows (pgoutput text)
    /// go through the same functions: byte-identical `key` and `after`.
    #[test]
    fn a_snapshot_row_and_an_insert_render_the_same_after() {
        let s = orders();
        let ch = s.insert(&Tuple(vec![t("9"), t("a"), t("0.10"), Datum::Null]));
        let snap_vals = [Val::Text("9"), Val::Text("a"), Val::Text("0.10"), Val::Null];
        assert_eq!(
            ch.events[0].1.after.as_deref(),
            Some(object_all(&s.cols, &snap_vals, None).as_str())
        );
        assert_eq!(
            ch.events[0].1.key,
            object_at(&s.cols, &snap_vals, &s.key, None)
        );
    }
}
