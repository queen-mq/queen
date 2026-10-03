//! The four modes (PLAN §5.3): what a message writes, and the statements
//! that write it.
//!
//! Everything here is pure: a [`Target`] (the table as the catalog describes
//! it, plus the key and the metadata columns of the document) turns each
//! message into [`Op`]s — one statement and its TEXT parameters — and renders
//! the SQL of each statement shape. `engine.rs` runs the ops inside the batch's
//! transaction.
//!
//! Three choices shape the statements:
//!
//! * **JSON in, the server converts.** A row travels as ONE text parameter
//!   holding a JSON object, cast `$1::text::jsonb` (tokio-postgres binds a
//!   `&str` only to text-like parameters) and spread over the table's row type
//!   by `jsonb_populate_record(NULL::t, …)`. Each value is converted by the
//!   column's own type input, from the exact characters the producer wrote:
//!   a 30-digit numeric keeps its digits, a nested object lands in a jsonb
//!   column as itself. Values are spliced from the raw payload
//!   ([`RawValue`]), never re-encoded through an `f64`.
//! * **Only the columns the payload names.** `<cols>` is the payload's keys
//!   that are columns of the table (generated columns excluded), plus the
//!   metadata columns. A column the payload does not name keeps its value on
//!   an upsert and gets its DEFAULT on an insert — never a NULL nobody sent.
//!   Each distinct column set is its own statement, prepared once per
//!   connection (the engine's cache, keyed by [`StmtKind`] and the set).
//! * **`OVERRIDING SYSTEM VALUE`** on every INSERT: a payload that carries the
//!   value of a `GENERATED ALWAYS AS IDENTITY` column (a replicated primary
//!   key, typically) writes that value instead of failing every message with
//!   428C9. A payload that does not name the column still gets a generated one.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use serde::Deserialize;
use serde_json::value::RawValue;

use super::params::{self, MessageRef, Meta, Param};
use crate::config::{MetadataColumns, SinkMode};
use crate::error::{Error, Result};
use crate::pg::catalog::{quote_ident, TableInfo, TableName};
use crate::values::append_json_string;

/// The longest JSON document one append statement carries: a run of rows
/// longer than this is split. jsonb's own ceiling is far above (255 MiB), but
/// one huge parameter is held twice by the server while it parses, and a
/// statement this size already amortizes its round trip completely.
pub const MAX_STATEMENT_JSON_BYTES: usize = 16 * 1024 * 1024;

/// Where a metadata column's value comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaSource {
    Field(Meta),
    /// `metadata.payload`: the whole payload.
    Payload,
}

/// A writable column (live, not generated).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub quoted: String,
}

/// The target table as the sink writes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub table: TableName,
    /// `"schema"."name"`.
    pub quoted: String,
    /// Writable columns in attnum order; every column set is a sorted list of
    /// indices into this.
    pub columns: Vec<Column>,
    index: HashMap<String, usize>,
    /// upsert/cdc: the conflict key (configured, else the table's primary
    /// key, else its replica identity index), in key order.
    pub key: Vec<usize>,
    /// append/upsert: the metadata columns.
    pub meta: Vec<(MetaSource, usize)>,
    /// Per column: written from the message's metadata, never from a payload
    /// key of the same name.
    from_meta: Vec<bool>,
}

/// The statement shapes whose SQL depends on the column set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StmtKind {
    Append,
    Upsert,
    Move,
}

/// One row a message writes: the column set (ascending indices) and the JSON
/// object (`{"col": value, …}`) that carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub cols: Vec<usize>,
    pub json: String,
}

/// One statement of a batch, with the messages it applies (positions in the
/// batch's message list).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// `append_sql(cols)` with a JSON array of rows.
    Append {
        cols: Vec<usize>,
        json: String,
        msgs: Vec<usize>,
    },
    /// `upsert_sql(cols)` with one row.
    Upsert { row: Row, msg: usize },
    /// cdc `u` whose key changed: `move_sql(row.cols)` with the new row and
    /// the old key; when it moved nothing (the old row is not in the table),
    /// the upsert of the new row.
    Move {
        row: Row,
        old_key: String,
        msg: usize,
    },
    /// cdc `d`: `delete_sql()` with the key.
    Delete { key: String, msg: usize },
    /// cdc `t`.
    Truncate { msg: usize },
    /// sql mode: the statement with these parameters.
    Sql {
        params: Vec<Option<String>>,
        msg: usize,
    },
}

impl Op {
    /// The messages this op applies.
    pub fn msgs(&self) -> &[usize] {
        match self {
            Op::Append { msgs, .. } => msgs,
            Op::Upsert { msg, .. }
            | Op::Move { msg, .. }
            | Op::Delete { msg, .. }
            | Op::Truncate { msg }
            | Op::Sql { msg, .. } => std::slice::from_ref(msg),
        }
    }
}

/// A message that cannot be applied for what it carries (not for a database
/// failure): the text the DLQ gets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadError {
    pub msg: usize,
    pub text: String,
}

impl Target {
    /// The plan of `info` for `mode`. Fails (Fatal) when the table cannot
    /// take this mode: no key for upsert/cdc, a key or metadata column that is
    /// not a writable column.
    pub fn build(
        info: &TableInfo,
        mode: SinkMode,
        key: Option<&[String]>,
        metadata: Option<&MetadataColumns>,
    ) -> Result<Target> {
        let columns: Vec<Column> = info
            .columns
            .iter()
            .filter(|c| !c.generated)
            .map(|c| Column {
                name: c.name.clone(),
                quoted: quote_ident(&c.name),
            })
            .collect();
        let index: HashMap<String, usize> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        let table = info.name.clone();
        let mut key_idx = Vec::new();
        if matches!(mode, SinkMode::Upsert | SinkMode::Cdc) {
            let names: Vec<String> = match key {
                Some(k) if !k.is_empty() => k.to_vec(),
                _ => info.key_columns().map(<[String]>::to_vec).ok_or_else(|| {
                    Error::fatal(
                        "no_key",
                        format!(
                            "{table} has no primary key (nor a replica identity index): set \
                             sink.key to the columns that identify a row"
                        ),
                    )
                })?,
            };
            for n in &names {
                let i = *index.get(n).ok_or_else(|| {
                    Error::fatal(
                        "no_key",
                        format!(
                            "key column {} is not a writable column of {table}",
                            quote_ident(n)
                        ),
                    )
                })?;
                if key_idx.contains(&i) {
                    return Err(Error::fatal(
                        "no_key",
                        format!("key column {} is listed twice", quote_ident(n)),
                    ));
                }
                key_idx.push(i);
            }
        }
        let mut meta = Vec::new();
        let mut from_meta = vec![false; columns.len()];
        if matches!(mode, SinkMode::Append | SinkMode::Upsert) {
            if let Some(m) = metadata {
                let fields = [
                    (MetaSource::Field(Meta::Partition), &m.partition),
                    (MetaSource::Field(Meta::PartitionId), &m.partition_id),
                    (MetaSource::Field(Meta::Offset), &m.offset),
                    (MetaSource::Field(Meta::TransactionId), &m.transaction_id),
                    (MetaSource::Field(Meta::CreatedAt), &m.created_at),
                    (MetaSource::Payload, &m.payload),
                ];
                for (src, col) in fields {
                    let Some(col) = col else { continue };
                    let i = *index.get(col).ok_or_else(|| {
                        Error::fatal(
                            "metadata_column",
                            format!(
                                "metadata column {} is not a writable column of {table}",
                                quote_ident(col)
                            ),
                        )
                    })?;
                    if from_meta[i] {
                        return Err(Error::fatal(
                            "metadata_column",
                            format!("two metadata fields name the column {}", quote_ident(col)),
                        ));
                    }
                    from_meta[i] = true;
                    meta.push((src, i));
                }
            }
        }
        Ok(Target {
            quoted: table.quoted(),
            table,
            columns,
            index,
            key: key_idx,
            meta,
            from_meta,
        })
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.index.get(name).copied()
    }

    fn payload_mapped(&self) -> bool {
        self.meta.iter().any(|(s, _)| *s == MetaSource::Payload)
    }

    fn quoted_list(&self, cols: &[usize], prefix: &str) -> String {
        let mut s = String::new();
        for (n, &i) in cols.iter().enumerate() {
            if n > 0 {
                s.push_str(", ");
            }
            s.push_str(prefix);
            s.push_str(&self.columns[i].quoted);
        }
        s
    }

    fn key_match(&self, left: &str, right: &str) -> String {
        let mut s = String::new();
        for (n, &i) in self.key.iter().enumerate() {
            if n > 0 {
                s.push_str(" AND ");
            }
            let c = &self.columns[i].quoted;
            let _ = write!(s, "{left}.{c} = {right}.{c}");
        }
        s
    }

    /// append: one statement per run of rows with the same column set.
    pub fn append_sql(&self, cols: &[usize]) -> String {
        format!(
            "INSERT INTO {t} ({c}) OVERRIDING SYSTEM VALUE SELECT {r} \
             FROM jsonb_populate_recordset(NULL::{t}, $1::text::jsonb) AS q_src",
            t = self.quoted,
            c = self.quoted_list(cols, ""),
            r = self.quoted_list(cols, "q_src."),
        )
    }

    /// upsert (and cdc `c`/`r`/`u`): one row, the columns it names; the
    /// non-key ones replace the stored values, the others are left alone.
    pub fn upsert_sql(&self, cols: &[usize]) -> String {
        self.upsert_sql_from(cols, "$1::text::jsonb")
    }

    /// The upsert of the key columns alone, from a NULL row, under `EXPLAIN`:
    /// planned, never executed. The planner is where ON CONFLICT looks for
    /// a unique index matching the key (preparing only parses), so this is
    /// how the sink learns at start that its key has none — rather than at
    /// the first message, as an error that stalls it.
    pub fn explain_key_sql(&self) -> String {
        format!("EXPLAIN {}", self.upsert_sql_from(&self.key, "NULL::jsonb"))
    }

    fn upsert_sql_from(&self, cols: &[usize], src: &str) -> String {
        let set: Vec<usize> = cols
            .iter()
            .copied()
            .filter(|i| !self.key.contains(i))
            .collect();
        let action = if set.is_empty() {
            "DO NOTHING".to_string()
        } else {
            let mut a = String::from("DO UPDATE SET ");
            for (n, &i) in set.iter().enumerate() {
                if n > 0 {
                    a.push_str(", ");
                }
                let c = &self.columns[i].quoted;
                let _ = write!(a, "{c} = EXCLUDED.{c}");
            }
            a
        };
        format!(
            "INSERT INTO {t} ({c}) OVERRIDING SYSTEM VALUE SELECT {r} \
             FROM jsonb_populate_record(NULL::{t}, {src}) AS q_src \
             ON CONFLICT ({k}) {action}",
            t = self.quoted,
            c = self.quoted_list(cols, ""),
            r = self.quoted_list(cols, "q_src."),
            k = self.quoted_list(&self.key, ""),
        )
    }

    /// cdc `u` with a changed key: the stored row moves to the new key and
    /// takes the new values of the columns the event carries; the others —
    /// an unchanged TOAST value, a column only the sink table has — stay as
    /// stored. (A delete of the old key followed by an insert of the new row
    /// would lose them.) `$1` the new row, `$2` the old key.
    pub fn move_sql(&self, cols: &[usize]) -> String {
        let mut set = String::new();
        for (n, &i) in cols.iter().enumerate() {
            if n > 0 {
                set.push_str(", ");
            }
            let c = &self.columns[i].quoted;
            let _ = write!(set, "{c} = q_new.{c}");
        }
        format!(
            "UPDATE {t} AS q_dst SET {set} \
             FROM jsonb_populate_record(NULL::{t}, $1::text::jsonb) AS q_new, \
             jsonb_populate_record(NULL::{t}, $2::text::jsonb) AS q_old \
             WHERE {m}",
            t = self.quoted,
            m = self.key_match("q_dst", "q_old"),
        )
    }

    /// cdc `d`: `$1` the key.
    pub fn delete_sql(&self) -> String {
        format!(
            "DELETE FROM {t} AS q_dst \
             USING jsonb_populate_record(NULL::{t}, $1::text::jsonb) AS q_old WHERE {m}",
            t = self.quoted,
            m = self.key_match("q_dst", "q_old"),
        )
    }

    /// cdc `t`.
    pub fn truncate_sql(&self) -> String {
        format!("TRUNCATE {}", self.quoted)
    }

    /// The SQL of a cached statement shape.
    pub fn sql(&self, kind: StmtKind, cols: &[usize]) -> String {
        match kind {
            StmtKind::Append => self.append_sql(cols),
            StmtKind::Upsert => self.upsert_sql(cols),
            StmtKind::Move => self.move_sql(cols),
        }
    }

    /// append/upsert: the row `m` writes. With `need_key`, every key column
    /// must be in it (an upsert without its key would insert a stray row, or
    /// fail on NOT NULL, depending on defaults — say it plainly instead).
    pub fn message_row(
        &self,
        m: &MessageRef<'_>,
        need_key: bool,
    ) -> std::result::Result<Row, String> {
        let mut vals: BTreeMap<usize, String> = BTreeMap::new();
        match parse_object(m.payload) {
            Some(obj) => {
                for (k, v) in obj {
                    if let Some(i) = self.column_index(&k) {
                        if !self.from_meta[i] {
                            vals.insert(i, v.get().to_string());
                        }
                    }
                }
            }
            None if self.payload_mapped() => {}
            None => {
                return Err(format!(
                    "payload is not a JSON object ({}); map it whole with metadata.payload",
                    kind_of(m.payload)
                ))
            }
        }
        for (src, i) in &self.meta {
            vals.insert(*i, meta_json(*src, m));
        }
        if vals.is_empty() {
            return Err(format!("payload names no column of {}", self.table));
        }
        if need_key {
            self.require_key(&vals, "payload")?;
        }
        Ok(self.row(vals))
    }

    fn require_key(
        &self,
        vals: &BTreeMap<usize, String>,
        what: &str,
    ) -> std::result::Result<(), String> {
        let missing: Vec<String> = self
            .key
            .iter()
            .filter(|i| !vals.contains_key(i))
            .map(|&i| self.columns[i].quoted.clone())
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(format!("{what} lacks key column(s) {}", missing.join(", ")))
        }
    }

    fn row(&self, vals: BTreeMap<usize, String>) -> Row {
        let mut json =
            String::with_capacity(2 + vals.values().map(|v| v.len() + 16).sum::<usize>());
        json.push('{');
        let mut cols = Vec::with_capacity(vals.len());
        for (n, (i, v)) in vals.into_iter().enumerate() {
            if n > 0 {
                json.push(',');
            }
            append_json_string(&self.columns[i].name, &mut json);
            json.push(':');
            json.push_str(&v);
            cols.push(i);
        }
        json.push('}');
        Row { cols, json }
    }

    /// The key columns' values of `obj`, as a row (`None` when one is
    /// missing).
    fn key_row(&self, objs: &[&HashMap<String, &RawValue>]) -> Option<Row> {
        let mut vals = BTreeMap::new();
        for &i in &self.key {
            let name = &self.columns[i].name;
            let v = objs.iter().find_map(|o| o.get(name.as_str()))?;
            vals.insert(i, v.get().to_string());
        }
        Some(self.row(vals))
    }

    /// cdc: what one source event does (PLAN §4.6 envelope).
    pub fn cdc_action(&self, payload: &RawValue) -> std::result::Result<CdcAction, String> {
        let ev: CdcEvent<'_> = serde_json::from_str(payload.get())
            .map_err(|e| format!("not a cdc event ({}): {e}", kind_of(payload)))?;
        let key = event_object(ev.key, "key")?;
        let before = event_object(ev.before, "before")?;
        match ev.op.as_deref() {
            Some("c") | Some("r") | Some("u") => {
                let op = ev.op.as_deref().unwrap_or_default();
                let after = event_object(ev.after, "after")?
                    .ok_or_else(|| format!("cdc '{op}' event without an after image"))?;
                let unchanged = ev.unchanged.unwrap_or_default();
                let mut vals: BTreeMap<usize, String> = BTreeMap::new();
                for (k, v) in &after {
                    if unchanged.iter().any(|u| u == k) {
                        continue;
                    }
                    if let Some(i) = self.column_index(k) {
                        vals.insert(i, v.get().to_string());
                    }
                }
                // A key column missing from the after image (it never is from
                // the source, but the envelope allows it): from `key`.
                if let Some(kobj) = &key {
                    for &i in &self.key {
                        if let Entry::Vacant(slot) = vals.entry(i) {
                            if let Some(v) = kobj.get(self.columns[i].name.as_str()) {
                                slot.insert(v.get().to_string());
                            }
                        }
                    }
                }
                self.require_key(&vals, &format!("cdc '{op}' event"))?;
                let new_key = self
                    .row(self.key.iter().map(|&i| (i, vals[&i].clone())).collect())
                    .json;
                let row = self.row(vals);
                if op == "u" {
                    // `before` with the sink's key columns: the old key (an
                    // update that changed it) or the old row (REPLICA
                    // IDENTITY FULL). The same characters are the same key —
                    // the source renders both images with one converter —
                    // and anything else is treated as a change: the move
                    // statement is right even when the values turn out equal.
                    if let Some(old) = before.as_ref().and_then(|b| self.key_row(&[b])) {
                        if old.json != new_key {
                            return Ok(CdcAction::Move {
                                row,
                                old_key: old.json,
                            });
                        }
                    }
                }
                Ok(CdcAction::Upsert(row))
            }
            Some("d") => {
                let mut objs: Vec<&HashMap<String, &RawValue>> = Vec::new();
                if let Some(k) = &key {
                    objs.push(k);
                }
                if let Some(b) = &before {
                    objs.push(b);
                }
                let row = self.key_row(&objs).ok_or_else(|| {
                    let names: Vec<String> = self
                        .key
                        .iter()
                        .map(|&i| self.columns[i].quoted.clone())
                        .collect();
                    format!(
                        "cdc 'd' event lacks key column(s) {} in key and before",
                        names.join(", ")
                    )
                })?;
                Ok(CdcAction::Delete(row.json))
            }
            Some("t") => Ok(CdcAction::Truncate),
            Some(other) => Err(format!("unknown cdc op {other:?}")),
            None => Err("cdc event without op".to_string()),
        }
    }
}

/// What one cdc event does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CdcAction {
    Upsert(Row),
    Move { row: Row, old_key: String },
    Delete(String),
    Truncate,
}

/// The source's envelope, the fields the sink reads (PLAN §4.6).
#[derive(Deserialize)]
struct CdcEvent<'a> {
    #[serde(default)]
    op: Option<String>,
    #[serde(default, borrow)]
    key: Option<&'a RawValue>,
    #[serde(default, borrow)]
    after: Option<&'a RawValue>,
    #[serde(default, borrow)]
    before: Option<&'a RawValue>,
    #[serde(default)]
    unchanged: Option<Vec<String>>,
}

/// One of an event's images (`key`, `after`, `before`): absent or `null` is
/// `None`, anything but an object a data error.
fn event_object<'a>(
    v: Option<&'a RawValue>,
    what: &str,
) -> std::result::Result<Option<HashMap<String, &'a RawValue>>, String> {
    match v {
        None => Ok(None),
        Some(r) if r.get().trim() == "null" => Ok(None),
        Some(r) => parse_object(r)
            .map(Some)
            .ok_or_else(|| format!("cdc event's {what} is not an object")),
    }
}

/// A payload's top-level object, key → raw value (a repeated key keeps its
/// last value, as jsonb does); `None` when the payload is not an object.
pub fn parse_object(raw: &RawValue) -> Option<HashMap<String, &RawValue>> {
    if !raw.get().trim_start().starts_with('{') {
        return None;
    }
    serde_json::from_str(raw.get()).ok()
}

fn kind_of(raw: &RawValue) -> &'static str {
    match raw.get().trim_start().as_bytes().first() {
        Some(b'{') => "an object",
        Some(b'[') => "an array",
        Some(b'"') => "a string",
        Some(b'n') => "null",
        Some(b't') | Some(b'f') => "a boolean",
        _ => "a number",
    }
}

/// A metadata column's value as JSON.
fn meta_json(src: MetaSource, m: &MessageRef<'_>) -> String {
    let mut s = String::new();
    match src {
        MetaSource::Payload => s.push_str(m.payload.get()),
        MetaSource::Field(Meta::Offset) => {
            let _ = write!(s, "{}", m.offset);
        }
        // Numeric as the broker sends it: a JSON number, so a bigint column
        // takes it as is (a text column takes its digits either way).
        MetaSource::Field(Meta::PartitionId) if m.partition_id.parse::<i64>().is_ok() => {
            let _ = write!(s, "{}", m.partition_id.parse::<i64>().unwrap_or_default());
        }
        MetaSource::Field(Meta::CreatedAt) if m.created_at.is_empty() => s.push_str("null"),
        MetaSource::Field(f) => append_json_string(&m.meta_text(f).unwrap_or_default(), &mut s),
    }
    s
}

/// The ops that apply `msgs` (positions, in apply order) in `mode`. A
/// message that cannot be applied for its content stops the build: the
/// engine isolates it.
pub fn build_ops(
    target: &Target,
    mode: SinkMode,
    sql_params: &[Param],
    msgs: &[(usize, MessageRef<'_>)],
) -> std::result::Result<Vec<Op>, PayloadError> {
    let mut ops = Vec::with_capacity(msgs.len());
    match mode {
        SinkMode::Append => {
            // Consecutive rows with the same column set share a statement
            // (one jsonb_populate_recordset), so the rows go in in order —
            // serial and identity defaults follow the queue — while a uniform
            // queue costs one statement per batch.
            let mut run: Option<(Vec<usize>, String, Vec<usize>)> = None;
            for (pos, m) in msgs {
                let row = target
                    .message_row(m, false)
                    .map_err(|text| PayloadError { msg: *pos, text })?;
                match &mut run {
                    Some((cols, json, members))
                        if *cols == row.cols
                            && json.len() + row.json.len() < MAX_STATEMENT_JSON_BYTES =>
                    {
                        json.push(',');
                        json.push_str(&row.json);
                        members.push(*pos);
                    }
                    _ => {
                        if let Some((cols, mut json, members)) = run.take() {
                            json.push(']');
                            ops.push(Op::Append {
                                cols,
                                json,
                                msgs: members,
                            });
                        }
                        let mut json = String::with_capacity(row.json.len() + 2);
                        json.push('[');
                        json.push_str(&row.json);
                        run = Some((row.cols, json, vec![*pos]));
                    }
                }
            }
            if let Some((cols, mut json, members)) = run {
                json.push(']');
                ops.push(Op::Append {
                    cols,
                    json,
                    msgs: members,
                });
            }
        }
        SinkMode::Upsert => {
            for (pos, m) in msgs {
                let row = target
                    .message_row(m, true)
                    .map_err(|text| PayloadError { msg: *pos, text })?;
                ops.push(Op::Upsert { row, msg: *pos });
            }
        }
        SinkMode::Cdc => {
            for (pos, m) in msgs {
                let a = target
                    .cdc_action(m.payload)
                    .map_err(|text| PayloadError { msg: *pos, text })?;
                ops.push(match a {
                    CdcAction::Upsert(row) => Op::Upsert { row, msg: *pos },
                    CdcAction::Move { row, old_key } => Op::Move {
                        row,
                        old_key,
                        msg: *pos,
                    },
                    CdcAction::Delete(key) => Op::Delete { key, msg: *pos },
                    CdcAction::Truncate => Op::Truncate { msg: *pos },
                });
            }
        }
        SinkMode::Sql => {
            for (pos, m) in msgs {
                ops.push(Op::Sql {
                    params: sql_params.iter().map(|p| params::eval(p, m)).collect(),
                    msg: *pos,
                });
            }
        }
    }
    Ok(ops)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg::catalog::ColumnInfo;

    fn col(name: &str, generated: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            attnum: 0,
            type_oid: 25,
            type_name: "text".into(),
            not_null: false,
            generated,
        }
    }

    fn info(cols: &[&str], pk: &[&str]) -> TableInfo {
        TableInfo {
            oid: 1,
            name: TableName {
                schema: "Sales".into(),
                name: "Orders".into(),
            },
            columns: cols
                .iter()
                .map(|c| col(c.trim_end_matches('*'), c.ends_with('*')))
                .collect(),
            primary_key: pk.iter().map(|s| s.to_string()).collect(),
            replica_identity: b'd',
            identity_index: vec![],
        }
    }

    fn raw(s: &str) -> Box<RawValue> {
        RawValue::from_string(s.to_string()).unwrap()
    }

    fn msg(payload: &RawValue) -> MessageRef<'_> {
        MessageRef {
            payload,
            partition: "p-1",
            partition_id: "77",
            offset: 9,
            transaction_id: "tx",
            created_at: "2026-10-02T10:00:00Z",
        }
    }

    fn upsert_target() -> Target {
        Target::build(
            &info(&["id", "Name", "qty", "total*", "note"], &["id"]),
            SinkMode::Upsert,
            None,
            None,
        )
        .unwrap()
    }

    #[test]
    fn statements_quote_every_identifier_and_keep_case() {
        let t = upsert_target();
        let all: Vec<usize> = (0..t.columns.len()).collect();
        assert_eq!(t.columns.len(), 4, "the generated column is not writable");
        assert_eq!(
            t.append_sql(&[0, 1]),
            "INSERT INTO \"Sales\".\"Orders\" (\"id\", \"Name\") OVERRIDING SYSTEM VALUE \
             SELECT q_src.\"id\", q_src.\"Name\" FROM jsonb_populate_recordset(NULL::\"Sales\".\"Orders\", \
             $1::text::jsonb) AS q_src"
        );
        assert_eq!(
            t.upsert_sql(&all),
            "INSERT INTO \"Sales\".\"Orders\" (\"id\", \"Name\", \"qty\", \"note\") OVERRIDING SYSTEM VALUE \
             SELECT q_src.\"id\", q_src.\"Name\", q_src.\"qty\", q_src.\"note\" \
             FROM jsonb_populate_record(NULL::\"Sales\".\"Orders\", $1::text::jsonb) AS q_src \
             ON CONFLICT (\"id\") DO UPDATE SET \"Name\" = EXCLUDED.\"Name\", \"qty\" = EXCLUDED.\"qty\", \
             \"note\" = EXCLUDED.\"note\""
        );
        assert!(
            t.upsert_sql(&[0])
                .ends_with("ON CONFLICT (\"id\") DO NOTHING"),
            "every column a key column"
        );
        assert_eq!(
            t.delete_sql(),
            "DELETE FROM \"Sales\".\"Orders\" AS q_dst USING jsonb_populate_record(NULL::\"Sales\".\"Orders\", \
             $1::text::jsonb) AS q_old WHERE q_dst.\"id\" = q_old.\"id\""
        );
        assert_eq!(
            t.move_sql(&[0, 2]),
            "UPDATE \"Sales\".\"Orders\" AS q_dst SET \"id\" = q_new.\"id\", \"qty\" = q_new.\"qty\" \
             FROM jsonb_populate_record(NULL::\"Sales\".\"Orders\", $1::text::jsonb) AS q_new, \
             jsonb_populate_record(NULL::\"Sales\".\"Orders\", $2::text::jsonb) AS q_old \
             WHERE q_dst.\"id\" = q_old.\"id\""
        );
        assert_eq!(t.truncate_sql(), "TRUNCATE \"Sales\".\"Orders\"");
        assert_eq!(
            t.explain_key_sql(),
            "EXPLAIN INSERT INTO \"Sales\".\"Orders\" (\"id\") OVERRIDING SYSTEM VALUE SELECT q_src.\"id\" \
             FROM jsonb_populate_record(NULL::\"Sales\".\"Orders\", NULL::jsonb) AS q_src \
             ON CONFLICT (\"id\") DO NOTHING"
        );
    }

    #[test]
    fn a_composite_key_matches_every_column_and_odd_names_are_escaped() {
        let t = Target::build(
            &info(&["a\"b", "k2", "v"], &[]),
            SinkMode::Cdc,
            Some(&["k2".to_string(), "a\"b".to_string()]),
            None,
        )
        .unwrap();
        assert_eq!(t.key, vec![1, 0]);
        assert!(t.delete_sql().ends_with(
            "WHERE q_dst.\"k2\" = q_old.\"k2\" AND q_dst.\"a\"\"b\" = q_old.\"a\"\"b\""
        ));
        assert!(t
            .upsert_sql(&[0, 1, 2])
            .contains("ON CONFLICT (\"k2\", \"a\"\"b\") DO UPDATE SET \"v\" = EXCLUDED.\"v\""));
    }

    #[test]
    fn a_target_without_a_usable_key_is_fatal() {
        let e = Target::build(&info(&["a"], &[]), SinkMode::Upsert, None, None).unwrap_err();
        assert_eq!(e.code(), "no_key");
        let e = Target::build(
            &info(&["a"], &["a"]),
            SinkMode::Cdc,
            Some(&["zz".into()]),
            None,
        )
        .unwrap_err();
        assert_eq!(e.code(), "no_key");
        let e =
            Target::build(&info(&["a", "g*"], &["g"]), SinkMode::Upsert, None, None).unwrap_err();
        assert_eq!(e.code(), "no_key", "a generated column cannot be the key");
        // append and sql need none.
        assert!(Target::build(&info(&["a"], &[]), SinkMode::Append, None, None).is_ok());
        assert!(Target::build(&info(&["a"], &[]), SinkMode::Sql, None, None).is_ok());
        // The replica identity index stands in for a missing primary key.
        let mut i = info(&["a", "b"], &[]);
        i.identity_index = vec!["b".into()];
        assert_eq!(
            Target::build(&i, SinkMode::Upsert, None, None).unwrap().key,
            vec![1]
        );
        let md = MetadataColumns {
            offset: Some("nope".into()),
            ..MetadataColumns::default()
        };
        let e = Target::build(&info(&["a"], &[]), SinkMode::Append, None, Some(&md)).unwrap_err();
        assert_eq!(e.code(), "metadata_column");
    }

    #[test]
    fn upsert_rows_carry_only_the_named_columns_with_exact_values() {
        let t = upsert_target();
        let p = raw(
            r#"{"qty": 12345678901234567890123, "id": 7, "extra": 1, "total": 5, "Name": "é\"x"}"#,
        );
        let row = t.message_row(&msg(&p), true).unwrap();
        assert_eq!(
            row.cols,
            vec![0, 1, 2],
            "payload keys that are writable columns, in table order"
        );
        assert_eq!(
            row.json,
            r#"{"id":7,"Name":"é\"x","qty":12345678901234567890123}"#
        );
        let partial = raw(r#"{"id": 7, "note": null}"#);
        let row = t.message_row(&msg(&partial), true).unwrap();
        assert_eq!(
            row.cols,
            vec![0, 3],
            "an absent column is not in the set: never overwritten"
        );
        let e = t
            .message_row(&msg(&raw(r#"{"qty": 1}"#)), true)
            .unwrap_err();
        assert!(e.contains("lacks key column(s) \"id\""), "{e}");
        let e = t.message_row(&msg(&raw("[1]")), true).unwrap_err();
        assert!(e.contains("not a JSON object (an array)"), "{e}");
        let e = t.message_row(&msg(&raw(r#"{"x": 1}"#)), false).unwrap_err();
        assert!(e.contains("names no column"), "{e}");
    }

    #[test]
    fn metadata_columns_come_from_the_message_and_win_over_the_payload() {
        let md = MetadataColumns {
            offset: Some("q_off".into()),
            partition: Some("q_part".into()),
            partition_id: Some("q_pid".into()),
            transaction_id: Some("q_tx".into()),
            created_at: Some("q_at".into()),
            payload: Some("doc".into()),
        };
        let t = Target::build(
            &info(
                &["v", "q_off", "q_part", "q_pid", "q_tx", "q_at", "doc"],
                &[],
            ),
            SinkMode::Append,
            None,
            Some(&md),
        )
        .unwrap();
        let p = raw(r#"{"v": 1.50, "q_off": "spoofed"}"#);
        let row = t.message_row(&msg(&p), false).unwrap();
        assert_eq!(
            row.json,
            r#"{"v":1.50,"q_off":9,"q_part":"p-1","q_pid":77,"q_tx":"tx","q_at":"2026-10-02T10:00:00Z","doc":{"v": 1.50, "q_off": "spoofed"}}"#
        );
        // Not an object, but the whole payload has a column: only metadata.
        let s = raw(r#""hello""#);
        let row = t.message_row(&msg(&s), false).unwrap();
        assert_eq!(row.cols, vec![1, 2, 3, 4, 5, 6]);
        assert!(row.json.ends_with(r#""doc":"hello"}"#));
    }

    #[test]
    fn append_runs_split_where_the_column_set_changes() {
        let t = Target::build(&info(&["a", "b"], &[]), SinkMode::Append, None, None).unwrap();
        let ps: Vec<Box<RawValue>> = [r#"{"a":1}"#, r#"{"a":2}"#, r#"{"a":3,"b":4}"#, r#"{"a":5}"#]
            .iter()
            .map(|s| raw(s))
            .collect();
        let msgs: Vec<(usize, MessageRef<'_>)> = ps
            .iter()
            .enumerate()
            .map(|(i, p)| (i * 10, msg(p)))
            .collect();
        let ops = build_ops(&t, SinkMode::Append, &[], &msgs).unwrap();
        assert_eq!(
            ops,
            vec![
                Op::Append {
                    cols: vec![0],
                    json: r#"[{"a":1},{"a":2}]"#.into(),
                    msgs: vec![0, 10]
                },
                Op::Append {
                    cols: vec![0, 1],
                    json: r#"[{"a":3,"b":4}]"#.into(),
                    msgs: vec![20]
                },
                Op::Append {
                    cols: vec![0],
                    json: r#"[{"a":5}]"#.into(),
                    msgs: vec![30]
                },
            ]
        );
        let bad = raw("7");
        let e = build_ops(
            &t,
            SinkMode::Append,
            &[],
            &[(0, msg(&ps[0])), (5, msg(&bad))],
        )
        .unwrap_err();
        assert_eq!(e.msg, 5);
    }

    fn cdc_target() -> Target {
        Target::build(
            &info(&["id", "status", "doc"], &["id"]),
            SinkMode::Cdc,
            None,
            None,
        )
        .unwrap()
    }

    fn cdc(s: &str) -> std::result::Result<CdcAction, String> {
        cdc_target().cdc_action(&raw(s))
    }

    #[test]
    fn cdc_events_map_to_their_statements() {
        assert_eq!(
            cdc(
                r#"{"op":"c","table":"public.o","key":{"id":1},"after":{"id":1,"status":"new","doc":"x","gone":1},"before":null,"lsn":"0/1","seq":0}"#
            ),
            Ok(CdcAction::Upsert(Row {
                cols: vec![0, 1, 2],
                json: r#"{"id":1,"status":"new","doc":"x"}"#.into()
            }))
        );
        assert!(matches!(
            cdc(r#"{"op":"r","key":{"id":1},"after":{"id":1}}"#),
            Ok(CdcAction::Upsert(_))
        ));
        // An unchanged TOAST column is not written.
        assert_eq!(
            cdc(
                r#"{"op":"u","key":{"id":1},"after":{"id":1,"status":"paid"},"before":null,"unchanged":["doc"]}"#
            ),
            Ok(CdcAction::Upsert(Row {
                cols: vec![0, 1],
                json: r#"{"id":1,"status":"paid"}"#.into()
            }))
        );
        // A changed key moves the stored row.
        assert_eq!(
            cdc(
                r#"{"op":"u","key":{"id":2},"after":{"id":2,"status":"s"},"before":{"id":1},"unchanged":["doc"]}"#
            ),
            Ok(CdcAction::Move {
                row: Row {
                    cols: vec![0, 1],
                    json: r#"{"id":2,"status":"s"}"#.into()
                },
                old_key: r#"{"id":1}"#.into()
            })
        );
        // REPLICA IDENTITY FULL: before is the old row; same key → upsert.
        assert!(matches!(
            cdc(
                r#"{"op":"u","key":{"id":1},"after":{"id":1,"status":"b"},"before":{"id":1,"status":"a","doc":"d"}}"#
            ),
            Ok(CdcAction::Upsert(_))
        ));
        assert_eq!(
            cdc(r#"{"op":"d","key":{"id":5},"after":null,"before":{"id":5}}"#),
            Ok(CdcAction::Delete(r#"{"id":5}"#.into()))
        );
        assert_eq!(
            cdc(r#"{"op":"d","key":null,"before":{"id":6,"status":"x"}}"#),
            Ok(CdcAction::Delete(r#"{"id":6}"#.into()))
        );
        assert_eq!(
            cdc(r#"{"op":"t","tables":["public.o"],"cascade":false}"#),
            Ok(CdcAction::Truncate)
        );
    }

    #[test]
    fn bad_cdc_events_are_data_errors() {
        assert!(cdc(r#"{"op":"x","after":{"id":1}}"#)
            .unwrap_err()
            .contains("unknown cdc op"));
        assert!(cdc(r#"{"after":{"id":1}}"#)
            .unwrap_err()
            .contains("without op"));
        assert!(cdc(r#"[1,2]"#)
            .unwrap_err()
            .contains("not a cdc event (an array)"));
        assert!(cdc(r#"{"op":"c","after":null}"#)
            .unwrap_err()
            .contains("without an after image"));
        assert!(cdc(r#"{"op":"c","after":{"status":"x"}}"#)
            .unwrap_err()
            .contains("lacks key column(s) \"id\""));
        assert!(cdc(r#"{"op":"d","key":{"other":1}}"#)
            .unwrap_err()
            .contains("lacks key column(s)"));
        assert!(cdc(r#"{"op":"u","after":[1]}"#)
            .unwrap_err()
            .contains("after is not an object"));
    }

    #[test]
    fn sql_ops_carry_text_params() {
        let t = Target::build(&info(&["a"], &[]), SinkMode::Sql, None, None).unwrap();
        let p = raw(r#"{"amount":"10.50","account_id":12345678901234567}"#);
        let params = vec![
            Param::parse("$.amount").unwrap(),
            Param::parse("$.account_id").unwrap(),
            Param::parse("$.missing").unwrap(),
            Param::parse("@offset").unwrap(),
        ];
        let ops = build_ops(&t, SinkMode::Sql, &params, &[(3, msg(&p))]).unwrap();
        assert_eq!(
            ops,
            vec![Op::Sql {
                params: vec![
                    Some("10.50".into()),
                    Some("12345678901234567".into()),
                    None,
                    Some("9".into())
                ],
                msg: 3
            }]
        );
    }
}
