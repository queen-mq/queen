//! A configured table against the catalog, and a binlog table map event
//! against a configured table.

use std::sync::Arc;

use mysql_common::binlog::events::{OptionalMetaExtractor, OptionalMetadataField, TableMapEvent};

use crate::config::TableSpec;
use crate::error::{Error, Result};
use crate::source::events::PartitionRule;

use super::catalog::{ColumnInfo, MyTableName, TableInfo};
use super::values::Kind;

/// One configured table, resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MyTable {
    /// `db.table`: the events' `table`, and the snapshot progress's name.
    pub name: String,
    pub tn: MyTableName,
    pub queue: Arc<str>,
    /// In `information_schema` order (what the snapshot selects).
    pub cols: Vec<ColumnInfo>,
    /// The primary key's positions in `cols`, in key order.
    pub key_pos: Vec<usize>,
    pub rule: PartitionRule,
    /// The partition columns' positions in `cols` (`None`: `single`).
    pub part_pos: Option<Vec<usize>>,
}

impl MyTable {
    pub fn key_names(&self) -> Vec<&str> {
        self.key_pos
            .iter()
            .map(|&i| self.cols[i].name.as_str())
            .collect()
    }
}

fn position(cols: &[ColumnInfo], name: &str) -> Option<usize> {
    cols.iter()
        .position(|c| c.name == name)
        .or_else(|| cols.iter().position(|c| c.name.eq_ignore_ascii_case(name)))
}

pub fn resolve(spec: &TableSpec, info: &TableInfo) -> Result<MyTable> {
    let t = &info.name;
    if info.primary_key.is_empty() {
        return Err(Error::fatal(
            "primary_key",
            format!(
                "the table {t} has no primary key: the source keys every message and every \
                 snapshot chunk by it (`ALTER TABLE {} ADD PRIMARY KEY (…)`)",
                t.quoted()
            ),
        ));
    }
    let mut key_pos = Vec::with_capacity(info.primary_key.len());
    for k in &info.primary_key {
        let p = position(&info.columns, k)
            .ok_or_else(|| Error::io(format!("the primary key column {t}.{k} is not a column")))?;
        if !info.columns[p].kind.keyable() {
            return Err(Error::fatal(
                "key_type",
                format!(
                    "the primary key column {t}.{k} is {}: the snapshot cannot page through a \
                     key of that type",
                    info.columns[p].column_type
                ),
            ));
        }
        key_pos.push(p);
    }
    let rule = PartitionRule::from_config(&spec.partition_by);
    let part_pos = match &rule {
        PartitionRule::Single => None,
        PartitionRule::Key => Some(key_pos.clone()),
        PartitionRule::Columns(cols) => {
            let mut v = Vec::with_capacity(cols.len());
            for c in cols {
                v.push(position(&info.columns, c).ok_or_else(|| {
                    Error::fatal(
                        "partition_by",
                        format!("partitionBy names {c:?}, which is not a column of {t}"),
                    )
                })?);
            }
            Some(v)
        }
    };
    Ok(MyTable {
        name: t.to_string(),
        tn: t.clone(),
        queue: Arc::from(spec.queue.as_str()),
        cols: info.columns.clone(),
        key_pos,
        rule,
        part_pos,
    })
}

/// One column as the binlog describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinCol {
    pub name: String,
    pub kind: Kind,
    /// ENUM and SET values, in definition order.
    pub names: Vec<String>,
}

/// A table map event of a configured table: how to read its rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shape {
    pub table: usize,
    pub cols: Vec<BinCol>,
    /// Key positions in `cols`, in key order.
    pub key: Vec<usize>,
    /// Partition positions in `cols` (`None`: `single`).
    pub part: Option<Vec<usize>>,
}

/// Read a table map event against table `idx`. Columns come by name (the
/// event's `binlog_row_metadata=FULL` names), so a column the table gained
/// after the source started is carried; a key or partition column the event
/// lacks is the table changed under the source.
pub fn shape(idx: usize, t: &MyTable, tme: &TableMapEvent<'_>) -> Result<Shape> {
    let n = tme.columns_count() as usize;
    let bad = |e: std::io::Error| Error::io(format!("the table map of {}: {e}", t.name));
    let mut names: Vec<String> = Vec::with_capacity(n);
    let mut enums: Vec<Vec<String>> = Vec::new();
    let mut sets: Vec<Vec<String>> = Vec::new();
    for field in tme.iter_optional_meta() {
        match field.map_err(bad)? {
            OptionalMetadataField::ColumnName(c) => {
                for name in c.iter_names() {
                    names.push(name.map_err(bad)?.name().into_owned());
                }
            }
            OptionalMetadataField::EnumStrValue(e) => {
                for v in e.iter_values() {
                    let v = v.map_err(bad)?;
                    enums.push(v.values().iter().map(|x| x.value().into_owned()).collect());
                }
            }
            OptionalMetadataField::SetStrValue(s) => {
                for v in s.iter_values() {
                    let v = v.map_err(bad)?;
                    sets.push(v.values().iter().map(|x| x.value().into_owned()).collect());
                }
            }
            _ => {}
        }
    }
    if names.len() != n {
        return Err(Error::fatal(
            "binlog_row_metadata",
            format!(
                "the binlog's table map of {} carries no column names: \
                 binlog_row_metadata must be FULL for every transaction the source reads \
                 (it was MINIMAL when this one was written); resync once it is FULL",
                t.name
            ),
        ));
    }
    let meta = OptionalMetaExtractor::new(tme.iter_optional_meta()).map_err(bad)?;
    let mut charsets = meta.iter_charset();
    let mut es_charsets = meta.iter_enum_and_set_charset();
    let (mut ei, mut si) = (0usize, 0usize);
    let mut cols = Vec::with_capacity(n);
    for (i, name) in names.into_iter().enumerate() {
        let ty = tme
            .get_column_type(i)
            .map_err(|e| Error::io(format!("the table map of {}: {e}", t.name)))?
            .ok_or_else(|| Error::io(format!("the table map of {} lacks column {i}", t.name)))?;
        let collation = if ty.is_character_type() {
            charsets
                .next()
                .transpose()
                .map_err(bad)?
                .unwrap_or_default()
        } else if ty.is_enum_or_set_type() {
            es_charsets
                .next()
                .transpose()
                .map_err(bad)?
                .unwrap_or_default()
        } else {
            0
        };
        let kind = Kind::from_binlog(ty, tme.get_column_metadata(i).unwrap_or(&[]), collation)
            .map_err(|e| match e {
                Error::Fatal { code, message } => {
                    Error::fatal(code, format!("{}.{name}: {message}", t.name))
                }
                other => other,
            })?;
        let names = match kind {
            Kind::Enum => {
                ei += 1;
                enums.get(ei - 1).cloned().unwrap_or_default()
            }
            Kind::Set => {
                si += 1;
                sets.get(si - 1).cloned().unwrap_or_default()
            }
            _ => Vec::new(),
        };
        cols.push(BinCol { name, kind, names });
    }
    let find = |name: &str| {
        cols.iter()
            .position(|c| c.name == name)
            .or_else(|| cols.iter().position(|c| c.name.eq_ignore_ascii_case(name)))
    };
    let changed = |what: &str, c: &str| {
        Error::fatal(
            "table_changed",
            format!(
                "the binlog's {} has no column {c:?} (the {what}): the table changed under \
                 the source; fix the document or resync",
                t.name
            ),
        )
    };
    let mut key = Vec::with_capacity(t.key_pos.len());
    for &p in &t.key_pos {
        let c = &t.cols[p].name;
        key.push(find(c).ok_or_else(|| changed("primary key", c))?);
    }
    let part = match &t.part_pos {
        None => None,
        Some(pp) => {
            let mut v = Vec::with_capacity(pp.len());
            for &p in pp {
                let c = &t.cols[p].name;
                v.push(find(c).ok_or_else(|| changed("partition column", c))?);
            }
            Some(v)
        }
    };
    Ok(Shape {
        table: idx,
        cols,
        key,
        part,
    })
}
