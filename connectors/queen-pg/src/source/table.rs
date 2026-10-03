//! One configured table, resolved against the catalog at start (PLAN §4.3
//! step 3), and the shape the decoder gives each pgoutput Relation.
//!
//! The key the source chunks the snapshot by and partitions by default is the
//! replica identity's key: the identity index under `REPLICA IDENTITY USING
//! INDEX` (the old tuple of a delete carries exactly those columns), else the
//! primary key. `NOTHING`, or no key at all, is refused: deletes could not
//! name their row and the snapshot could not walk the table.

use std::collections::HashMap;
use std::sync::Arc;

use crate::config::TableSpec;
use crate::error::{Error, Result};
use crate::pg::catalog::{TableInfo, TableName};
use crate::repl::pgoutput::Relation;

use super::events::{Col, PartitionRule, Shape, TypeConv};

/// A configured table as the source runs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableState {
    /// `schema.name`, as events and the pointer spell it.
    pub name: String,
    pub table: TableName,
    pub oid: u32,
    pub queue: Arc<str>,
    pub rule: PartitionRule,
    /// `relreplident`.
    pub identity: u8,
    /// The key's column names, in key order.
    pub key: Vec<String>,
    /// The snapshot's columns: published, not generated, in attnum order —
    /// exactly the columns pgoutput sends, so `r` and `c` events match.
    pub cols: Vec<Col>,
    /// `format_type` of each of `cols`: the casts of the chunk's key bounds.
    pub type_names: Vec<String>,
    /// Positions of the key columns in `cols`, key order.
    pub key_pos: Vec<usize>,
    /// Positions of the partition columns in `cols`; `None` = `single`.
    pub part_pos: Option<Vec<usize>>,
}

fn identity_fix(t: &TableName) -> String {
    format!(
        "ALTER TABLE {} REPLICA IDENTITY DEFAULT (with a primary key), USING INDEX <a unique \
         index on NOT NULL columns>, or FULL (with a primary key)",
        t.quoted()
    )
}

/// Resolve `spec` against `info`. `published` is the publication's column
/// list for the table when it has one (`pg_publication_tables.attnames`);
/// `conv` the converter of each type OID (domains and non-builtin arrays
/// resolved by the caller).
pub fn resolve(
    spec: &TableSpec,
    info: &TableInfo,
    published: Option<&[String]>,
    conv: &HashMap<u32, TypeConv>,
) -> Result<TableState> {
    let t = &info.name;
    let key: Vec<String> = match info.replica_identity {
        b'n' => {
            return Err(Error::fatal(
                "replica_identity",
                format!(
                    "{t} has REPLICA IDENTITY NOTHING: its updates and deletes cannot name their \
                     row; {}",
                    identity_fix(t)
                ),
            ))
        }
        b'i' => info.identity_index.clone(),
        _ => info.primary_key.clone(),
    };
    if key.is_empty() {
        return Err(Error::fatal(
            "replica_identity",
            format!(
                "{t} has no key the source can use (no primary key{}); {}",
                if info.replica_identity == b'f' {
                    " under REPLICA IDENTITY FULL"
                } else {
                    ""
                },
                identity_fix(t)
            ),
        ));
    }
    let cols_info: Vec<_> = info
        .columns
        .iter()
        .filter(|c| !c.generated)
        .filter(|c| published.is_none_or(|p| p.contains(&c.name)))
        .collect();
    let cols: Vec<Col> = cols_info
        .iter()
        .map(|c| Col {
            name: c.name.clone(),
            conv: conv
                .get(&c.type_oid)
                .copied()
                .unwrap_or_else(|| TypeConv::builtin(c.type_oid)),
        })
        .collect();
    let type_names = cols_info.iter().map(|c| c.type_name.clone()).collect();
    let pos_of = |n: &str| cols.iter().position(|c| c.name == n);
    let mut key_pos = Vec::with_capacity(key.len());
    for k in &key {
        match pos_of(k) {
            Some(p) => key_pos.push(p),
            None => {
                return Err(Error::fatal(
                    "publication",
                    format!(
                        "the key column {k:?} of {t} is not published (a column list leaves it \
                         out); publish every key column"
                    ),
                ))
            }
        }
    }
    let rule = PartitionRule::from_config(&spec.partition_by);
    let part_pos = match rule.columns(&key) {
        None => None,
        Some(names) => {
            let mut v = Vec::with_capacity(names.len());
            for n in names {
                let Some(p) = pos_of(n) else {
                    return Err(Error::fatal(
                        "partition_by",
                        format!("partitionBy names {n:?}, which is not a published column of {t}"),
                    ));
                };
                if info.replica_identity != b'f' && !key.contains(n) {
                    return Err(Error::fatal(
                        "partition_by",
                        format!(
                            "partitionBy column {n:?} of {t} is not part of its replica identity \
                             ({}), so a delete could not say which partition its row was in; \
                             partition by key columns, or ALTER TABLE {} REPLICA IDENTITY FULL",
                            key.join(", "),
                            t.quoted()
                        ),
                    ));
                }
                v.push(p);
            }
            Some(v)
        }
    };
    Ok(TableState {
        name: t.to_string(),
        table: t.clone(),
        oid: info.oid,
        queue: Arc::from(spec.queue.as_str()),
        rule,
        identity: info.replica_identity,
        key,
        cols,
        type_names,
        key_pos,
        part_pos,
    })
}

/// The decoder's view of a Relation message for table `idx`. The key comes
/// from the relation's own identity flags (they describe the table AS OF that
/// point in the WAL), in the configured key's order where they agree; under
/// `FULL` every column is flagged, so the configured key is used.
pub fn shape(
    idx: usize,
    t: &TableState,
    rel: &Relation,
    conv: &HashMap<u32, TypeConv>,
) -> Result<Shape> {
    let cols: Vec<Col> = rel
        .columns
        .iter()
        .map(|c| Col {
            name: c.name.clone(),
            conv: conv
                .get(&c.type_oid)
                .copied()
                .unwrap_or_else(|| TypeConv::builtin(c.type_oid)),
        })
        .collect();
    let pos_of = |n: &str| rel.columns.iter().position(|c| c.name == n);
    let full = rel.replica_identity == b'f';
    let key: Vec<usize> = if full {
        t.key.iter().filter_map(|k| pos_of(k)).collect()
    } else {
        let flagged: Vec<usize> = (0..rel.columns.len())
            .filter(|&i| rel.columns[i].key)
            .collect();
        let mut ordered: Vec<usize> = t
            .key
            .iter()
            .filter_map(|k| pos_of(k))
            .filter(|p| flagged.contains(p))
            .collect();
        for p in flagged {
            if !ordered.contains(&p) {
                ordered.push(p);
            }
        }
        ordered
    };
    if key.is_empty() || (full && key.len() != t.key.len()) {
        return Err(Error::fatal(
            "relation_changed",
            format!(
                "the stream describes {}.{} without its key columns ({}); the table changed \
                 under the source — fix the table or resync",
                rel.namespace,
                rel.name,
                t.key.join(", ")
            ),
        ));
    }
    let key_names: Vec<String> = key.iter().map(|&i| rel.columns[i].name.clone()).collect();
    let part = match t.rule.columns(&key_names) {
        None => None,
        Some(names) => {
            let mut v = Vec::with_capacity(names.len());
            for n in names {
                match pos_of(n) {
                    Some(p) => v.push(p),
                    None => {
                        return Err(Error::fatal(
                            "relation_changed",
                            format!(
                                "the stream describes {}.{} without its partition column {n:?}",
                                rel.namespace, rel.name
                            ),
                        ))
                    }
                }
            }
            Some(v)
        }
    };
    Ok(Shape {
        table: idx,
        cols,
        key,
        part,
        full,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{PartitionBy, PartitionMode};
    use crate::pg::catalog::ColumnInfo;
    use crate::repl::pgoutput::RelColumn;

    fn info(identity: u8, pk: &[&str], idx: &[&str]) -> TableInfo {
        let col = |n: &str, attnum: i16, oid: u32, generated: bool| ColumnInfo {
            name: n.into(),
            attnum,
            type_oid: oid,
            type_name: if oid == 20 {
                "bigint".into()
            } else {
                "text".into()
            },
            not_null: true,
            generated,
        };
        TableInfo {
            oid: 16390,
            name: TableName {
                schema: "public".into(),
                name: "orders".into(),
            },
            columns: vec![
                col("id", 1, 20, false),
                col("region", 2, 25, false),
                col("status", 3, 25, false),
                col("upper_status", 4, 25, true),
            ],
            primary_key: pk.iter().map(|s| s.to_string()).collect(),
            replica_identity: identity,
            identity_index: idx.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn spec(p: PartitionBy) -> TableSpec {
        TableSpec {
            table: "public.orders".into(),
            queue: "orders".into(),
            partition_by: p,
        }
    }

    #[test]
    fn the_key_is_the_identity_index_or_the_primary_key() {
        let none = HashMap::new();
        let t = resolve(
            &spec(PartitionBy::Default),
            &info(b'd', &["id"], &[]),
            None,
            &none,
        )
        .unwrap();
        assert_eq!(t.key, vec!["id"]);
        assert_eq!(t.key_pos, vec![0]);
        assert_eq!(t.part_pos, Some(vec![0]));
        assert_eq!(t.cols.len(), 3, "the generated column is not published");
        assert_eq!(t.name, "public.orders");
        let t = resolve(
            &spec(PartitionBy::Default),
            &info(b'i', &["id"], &["region", "status"]),
            None,
            &none,
        )
        .unwrap();
        assert_eq!(
            t.key,
            vec!["region", "status"],
            "USING INDEX wins over the pk"
        );
        assert_eq!(t.key_pos, vec![1, 2]);
        for (ri, pk) in [(b'n', &["id"][..]), (b'd', &[][..]), (b'f', &[][..])] {
            let e =
                resolve(&spec(PartitionBy::Default), &info(ri, pk, &[]), None, &none).unwrap_err();
            assert_eq!(e.code(), "replica_identity", "{e}");
        }
    }

    #[test]
    fn partition_columns_must_be_in_the_identity_unless_full() {
        let none = HashMap::new();
        let by_region = PartitionBy::Columns(vec!["region".into()]);
        let e = resolve(
            &spec(by_region.clone()),
            &info(b'd', &["id"], &[]),
            None,
            &none,
        )
        .unwrap_err();
        assert_eq!(e.code(), "partition_by");
        let t = resolve(&spec(by_region), &info(b'f', &["id"], &[]), None, &none).unwrap();
        assert_eq!(t.part_pos, Some(vec![1]));
        let e = resolve(
            &spec(PartitionBy::Columns(vec!["nope".into()])),
            &info(b'f', &["id"], &[]),
            None,
            &none,
        )
        .unwrap_err();
        assert_eq!(e.code(), "partition_by");
        let t = resolve(
            &spec(PartitionBy::Mode(PartitionMode::Single)),
            &info(b'd', &["id"], &[]),
            None,
            &none,
        )
        .unwrap();
        assert_eq!(t.part_pos, None);
    }

    #[test]
    fn a_column_list_narrows_the_columns_but_must_keep_the_key() {
        let none = HashMap::new();
        let published = vec!["id".to_string(), "status".to_string()];
        let t = resolve(
            &spec(PartitionBy::Default),
            &info(b'd', &["id"], &[]),
            Some(&published),
            &none,
        )
        .unwrap();
        assert_eq!(
            t.cols.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            vec!["id", "status"]
        );
        let published = vec!["status".to_string()];
        let e = resolve(
            &spec(PartitionBy::Default),
            &info(b'd', &["id"], &[]),
            Some(&published),
            &none,
        )
        .unwrap_err();
        assert_eq!(e.code(), "publication");
    }

    #[test]
    fn a_relation_shape_follows_the_streamed_identity() {
        let none = HashMap::new();
        let t = resolve(
            &spec(PartitionBy::Default),
            &info(b'd', &["status", "id"], &[]),
            None,
            &none,
        )
        .unwrap();
        let rc = |n: &str, key: bool| RelColumn {
            key,
            name: n.into(),
            type_oid: 25,
            type_mod: -1,
        };
        let rel = Relation {
            id: 16390,
            namespace: "public".into(),
            name: "orders".into(),
            replica_identity: b'd',
            columns: vec![rc("id", true), rc("region", false), rc("status", true)],
        };
        let s = shape(0, &t, &rel, &none).unwrap();
        assert_eq!(s.key, vec![2, 0], "the configured key order");
        assert_eq!(s.part, Some(vec![2, 0]));
        assert!(!s.full);
        let mut full = rel.clone();
        full.replica_identity = b'f';
        for c in &mut full.columns {
            c.key = true;
        }
        let s = shape(0, &t, &full, &none).unwrap();
        assert_eq!(
            s.key,
            vec![2, 0],
            "under FULL every column is flagged: use the key"
        );
        assert!(s.full);
        let mut gone = rel.clone();
        gone.columns.retain(|c| c.name == "region");
        assert_eq!(
            shape(0, &t, &gone, &none).unwrap_err().code(),
            "relation_changed"
        );
    }
}
