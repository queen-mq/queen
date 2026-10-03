//! The database side of a start (PLAN §4.3): server checks, the publication,
//! the replication slot, and the reads the status needs (slot lag). Every
//! function here is one or two statements on the regular connection; the
//! decisions they feed are in [`super::Source`].

use tokio_postgres::Client;

use crate::error::{Error, Result};
use crate::pg::catalog::{quote_ident, TableName};
use crate::pg::connect::classify;
use crate::repl::Lsn;

/// PostgreSQL 17: failover slots and the five-argument slot function.
pub const MIN_VERSION_NUM: i32 = 170_000;

/// `server_version_num >= 170000` and `wal_level = logical`.
pub fn check_server(version_num: i32, version: &str, wal_level: &str) -> Result<()> {
    if version_num < MIN_VERSION_NUM {
        return Err(Error::fatal(
            "version",
            format!(
                "PostgreSQL {version} is too old: the source needs PostgreSQL 17 or newer \
                 (failover slots); upgrade the database"
            ),
        ));
    }
    if wal_level != "logical" {
        return Err(Error::fatal(
            "wal_level",
            format!(
                "wal_level is {wal_level:?}: logical replication needs wal_level = logical \
                 (ALTER SYSTEM SET wal_level = logical, then restart PostgreSQL)"
            ),
        ));
    }
    Ok(())
}

/// One row of `pg_replication_slots`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotRow {
    pub slot_type: String,
    pub plugin: Option<String>,
    pub database: Option<String>,
    pub active_pid: Option<i32>,
    pub confirmed_flush: Option<Lsn>,
    pub wal_status: Option<String>,
    pub invalidation_reason: Option<String>,
}

fn pg(e: tokio_postgres::Error) -> Error {
    classify(&e)
}

pub async fn read_slot(c: &Client, slot: &str) -> Result<Option<SlotRow>> {
    let r = c
        .query_opt(
            "SELECT slot_type, plugin::text, database::text, active_pid, \
                    confirmed_flush_lsn::text, wal_status, invalidation_reason \
               FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(pg)?;
    Ok(r.map(|r| SlotRow {
        slot_type: r.get(0),
        plugin: r.get(1),
        database: r.get(2),
        active_pid: r.get(3),
        confirmed_flush: r.get::<_, Option<String>>(4).and_then(|s| s.parse().ok()),
        wal_status: r.get(5),
        invalidation_reason: r.get(6),
    }))
}

/// `pg_create_logical_replication_slot(slot, 'pgoutput', false, false, true)`
/// — failover = true (PG 17). The returned LSN is the slot's consistent
/// point: every transaction committed after it will be streamed.
pub async fn create_slot(c: &Client, slot: &str) -> Result<Lsn> {
    let r = c
        .query_one(
            "SELECT lsn::text FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput', \
             false, false, true)",
            &[&slot],
        )
        .await
        .map_err(|e| match classify(&e) {
            // Created by someone else between the read and this call.
            Error::Pg {
                sqlstate: Some(s), ..
            } if s == "42710" => {
                Error::Fenced(format!("the slot {slot} appeared while creating it"))
            }
            other => other,
        })?;
    let s: String = r.get(0);
    s.parse::<Lsn>().map_err(Error::io)
}

/// Drop the slot, terminating a walsender that holds it (a stale consumer:
/// the caller holds the lease). An absent slot is fine.
pub async fn drop_slot(c: &Client, slot: &str) -> Result<()> {
    for attempt in 0..20 {
        let r = c
            .execute(
                "SELECT pg_catalog.pg_drop_replication_slot(slot_name) \
                   FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await;
        match r {
            Ok(_) => return Ok(()),
            Err(e) => match classify(&e) {
                // In use by a walsender: end it and try again.
                Error::Pg {
                    sqlstate: Some(s), ..
                } if s == "55006" && attempt < 19 => {
                    let _ = c
                        .execute(
                            "SELECT pg_catalog.pg_terminate_backend(active_pid) \
                               FROM pg_catalog.pg_replication_slots \
                              WHERE slot_name = $1 AND active_pid IS NOT NULL",
                            &[&slot],
                        )
                        .await;
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
                Error::Pg {
                    sqlstate: Some(s), ..
                } if s == "42704" => return Ok(()),
                other => return Err(other),
            },
        }
    }
    Err(Error::io(format!("the slot {slot} stayed in use")))
}

/// `pg_current_wal_lsn() - confirmed_flush_lsn`, `wal_status`, and the
/// confirmed position, for the status block (`None`: the slot is gone).
pub async fn slot_lag(
    c: &Client,
    slot: &str,
) -> Result<Option<(i64, Option<String>, Option<Lsn>)>> {
    let r = c
        .query_opt(
            "SELECT COALESCE(pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(), \
                    confirmed_flush_lsn), 0)::bigint, wal_status, confirmed_flush_lsn::text \
               FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(pg)?;
    Ok(r.map(|r| {
        (
            r.get(0),
            r.get(1),
            r.get::<_, Option<String>>(2).and_then(|s| s.parse().ok()),
        )
    }))
}

/// The database this connection is in (a slot of another database is not
/// this source's).
pub async fn current_database(c: &Client) -> Result<String> {
    let r = c
        .query_one("SELECT current_database()::text", &[])
        .await
        .map_err(pg)?;
    Ok(r.get(0))
}

/// One table as a publication carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubTable {
    /// `schema.name`.
    pub name: String,
    /// The row filter's expression, when it has one.
    pub rowfilter: Option<String>,
    /// The published columns (`attnames`).
    pub columns: Vec<String>,
}

/// The publication's state: `None` when absent, else whether it is `FOR ALL
/// TABLES` and the tables it carries.
pub async fn read_publication(c: &Client, name: &str) -> Result<Option<(bool, Vec<PubTable>)>> {
    let p = c
        .query_opt(
            "SELECT puballtables FROM pg_catalog.pg_publication WHERE pubname = $1",
            &[&name],
        )
        .await
        .map_err(pg)?;
    let Some(p) = p else {
        return Ok(None);
    };
    let all: bool = p.get(0);
    let rows = c
        .query(
            "SELECT schemaname::text || '.' || tablename::text, rowfilter, attnames::text[] \
               FROM pg_catalog.pg_publication_tables WHERE pubname = $1",
            &[&name],
        )
        .await
        .map_err(pg)?;
    let mut tables: Vec<PubTable> = rows
        .iter()
        .map(|r| PubTable {
            name: r.get(0),
            rowfilter: r.get(1),
            columns: r.get::<_, Option<Vec<String>>>(2).unwrap_or_default(),
        })
        .collect();
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Some((all, tables)))
}

/// Why a publication's view of a table is narrower than the table — a row
/// filter, or a column list leaving out some of `columns` (the table's
/// non-generated columns) — `None` when it carries the whole table. The
/// stream honours such a filter and the snapshot (v1) does not, so the two
/// would disagree.
pub fn narrowing(t: &PubTable, columns: &[String]) -> Option<String> {
    if let Some(f) = &t.rowfilter {
        return Some(format!("a row filter (WHERE {f})"));
    }
    let missing: Vec<&String> = columns.iter().filter(|c| !t.columns.contains(c)).collect();
    if !missing.is_empty() {
        return Some(format!("a column list that leaves out {missing:?}"));
    }
    None
}

/// The publication's columns for one table (`attnames`), `None` when the
/// table is not in it.
pub async fn published_columns(
    c: &Client,
    publication: &str,
    t: &TableName,
) -> Result<Option<Vec<String>>> {
    let r = c
        .query_opt(
            "SELECT attnames::text[] FROM pg_catalog.pg_publication_tables \
              WHERE pubname = $1 AND schemaname = $2 AND tablename = $3",
            &[&publication, &t.schema, &t.name],
        )
        .await
        .map_err(pg)?;
    Ok(r.map(|r| r.get::<_, Option<Vec<String>>>(0).unwrap_or_default()))
}

/// Make a managed publication carry exactly `tables`, whole (a row filter or
/// a column list found on it is reset: `SET TABLE` without them); verify an
/// unmanaged one carries every table, whole — a narrowed table is refused
/// (`publication`), never streamed. `tables` pairs each table with its
/// non-generated columns.
pub async fn ensure_publication(
    c: &Client,
    name: &str,
    tables: &[(TableName, Vec<String>)],
    managed: bool,
) -> Result<()> {
    let mut want: Vec<String> = tables.iter().map(|(t, _)| t.to_string()).collect();
    want.sort();
    want.dedup();
    let list = tables
        .iter()
        .map(|(t, _)| t.quoted())
        .collect::<Vec<_>>()
        .join(", ");
    let Some((all, have)) = read_publication(c, name).await? else {
        if managed {
            c.batch_execute(&format!(
                "CREATE PUBLICATION {} FOR TABLE {list}",
                quote_ident(name)
            ))
            .await
            .map_err(|e| privilege_hint(classify(&e), name))?;
            return Ok(());
        }
        return Err(Error::fatal(
            "publication",
            format!(
                "the publication {name:?} does not exist and managePublication is false; \
                 CREATE PUBLICATION {} FOR TABLE {list}",
                quote_ident(name)
            ),
        ));
    };
    let narrowed: Vec<(String, String)> = tables
        .iter()
        .filter_map(|(t, cols)| {
            let n = t.to_string();
            have.iter()
                .find(|p| p.name == n)
                .and_then(|p| narrowing(p, cols))
                .map(|why| (n, why))
        })
        .collect();
    let have_names: Vec<String> = have.iter().map(|p| p.name.clone()).collect();
    if managed && !all {
        if have_names != want || !narrowed.is_empty() {
            c.batch_execute(&format!(
                "ALTER PUBLICATION {} SET TABLE {list}",
                quote_ident(name)
            ))
            .await
            .map_err(|e| privilege_hint(classify(&e), name))?;
        }
        return Ok(());
    }
    let missing: Vec<&String> = want.iter().filter(|w| !have_names.contains(w)).collect();
    if !missing.is_empty() {
        return Err(Error::fatal(
            "publication",
            format!(
                "the publication {name:?} does not carry {missing:?} (managePublication \
                 is false); ALTER PUBLICATION {} ADD TABLE …",
                quote_ident(name)
            ),
        ));
    }
    if let Some((t, why)) = narrowed.first() {
        return Err(Error::fatal(
            "publication",
            format!(
                "the publication {name:?} carries {t} with {why}: the stream would leave out \
                 what the snapshot reads (publication filters are not applied to snapshots); \
                 publish the whole table (ALTER PUBLICATION {} SET TABLE …) or set \
                 managePublication true",
                quote_ident(name)
            ),
        ));
    }
    Ok(())
}

fn privilege_hint(e: Error, name: &str) -> Error {
    match e {
        Error::Pg {
            sqlstate: Some(s),
            message,
            ..
        } if s == "42501" => Error::fatal(
            "publication",
            format!(
                "cannot manage the publication {name:?} ({message}): the role must own the \
                 tables, or create the publication yourself and set managePublication false"
            ),
        ),
        other => other,
    }
}

/// Drop a managed publication (teardown).
pub async fn drop_publication(c: &Client, name: &str) -> Result<()> {
    c.batch_execute(&format!("DROP PUBLICATION IF EXISTS {}", quote_ident(name)))
        .await
        .map_err(pg)
}

/// Emit one logical message (a watermark or a heartbeat) and return the LSN
/// the function answered.
pub async fn emit(c: &Client, sql: &str) -> Result<Lsn> {
    let msgs = c.simple_query(sql).await.map_err(pg)?;
    for m in &msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            if let Some(s) = r.get(0) {
                return s.parse::<Lsn>().map_err(Error::io);
            }
        }
    }
    Err(Error::io("pg_logical_emit_message answered no row"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_filter_or_a_short_column_list_narrows_a_table() {
        let cols = vec!["id".to_string(), "v".to_string(), "n".to_string()];
        let whole = PubTable {
            name: "public.t".into(),
            rowfilter: None,
            columns: cols.clone(),
        };
        assert_eq!(narrowing(&whole, &cols), None);
        let filtered = PubTable {
            rowfilter: Some("(id > 2)".into()),
            ..whole.clone()
        };
        assert!(narrowing(&filtered, &cols)
            .unwrap()
            .contains("row filter (WHERE (id > 2))"));
        let short = PubTable {
            columns: vec!["id".into(), "v".into()],
            ..whole.clone()
        };
        assert!(narrowing(&short, &cols)
            .unwrap()
            .contains("column list that leaves out [\"n\"]"));
        // A generated column is not among `columns` (never published by
        // default): its absence from the list is not a narrowing.
        assert_eq!(narrowing(&whole, &cols[..2]), None);
    }

    #[test]
    fn server_checks_name_the_fix() {
        assert!(check_server(170_000, "17.0", "logical").is_ok());
        assert!(check_server(180_002, "18.2", "logical").is_ok());
        let e = check_server(160_004, "16.4", "logical").unwrap_err();
        assert_eq!(e.code(), "version");
        assert!(e.to_string().contains("17"));
        let e = check_server(170_000, "17.0", "replica").unwrap_err();
        assert_eq!(e.code(), "wal_level");
        assert!(e.to_string().contains("wal_level = logical"));
    }
}
