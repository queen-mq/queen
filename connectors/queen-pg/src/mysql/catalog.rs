//! What the MySQL source reads from the server before it streams: the
//! server's settings (the checks that name a fix), the binlog position, and
//! each configured table's columns and primary key.

use std::fmt;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row, Value};

use crate::error::{Error, Result};

use super::conn::classify;
use super::gtid::GtidSet;
use super::values::Kind;

/// MySQL identifiers: 1 to 64 characters, no control characters, no
/// trailing space (MySQL refuses one).
pub fn valid_identifier(s: &str) -> bool {
    let n = s.chars().count();
    (1..=64).contains(&n) && !s.chars().any(char::is_control) && !s.ends_with(' ')
}

/// `` `name` `` with backticks doubled.
pub fn quote(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

/// A table: `database.table`, or `table` in the connection's database.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MyTableName {
    pub db: String,
    pub name: String,
}

impl MyTableName {
    pub fn parse(s: &str, default_db: &str) -> Result<MyTableName> {
        let (db, name) = match s.split_once('.') {
            Some((d, n)) => (d, n),
            None => (default_db, s),
        };
        for (what, v) in [("database", db), ("table", name)] {
            if !valid_identifier(v) || v.contains('.') {
                return Err(Error::config(format!(
                    "{s:?}: the {what} name {v:?} is not a MySQL name (1 to 64 characters, \
                     no '.', no control characters)"
                )));
            }
        }
        Ok(MyTableName {
            db: db.to_string(),
            name: name.to_string(),
        })
    }

    /// `` `db`.`table` ``.
    pub fn quoted(&self) -> String {
        format!("{}.{}", quote(&self.db), quote(&self.name))
    }
}

impl fmt::Display for MyTableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.db, self.name)
    }
}

fn text(v: &Value) -> Option<String> {
    match v {
        Value::NULL => None,
        Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        Value::Int(i) => Some(i.to_string()),
        Value::UInt(u) => Some(u.to_string()),
        other => Some(format!("{other:?}")),
    }
}

fn col(row: &Row, i: usize) -> Option<String> {
    row.as_ref(i).and_then(text)
}

/// The server, as the checks need it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerInfo {
    pub version: String,
    pub major: u32,
    pub minor: u32,
    pub mariadb: bool,
    pub server_uuid: String,
    pub log_bin: bool,
    pub binlog_format: String,
    pub binlog_row_image: String,
    pub binlog_row_metadata: String,
    pub binlog_row_value_options: String,
    pub gtid_mode: String,
    pub expire_seconds: Option<u64>,
}

impl ServerInfo {
    /// `SHOW BINARY LOG STATUS` from 8.2, `SHOW MASTER STATUS` before.
    pub fn status_sql(&self) -> &'static str {
        if (self.major, self.minor) >= (8, 2) {
            "SHOW BINARY LOG STATUS"
        } else {
            "SHOW MASTER STATUS"
        }
    }
}

pub async fn server_info(c: &mut Conn) -> Result<ServerInfo> {
    let row: Option<Row> = c
        .query_first("SELECT VERSION(), @@version_comment")
        .await
        .map_err(|e| classify(&e))?;
    let row = row.ok_or_else(|| Error::io("SELECT VERSION() answered no row"))?;
    let version = col(&row, 0).unwrap_or_default();
    let comment = col(&row, 1).unwrap_or_default();
    let mariadb = version.to_ascii_lowercase().contains("mariadb")
        || comment.to_ascii_lowercase().contains("mariadb");
    let mut nums = version
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<u32>().unwrap_or(0));
    let (major, minor) = (nums.next().unwrap_or(0), nums.next().unwrap_or(0));
    let mut info = ServerInfo {
        version,
        major,
        minor,
        mariadb,
        server_uuid: String::new(),
        log_bin: false,
        binlog_format: String::new(),
        binlog_row_image: String::new(),
        binlog_row_metadata: String::new(),
        binlog_row_value_options: String::new(),
        gtid_mode: String::new(),
        expire_seconds: None,
    };
    // A MariaDB or a 5.7 lacks some of the variables below: the version
    // check names the real problem first.
    if mariadb || major < 8 {
        return Ok(info);
    }
    let row: Option<Row> = c
        .query_first(
            "SELECT @@server_uuid, @@log_bin, @@binlog_format, @@binlog_row_image, \
             @@binlog_row_metadata, @@binlog_row_value_options, @@gtid_mode, \
             @@binlog_expire_logs_seconds",
        )
        .await
        .map_err(|e| classify(&e))?;
    let row = row.ok_or_else(|| Error::io("the server variables answered no row"))?;
    info.server_uuid = col(&row, 0).unwrap_or_default().to_ascii_lowercase();
    info.log_bin = matches!(col(&row, 1).as_deref(), Some("1") | Some("ON"));
    info.binlog_format = col(&row, 2).unwrap_or_default().to_ascii_uppercase();
    info.binlog_row_image = col(&row, 3).unwrap_or_default().to_ascii_uppercase();
    info.binlog_row_metadata = col(&row, 4).unwrap_or_default().to_ascii_uppercase();
    info.binlog_row_value_options = col(&row, 5).unwrap_or_default().to_ascii_uppercase();
    info.gtid_mode = col(&row, 6).unwrap_or_default().to_ascii_uppercase();
    info.expire_seconds = col(&row, 7).and_then(|s| s.parse().ok());
    Ok(info)
}

/// Every setting the source needs, each refusal naming its fix.
pub fn check_server(i: &ServerInfo) -> Result<()> {
    if i.mariadb {
        return Err(Error::fatal(
            "version",
            format!(
                "the server is MariaDB ({}): the MySQL source reads MySQL 8.0 or later \
                 (MariaDB's GTIDs and binlog differ)",
                i.version
            ),
        ));
    }
    if i.major < 8 {
        return Err(Error::fatal(
            "version",
            format!(
                "MySQL {} is too old: the source needs 8.0 or later",
                i.version
            ),
        ));
    }
    let need = |code: &'static str, ok: bool, msg: String| {
        if ok {
            Ok(())
        } else {
            Err(Error::fatal(code, msg))
        }
    };
    need(
        "log_bin",
        i.log_bin,
        "binary logging is off: start MySQL with log_bin (the default since 8.0; on a \
         managed MySQL, turn on point-in-time recovery or binary logs)"
            .into(),
    )?;
    need(
        "binlog_format",
        i.binlog_format == "ROW",
        format!(
            "binlog_format is {}: the source reads row events, set it to ROW",
            i.binlog_format
        ),
    )?;
    need(
        "binlog_row_image",
        i.binlog_row_image == "FULL",
        format!(
            "binlog_row_image is {}: the source needs whole rows, set it to FULL",
            i.binlog_row_image
        ),
    )?;
    need(
        "binlog_row_metadata",
        i.binlog_row_metadata == "FULL",
        format!(
            "binlog_row_metadata is {}: the source reads column names and keys from the \
             binlog, `SET PERSIST binlog_row_metadata = FULL`",
            i.binlog_row_metadata
        ),
    )?;
    need(
        "binlog_row_value_options",
        !i.binlog_row_value_options.contains("PARTIAL_JSON"),
        "binlog_row_value_options is PARTIAL_JSON: the source needs whole JSON values, set \
         it to ''"
            .into(),
    )?;
    need(
        "gtid_mode",
        i.gtid_mode == "ON",
        format!(
            "gtid_mode is {}: the source keeps its place by GTID, set gtid_mode = ON and \
             enforce_gtid_consistency = ON",
            i.gtid_mode
        ),
    )?;
    Ok(())
}

/// Where the binlog ends now, and what the server has executed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinlogStatus {
    pub pos: BinlogPos,
    pub executed: GtidSet,
}

/// A place in the binlog: the file and the offset in it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BinlogPos {
    pub file: String,
    pub pos: u64,
}

impl BinlogPos {
    /// `binlog.000012` → 12, so files compare by number whatever the name
    /// (and by name as a last resort).
    fn seq(&self) -> (u64, &str) {
        let n = self
            .file
            .rsplit_once('.')
            .and_then(|(_, n)| n.parse::<u64>().ok())
            .unwrap_or(0);
        (n, self.file.as_str())
    }

    /// At or past `other`.
    pub fn reached(&self, other: &BinlogPos) -> bool {
        if self.file.is_empty() {
            return false;
        }
        let (a, b) = (self.seq(), other.seq());
        a.0 > b.0 || (a.0 == b.0 && self.pos >= other.pos)
    }
}

impl fmt::Display for BinlogPos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.file, self.pos)
    }
}

pub async fn binlog_status(c: &mut Conn, info: &ServerInfo) -> Result<BinlogStatus> {
    let row: Option<Row> = c
        .query_first(info.status_sql())
        .await
        .map_err(|e| match &e {
            mysql_async::Error::Server(s) if s.code == 1227 => Error::fatal(
                "privileges",
                format!(
                    "{}: the source's user needs REPLICATION CLIENT (and REPLICATION SLAVE, \
                 SELECT): `GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO …`",
                    s.message
                ),
            ),
            _ => classify(&e),
        })?;
    let row = row.ok_or_else(|| {
        Error::fatal(
            "log_bin",
            "the server reports no binary log (binary logging is off)",
        )
    })?;
    let file = col(&row, 0).unwrap_or_default();
    let pos = col(&row, 1).and_then(|p| p.parse().ok()).unwrap_or(0);
    let executed = GtidSet::parse(&col(&row, 4).unwrap_or_default())?;
    Ok(BinlogStatus {
        pos: BinlogPos { file, pos },
        executed,
    })
}

/// `@@GLOBAL.gtid_purged`: what the binlog no longer holds.
pub async fn gtid_purged(c: &mut Conn) -> Result<GtidSet> {
    let v: Option<Option<String>> = c
        .query_first("SELECT @@GLOBAL.gtid_purged")
        .await
        .map_err(|e| classify(&e))?;
    GtidSet::parse(&v.flatten().unwrap_or_default())
}

/// One column of a configured table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub name: String,
    pub kind: Kind,
    /// `COLUMN_TYPE` (`decimal(12,2) unsigned`, `enum('a','b')`).
    pub column_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub name: MyTableName,
    pub columns: Vec<ColumnInfo>,
    /// The primary key's columns, in key order.
    pub primary_key: Vec<String>,
}

/// The table, `None` when there is no such base table.
pub async fn table_info(c: &mut Conn, t: &MyTableName) -> Result<Option<TableInfo>> {
    let kind: Option<Option<String>> = c
        .exec_first(
            "SELECT TABLE_TYPE FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (t.db.as_str(), t.name.as_str()),
        )
        .await
        .map_err(|e| classify(&e))?;
    match kind.flatten().as_deref() {
        Some("BASE TABLE") => {}
        _ => return Ok(None),
    }
    let rows: Vec<Row> = c
        .exec(
            "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, CHARACTER_SET_NAME, DATETIME_PRECISION \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (t.db.as_str(), t.name.as_str()),
        )
        .await
        .map_err(|e| classify(&e))?;
    let mut columns = Vec::with_capacity(rows.len());
    for r in &rows {
        let name = col(r, 0).unwrap_or_default();
        let data_type = col(r, 1).unwrap_or_default().to_ascii_lowercase();
        let column_type = col(r, 2).unwrap_or_default().to_ascii_lowercase();
        let charset = col(r, 3);
        let fsp = col(r, 4).and_then(|p| p.parse::<u8>().ok());
        let kind = Kind::from_info(&data_type, &column_type, charset.as_deref(), fsp).map_err(
            |e| match e {
                Error::Fatal { code, message } => {
                    Error::fatal(code, format!("{t}.{name}: {message}"))
                }
                other => other,
            },
        )?;
        columns.push(ColumnInfo {
            name,
            kind,
            column_type,
        });
    }
    let pk: Vec<Row> = c
        .exec(
            "SELECT COLUMN_NAME FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' \
             ORDER BY SEQ_IN_INDEX",
            (t.db.as_str(), t.name.as_str()),
        )
        .await
        .map_err(|e| classify(&e))?;
    Ok(Some(TableInfo {
        name: t.clone(),
        columns,
        primary_key: pk.iter().filter_map(|r| col(r, 0)).collect(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_names_and_quoting() {
        let t = MyTableName::parse("orders", "shop").unwrap();
        assert_eq!(t.to_string(), "shop.orders");
        let t = MyTableName::parse("inv.`x", "shop").unwrap();
        assert_eq!(t.quoted(), "`inv`.```x`");
        assert!(MyTableName::parse("a.b.c", "shop").is_err());
        assert!(MyTableName::parse("", "shop").is_err());
        assert!(MyTableName::parse("t ", "shop").is_err());
        assert!(MyTableName::parse(&"x".repeat(65), "shop").is_err());
    }

    #[test]
    fn binlog_positions_compare_by_file_number() {
        let p = |f: &str, n: u64| BinlogPos {
            file: f.into(),
            pos: n,
        };
        assert!(p("binlog.000010", 4).reached(&p("binlog.000009", 900)));
        assert!(p("binlog.000009", 900).reached(&p("binlog.000009", 900)));
        assert!(!p("binlog.000009", 899).reached(&p("binlog.000009", 900)));
        assert!(!BinlogPos::default().reached(&p("binlog.000001", 4)));
    }

    #[test]
    fn every_setting_is_checked_with_its_fix() {
        let ok = ServerInfo {
            version: "8.4.3".into(),
            major: 8,
            minor: 4,
            mariadb: false,
            server_uuid: "x".into(),
            log_bin: true,
            binlog_format: "ROW".into(),
            binlog_row_image: "FULL".into(),
            binlog_row_metadata: "FULL".into(),
            binlog_row_value_options: String::new(),
            gtid_mode: "ON".into(),
            expire_seconds: Some(2_592_000),
        };
        check_server(&ok).unwrap();
        assert_eq!(ok.status_sql(), "SHOW BINARY LOG STATUS");
        let cases: Vec<(&str, ServerInfo)> = vec![
            (
                "version",
                ServerInfo {
                    mariadb: true,
                    ..ok.clone()
                },
            ),
            (
                "version",
                ServerInfo {
                    major: 5,
                    ..ok.clone()
                },
            ),
            (
                "log_bin",
                ServerInfo {
                    log_bin: false,
                    ..ok.clone()
                },
            ),
            (
                "binlog_format",
                ServerInfo {
                    binlog_format: "MIXED".into(),
                    ..ok.clone()
                },
            ),
            (
                "binlog_row_image",
                ServerInfo {
                    binlog_row_image: "MINIMAL".into(),
                    ..ok.clone()
                },
            ),
            (
                "binlog_row_metadata",
                ServerInfo {
                    binlog_row_metadata: "MINIMAL".into(),
                    ..ok.clone()
                },
            ),
            (
                "binlog_row_value_options",
                ServerInfo {
                    binlog_row_value_options: "PARTIAL_JSON".into(),
                    ..ok.clone()
                },
            ),
            (
                "gtid_mode",
                ServerInfo {
                    gtid_mode: "OFF_PERMISSIVE".into(),
                    ..ok.clone()
                },
            ),
        ];
        for (code, info) in cases {
            assert_eq!(check_server(&info).unwrap_err().code(), code);
        }
        assert_eq!(
            ServerInfo { minor: 0, ..ok }.status_sql(),
            "SHOW MASTER STATUS"
        );
    }
}
