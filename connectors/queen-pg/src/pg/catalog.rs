//! Catalog reads shared by the source and the sink. OWNER: agent C.
//!
//! Every query names `pg_catalog` explicitly: the connector logs in as a
//! role whose `search_path` it does not control, and a table called
//! `pg_class` in a schema ahead of `pg_catalog` must not answer for the real
//! one. Names are compared EXACTLY (they are quoted identifiers when the
//! engines use them): `public.Orders` and `public.orders` are two tables.

use crate::error::{Error, Result};
use crate::pg::connect::classify;

/// `schema.name`, parsed and quoted.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableName {
    pub schema: String,
    pub name: String,
}

impl TableName {
    /// `schema.name` or `name` (= `public.name`); identifiers
    /// `^[A-Za-z_][A-Za-z0-9_$]{0,62}$`, case kept (they are quoted when used).
    pub fn parse(s: &str) -> Result<TableName> {
        let refuse = || {
            Error::config(format!(
                "{s:?} is not a table name: expected schema.name or name (= public.name), \
                 each identifier ^[A-Za-z_][A-Za-z0-9_$]{{0,62}}$"
            ))
        };
        let (schema, name) = match s.split_once('.') {
            Some((schema, name)) => (schema, name),
            None => ("public", s),
        };
        if !valid_identifier(schema) || !valid_identifier(name) {
            return Err(refuse());
        }
        Ok(TableName {
            schema: schema.to_string(),
            name: name.to_string(),
        })
    }

    /// `"schema"."name"`.
    pub fn quoted(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }
}

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.schema, self.name)
    }
}

/// `^[A-Za-z_][A-Za-z0-9_$]{0,62}$`: what a table, column or key name of a
/// connector document may be. Narrower than PostgreSQL (which quotes
/// anything) on purpose: these names travel into SQL text, status lines and
/// metric labels, and 63 bytes is PostgreSQL's own identifier limit
/// (NAMEDATALEN - 1), so a longer name could only ever be truncated.
pub fn valid_identifier(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 63
        && (b[0].is_ascii_alphabetic() || b[0] == b'_')
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c == b'$')
}

/// `"ident"` with inner quotes doubled.
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// `'literal'` with inner quotes doubled.
pub fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerInfo {
    pub version_num: i32,
    pub version: String,
    pub wal_level: String,
}

/// `server_version_num`, `server_version`, `wal_level`.
pub async fn server_info(c: &tokio_postgres::Client) -> Result<ServerInfo> {
    let row = c
        .query_one(
            "SELECT pg_catalog.current_setting('server_version_num')::int4, \
                    pg_catalog.current_setting('server_version'), \
                    pg_catalog.current_setting('wal_level')",
            &[],
        )
        .await
        .map_err(|e| classify(&e))?;
    Ok(ServerInfo {
        version_num: row.get(0),
        version: row.get(1),
        wal_level: row.get(2),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub name: String,
    pub attnum: i16,
    pub type_oid: u32,
    /// `format_type(atttypid, atttypmod)`: usable in a cast.
    pub type_name: String,
    pub not_null: bool,
    /// A generated column (`attgenerated <> ''`): never written by the sink.
    pub generated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub oid: u32,
    pub name: TableName,
    /// Live columns in attnum order (dropped ones excluded).
    pub columns: Vec<ColumnInfo>,
    /// Primary key columns in key order (empty when none).
    pub primary_key: Vec<String>,
    /// `relreplident`: `d`, `n`, `f`, `i`.
    pub replica_identity: u8,
    /// The replica identity index's columns when `replica_identity == 'i'`.
    pub identity_index: Vec<String>,
}

impl TableInfo {
    /// The ordered unique key the source partitions and chunks by: the
    /// primary key, else the replica identity index; `None` when neither.
    pub fn key_columns(&self) -> Option<&[String]> {
        if !self.primary_key.is_empty() {
            Some(&self.primary_key)
        } else if !self.identity_index.is_empty() {
            Some(&self.identity_index)
        } else {
            None
        }
    }

    pub fn column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.name == name)
    }
}

/// The table's description, `None` when it does not exist.
///
/// "Table" means an ordinary or a partitioned table (`relkind` `r` / `p`):
/// the only relations a publication can carry, and the only ones the sink's
/// `ON CONFLICT` and `TRUNCATE` work on. A view or a sequence of that name is
/// `None` too.
///
/// Three reads, not one snapshot: a concurrent `ALTER TABLE` between them
/// can produce a mixed answer. The engines read this at start and again when
/// pgoutput (or a failing statement) says the table changed, so a mixed
/// answer lives one reload at most.
pub async fn table_info(c: &tokio_postgres::Client, t: &TableName) -> Result<Option<TableInfo>> {
    let rel = c
        .query_opt(
            "SELECT c.oid, c.relreplident::text \
               FROM pg_catalog.pg_class c \
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
              WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')",
            &[&t.schema, &t.name],
        )
        .await
        .map_err(|e| classify(&e))?;
    let Some(rel) = rel else {
        return Ok(None);
    };
    let oid: u32 = rel.get(0);
    let replident: String = rel.get(1);
    let replica_identity = replident.as_bytes().first().copied().unwrap_or(b'd');

    let columns = c
        .query(
            "SELECT a.attname::text, a.attnum, a.atttypid, \
                    pg_catalog.format_type(a.atttypid, a.atttypmod), \
                    a.attnotnull, a.attgenerated <> '' \
               FROM pg_catalog.pg_attribute a \
              WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped \
              ORDER BY a.attnum",
            &[&oid],
        )
        .await
        .map_err(|e| classify(&e))?
        .iter()
        .map(|r| ColumnInfo {
            name: r.get(0),
            attnum: r.get(1),
            type_oid: r.get(2),
            type_name: r.get(3),
            not_null: r.get(4),
            generated: r.get(5),
        })
        .collect();

    // The key columns of the primary key and of the replica identity index,
    // in key order. `indnkeyatts` cuts the INCLUDE columns off: they ride in
    // the index but are part of neither the key nor the replica identity.
    let keys = c
        .query(
            "SELECT i.indisprimary, i.indisreplident, a.attname::text \
               FROM pg_catalog.pg_index i \
              CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) \
               JOIN pg_catalog.pg_attribute a \
                 ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
              WHERE i.indrelid = $1 AND (i.indisprimary OR i.indisreplident) \
                AND k.ord <= i.indnkeyatts \
              ORDER BY i.indexrelid, k.ord",
            &[&oid],
        )
        .await
        .map_err(|e| classify(&e))?;
    let mut primary_key = Vec::new();
    let mut identity_index = Vec::new();
    for r in &keys {
        let (is_primary, is_identity, name): (bool, bool, String) = (r.get(0), r.get(1), r.get(2));
        if is_primary {
            primary_key.push(name.clone());
        }
        // Read only while the table says USING INDEX (`relreplident` `i`):
        // the one mode in which that index IS the replica identity.
        if is_identity && replica_identity == b'i' {
            identity_index.push(name);
        }
    }
    Ok(Some(TableInfo {
        oid,
        name: t.clone(),
        columns,
        primary_key,
        replica_identity,
        identity_index,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_names_parse_with_public_as_the_default_schema() {
        let t = TableName::parse("orders").unwrap();
        assert_eq!(
            t,
            TableName {
                schema: "public".into(),
                name: "orders".into()
            }
        );
        assert_eq!(t.to_string(), "public.orders");
        assert_eq!(t.quoted(), "\"public\".\"orders\"");
        let t = TableName::parse("Sales.Order$Lines_2").unwrap();
        assert_eq!(t.schema, "Sales");
        assert_eq!(t.name, "Order$Lines_2");
        assert_eq!(t.quoted(), "\"Sales\".\"Order$Lines_2\"");
        let t = TableName::parse("_s._t").unwrap();
        assert_eq!((t.schema.as_str(), t.name.as_str()), ("_s", "_t"));
        let long = "a".repeat(63);
        assert!(TableName::parse(&format!("{long}.{long}")).is_ok());
    }

    #[test]
    fn anything_else_is_refused_as_a_config_error() {
        let long = "a".repeat(64);
        for bad in [
            "",
            ".",
            "a.",
            ".a",
            "a.b.c",
            "1abc",
            "a-b",
            "a b",
            "\"quoted\"",
            "a;drop",
            "sch.1t",
            "$a",
            "é",
            long.as_str(),
            "public.",
        ] {
            let e = TableName::parse(bad).unwrap_err();
            assert_eq!(e.code(), "config", "{bad:?}");
        }
    }

    #[test]
    fn identifiers() {
        assert!(valid_identifier("a"));
        assert!(valid_identifier("_"));
        assert!(valid_identifier("CamelCase_9$"));
        assert!(!valid_identifier(""));
        assert!(!valid_identifier("9a"));
        assert!(!valid_identifier("a.b"));
        assert!(!valid_identifier("a\"b"));
        assert!(!valid_identifier(&"x".repeat(64)));
    }

    #[test]
    fn quoting_doubles_the_quote_character() {
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_literal("it's"), "'it''s'");
    }

    #[test]
    fn key_columns_prefer_the_primary_key() {
        let t = TableInfo {
            oid: 1,
            name: TableName::parse("t").unwrap(),
            columns: vec![],
            primary_key: vec!["id".into()],
            replica_identity: b'i',
            identity_index: vec!["a".into(), "b".into()],
        };
        assert_eq!(t.key_columns(), Some(&["id".to_string()][..]));
        let t = TableInfo {
            primary_key: vec![],
            ..t
        };
        assert_eq!(t.key_columns().unwrap(), ["a", "b"]);
        let t = TableInfo {
            identity_index: vec![],
            ..t
        };
        assert_eq!(t.key_columns(), None);
    }
}
