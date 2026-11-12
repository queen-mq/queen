//! MySQL column values → JSON, one converter for the binlog and the snapshot.
//!
//! A value reaches the source two ways: as a binlog row image (the binary
//! row format, decoded by `mysql_common` against the table map event), and
//! as a snapshot row (a prepared statement, the binary result protocol). Both
//! become a [`Cell`] under the column's [`Kind`], and one renderer turns a
//! cell into JSON text, so an `r` event and a `c` event of the same row are
//! byte-identical in `key` and `after` (tests/mysql_live.rs pins it on a
//! real server, type by type).
//!
//! The kind comes from the table map event on the stream (types, signedness,
//! charsets, ENUM and SET names: `binlog_row_metadata=FULL`) and from
//! `information_schema.COLUMNS` for the snapshot. Strings are compared and
//! rendered from the bytes as stored: the snapshot connection runs with
//! `character_set_results = binary`, so both paths decode the same bytes.
//!
//! Renderings (the Postgres source's where the types meet):
//!
//! | Kind | JSON |
//! | --- | --- |
//! | integers, `YEAR`, `BIT` | number |
//! | `DECIMAL` | number, the exact digits |
//! | `FLOAT`, `DOUBLE` | number, the shortest form that reads back the same |
//! | `DATE` | `"2026-11-12"` |
//! | `DATETIME(n)` | `"2026-11-12T10:00:00.123"` (n fraction digits) |
//! | `TIMESTAMP(n)` | `"2026-11-12T10:00:00.123+00:00"` |
//! | `TIME(n)` | `"-838:59:59.000"` |
//! | `JSON` | the value, compact |
//! | `ENUM`, `SET`, text | string (`SET`: `"a,c"`) |
//! | binary, `BLOB`, geometry | `"\\x0a1b"` (hex, as Postgres bytea) |
//!
//! A zero date (`0000-00-00`, allowed by some sql_modes) keeps its zeros.

use std::fmt::Write as _;

use mysql_common::binlog::value::BinlogValue;
use mysql_common::constants::ColumnType;
use mysql_common::value::Value;

use crate::error::{Error, Result};
use crate::values::append_json_string;

/// How a column's bytes are text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Charset {
    /// utf8mb4, utf8mb3, ascii, and anything else that is valid UTF-8.
    Utf8,
    /// MySQL's latin1, which is Windows-1252.
    Latin1,
}

impl Charset {
    /// From a collation id (table map events and result sets carry
    /// collations, not charsets).
    pub fn from_collation(id: u16) -> Charset {
        match id {
            5 | 8 | 15 | 31 | 47 | 48 | 49 | 94 => Charset::Latin1,
            _ => Charset::Utf8,
        }
    }

    /// From `information_schema.COLUMNS.CHARACTER_SET_NAME`.
    pub fn from_name(name: &str) -> Charset {
        match name {
            "latin1" => Charset::Latin1,
            _ => Charset::Utf8,
        }
    }
}

/// What a column is, as far as rendering goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Int,
    Float,
    Double,
    Decimal,
    Bit,
    Year,
    Date,
    /// Fraction digits.
    DateTime(u8),
    Timestamp(u8),
    Time(u8),
    Json,
    Enum,
    Set,
    Text(Charset),
    /// `CHAR(n)`: trailing spaces are padding (a SELECT strips them).
    Char(Charset),
    Bin,
    /// `BINARY(n)`: zero-padded to n bytes as stored (the binlog leaves
    /// the trailing zeros out, a SELECT returns them).
    BinFixed(u16),
}

impl Kind {
    /// From `information_schema.COLUMNS` (`DATA_TYPE`, `COLUMN_TYPE`,
    /// `CHARACTER_SET_NAME`, `DATETIME_PRECISION`).
    pub fn from_info(
        data_type: &str,
        column_type: &str,
        charset: Option<&str>,
        fsp: Option<u8>,
    ) -> Result<Kind> {
        let cs = || Charset::from_name(charset.unwrap_or("utf8mb4"));
        let fsp = fsp.unwrap_or(0).min(6);
        Ok(match data_type {
            "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => Kind::Int,
            "decimal" | "numeric" => Kind::Decimal,
            "float" => Kind::Float,
            "double" | "real" => Kind::Double,
            "bit" => Kind::Bit,
            "year" => Kind::Year,
            "date" => Kind::Date,
            "datetime" => Kind::DateTime(fsp),
            "timestamp" => Kind::Timestamp(fsp),
            "time" => Kind::Time(fsp),
            "json" => Kind::Json,
            "enum" => Kind::Enum,
            "set" => Kind::Set,
            "char" if charset.is_some() => Kind::Char(cs()),
            "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" if charset.is_some() => {
                Kind::Text(cs())
            }
            "binary" => Kind::BinFixed(type_len(column_type).unwrap_or(1)),
            "char" | "varchar" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob"
            | "tinytext" | "text" | "mediumtext" | "longtext" | "geometry" | "point"
            | "linestring" | "polygon" | "multipoint" | "multilinestring" | "multipolygon"
            | "geometrycollection" | "geomcollection" | "vector" => Kind::Bin,
            other => {
                return Err(Error::fatal(
                    "column_type",
                    format!("the column type {other:?} is not one the MySQL source knows"),
                ))
            }
        })
    }

    /// From a table map event column: its (real) type, its metadata bytes
    /// and its collation (character columns).
    pub fn from_binlog(t: ColumnType, meta: &[u8], collation: u16) -> Result<Kind> {
        use ColumnType::*;
        let fsp = meta.first().copied().unwrap_or(0).min(6);
        let text = |c: u16| {
            if c == 63 {
                Kind::Bin
            } else {
                Kind::Text(Charset::from_collation(c))
            }
        };
        Ok(match t {
            MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG
            | MYSQL_TYPE_LONGLONG => Kind::Int,
            MYSQL_TYPE_NEWDECIMAL | MYSQL_TYPE_DECIMAL => Kind::Decimal,
            MYSQL_TYPE_FLOAT => Kind::Float,
            MYSQL_TYPE_DOUBLE => Kind::Double,
            MYSQL_TYPE_BIT => Kind::Bit,
            MYSQL_TYPE_YEAR => Kind::Year,
            MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => Kind::Date,
            MYSQL_TYPE_DATETIME => Kind::DateTime(0),
            MYSQL_TYPE_DATETIME2 => Kind::DateTime(fsp),
            MYSQL_TYPE_TIMESTAMP => Kind::Timestamp(0),
            MYSQL_TYPE_TIMESTAMP2 => Kind::Timestamp(fsp),
            MYSQL_TYPE_TIME => Kind::Time(0),
            MYSQL_TYPE_TIME2 => Kind::Time(fsp),
            MYSQL_TYPE_JSON => Kind::Json,
            MYSQL_TYPE_ENUM => Kind::Enum,
            MYSQL_TYPE_SET => Kind::Set,
            // CHAR and BINARY: the table map logs both as STRING.
            MYSQL_TYPE_STRING => match text(collation) {
                Kind::Text(c) => Kind::Char(c),
                _ => Kind::BinFixed(string_len(meta)),
            },
            MYSQL_TYPE_VARCHAR
            | MYSQL_TYPE_VAR_STRING
            | MYSQL_TYPE_BLOB
            | MYSQL_TYPE_TINY_BLOB
            | MYSQL_TYPE_MEDIUM_BLOB
            | MYSQL_TYPE_LONG_BLOB => text(collation),
            MYSQL_TYPE_GEOMETRY | MYSQL_TYPE_VECTOR => Kind::Bin,
            other => {
                return Err(Error::fatal(
                    "column_type",
                    format!("the binlog column type {other:?} is not one the MySQL source knows"),
                ))
            }
        })
    }

    /// A key column of this kind can bound a snapshot chunk: its `<` agrees
    /// with its `ORDER BY` (an ENUM orders by its index but compares a
    /// string by text; JSON and geometry do not order).
    pub fn keyable(self) -> bool {
        !matches!(self, Kind::Enum | Kind::Set | Kind::Json)
    }
}

/// One value, decoded and normalized.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Null,
    Int(i64),
    UInt(u64),
    F32(f32),
    F64(f64),
    /// DECIMAL: the digits as MySQL prints them.
    Num(String),
    Year(u16),
    /// Every temporal kind, broken down; a TIMESTAMP is UTC. `hours` holds
    /// a TIME's whole hours (up to 838).
    Temporal {
        neg: bool,
        y: u16,
        mo: u8,
        d: u8,
        hours: u32,
        mi: u8,
        s: u8,
        us: u32,
    },
    /// Compact JSON text.
    Json(String),
    Str(String),
    Bin(Vec<u8>),
}

fn bad(kind: Kind, what: &str) -> Error {
    Error::io(format!("a {kind:?} column held {what}"))
}

impl Cell {
    /// A binlog row value. `names` is the ENUM or SET column's value list
    /// (from the table map, in definition order).
    pub fn from_binlog(kind: Kind, v: &BinlogValue<'_>, names: &[String]) -> Result<Cell> {
        let v = match v {
            BinlogValue::Value(v) => v,
            BinlogValue::Jsonb(j) => {
                if kind != Kind::Json {
                    return Err(bad(kind, "a JSON value"));
                }
                let dom = j
                    .clone()
                    .parse()
                    .map_err(|e| Error::io(format!("a binlog JSON value: {e}")))?;
                let value: serde_json::Value = dom.into();
                return Ok(Cell::Json(value.to_string()));
            }
            BinlogValue::JsonDiff(_) => {
                return Err(Error::fatal(
                    "binlog_row_value_options",
                    "the binlog carries a partial JSON update: set binlog_row_value_options \
                     to '' (the source needs whole rows)",
                ))
            }
        };
        if matches!(v, Value::NULL) {
            return Ok(Cell::Null);
        }
        match kind {
            Kind::Enum => {
                let Value::Int(i) = v else {
                    return Err(bad(kind, "a non-index ENUM value"));
                };
                // 0 is the empty string MySQL stores for an invalid value.
                Ok(Cell::Str(
                    match usize::try_from(*i).ok().filter(|&i| i > 0) {
                        Some(i) => names.get(i - 1).cloned().unwrap_or_default(),
                        None => String::new(),
                    },
                ))
            }
            Kind::Set => {
                let Value::Bytes(b) = v else {
                    return Err(bad(kind, "a non-bitmap SET value"));
                };
                let mut bits: u64 = 0;
                for (i, byte) in b.iter().take(8).enumerate() {
                    bits |= u64::from(*byte) << (8 * i);
                }
                let mut out = String::new();
                for (i, n) in names.iter().enumerate().take(64) {
                    if bits & (1 << i) != 0 {
                        if !out.is_empty() {
                            out.push(',');
                        }
                        out.push_str(n);
                    }
                }
                Ok(Cell::Str(out))
            }
            Kind::Timestamp(_) => match v {
                // TIMESTAMP2: "seconds" or "seconds.micros" since the epoch.
                Value::Bytes(b) => {
                    let s = std::str::from_utf8(b).map_err(|_| bad(kind, "non-text"))?;
                    let (sec, us) = match s.split_once('.') {
                        Some((a, b)) => (a, b),
                        None => (s, "0"),
                    };
                    let sec: i64 = sec.parse().map_err(|_| bad(kind, s))?;
                    let us: u32 = us.parse().map_err(|_| bad(kind, s))?;
                    Ok(Cell::from_unix(sec, us))
                }
                Value::Int(sec) => Ok(Cell::from_unix(*sec, 0)),
                _ => Cell::from_value(kind, v.clone()),
            },
            Kind::Year => match v {
                // mysql_common prints 1900 + the stored byte; 0 is 0000.
                Value::Bytes(b) => {
                    let y: u16 = std::str::from_utf8(b)
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| bad(kind, "a non-number YEAR"))?;
                    Ok(Cell::Year(if y == 1900 { 0 } else { y }))
                }
                _ => Cell::from_value(kind, v.clone()),
            },
            _ => Cell::from_value(kind, v.clone()),
        }
    }

    /// A TIMESTAMP's seconds since the epoch, UTC; 0 is the zero value.
    fn from_unix(sec: i64, us: u32) -> Cell {
        if sec == 0 && us == 0 {
            return Cell::Temporal {
                neg: false,
                y: 0,
                mo: 0,
                d: 0,
                hours: 0,
                mi: 0,
                s: 0,
                us: 0,
            };
        }
        let days = sec.div_euclid(86_400);
        let sod = sec.rem_euclid(86_400);
        let (y, mo, d) = civil_from_days(days);
        Cell::Temporal {
            neg: false,
            y: y.clamp(0, 9999) as u16,
            mo: mo as u8,
            d: d as u8,
            hours: (sod / 3600) as u32,
            mi: (sod % 3600 / 60) as u8,
            s: (sod % 60) as u8,
            us,
        }
    }

    /// A result-set value (or a binlog value of the same shape).
    pub fn from_value(kind: Kind, v: Value) -> Result<Cell> {
        Ok(match (kind, v) {
            (_, Value::NULL) => Cell::Null,
            (Kind::Int, Value::Int(i)) => Cell::Int(i),
            (Kind::Int, Value::UInt(u)) => Cell::UInt(u),
            (Kind::Year, Value::Int(i)) => Cell::Year(i.clamp(0, 9999) as u16),
            (Kind::Year, Value::UInt(u)) => Cell::Year(u.min(9999) as u16),
            (Kind::Float, Value::Float(f)) => Cell::F32(f),
            (Kind::Float, Value::Double(f)) => Cell::F32(f as f32),
            (Kind::Double, Value::Double(f)) => Cell::F64(f),
            (Kind::Double, Value::Float(f)) => Cell::F64(f64::from(f)),
            (Kind::Decimal, Value::Bytes(b)) => {
                Cell::Num(String::from_utf8(b).map_err(|_| bad(kind, "non-text"))?)
            }
            (Kind::Bit, Value::Bytes(b)) => {
                let mut n: u64 = 0;
                for byte in b.iter().rev().take(8).rev() {
                    n = (n << 8) | u64::from(*byte);
                }
                Cell::UInt(n)
            }
            (Kind::Bit, Value::Int(i)) => Cell::UInt(i as u64),
            (Kind::Bit, Value::UInt(u)) => Cell::UInt(u),
            (
                Kind::Date | Kind::DateTime(_) | Kind::Timestamp(_),
                Value::Date(y, mo, d, h, mi, s, us),
            ) => Cell::Temporal {
                neg: false,
                y,
                mo,
                d,
                hours: u32::from(h),
                mi,
                s,
                us,
            },
            (Kind::Time(_), Value::Time(neg, days, h, mi, s, us)) => Cell::Temporal {
                neg,
                y: 0,
                mo: 0,
                d: 0,
                hours: days * 24 + u32::from(h),
                mi,
                s,
                us,
            },
            (Kind::Json, Value::Bytes(b)) => {
                let v: serde_json::Value = serde_json::from_slice(&b)
                    .map_err(|e| Error::io(format!("a JSON column held non-JSON ({e})")))?;
                Cell::Json(v.to_string())
            }
            (Kind::Enum | Kind::Set | Kind::Text(Charset::Utf8), Value::Bytes(b)) => {
                Cell::Str(utf8(b))
            }
            (Kind::Text(Charset::Latin1), Value::Bytes(b)) => Cell::Str(latin1(&b)),
            (Kind::Char(cs), Value::Bytes(mut b)) => {
                while b.last() == Some(&b' ') {
                    b.pop();
                }
                Cell::Str(match cs {
                    Charset::Utf8 => utf8(b),
                    Charset::Latin1 => latin1(&b),
                })
            }
            (Kind::Bin, Value::Bytes(b)) => Cell::Bin(b),
            (Kind::BinFixed(n), Value::Bytes(mut b)) => {
                if b.len() < usize::from(n) {
                    b.resize(usize::from(n), 0);
                }
                Cell::Bin(b)
            }
            (k, other) => return Err(bad(k, &format!("{other:?}"))),
        })
    }

    /// Append the JSON value.
    pub fn append_json(&self, kind: Kind, out: &mut String) {
        match self {
            Cell::Null => out.push_str("null"),
            Cell::Int(i) => {
                let _ = write!(out, "{i}");
            }
            Cell::UInt(u) => {
                let _ = write!(out, "{u}");
            }
            Cell::Year(y) => {
                let _ = write!(out, "{y}");
            }
            Cell::F32(f) => {
                let _ = write!(out, "{f}");
            }
            Cell::F64(f) => {
                let _ = write!(out, "{f}");
            }
            Cell::Num(n) => {
                if is_json_number(n) {
                    out.push_str(n);
                } else {
                    append_json_string(n, out);
                }
            }
            Cell::Json(j) => out.push_str(j),
            Cell::Str(s) => append_json_string(s, out),
            Cell::Temporal { .. } | Cell::Bin(_) => {
                let t = self.text(kind).unwrap_or_default();
                append_json_string(&t, out);
            }
        }
    }

    /// The value as text: what a partition name and the snapshot's key match
    /// are made of. `None` for NULL.
    pub fn text(&self, kind: Kind) -> Option<String> {
        Some(match self {
            Cell::Null => return None,
            Cell::Int(i) => i.to_string(),
            Cell::UInt(u) => u.to_string(),
            Cell::Year(y) => y.to_string(),
            Cell::F32(f) => f.to_string(),
            Cell::F64(f) => f.to_string(),
            Cell::Num(n) => n.clone(),
            Cell::Json(j) => j.clone(),
            Cell::Str(s) => s.clone(),
            Cell::Bin(b) => format!("\\x{}", hex::encode(b)),
            Cell::Temporal {
                neg,
                y,
                mo,
                d,
                hours,
                mi,
                s,
                us,
            } => {
                let mut t = String::with_capacity(32);
                let frac = |fsp: u8, t: &mut String| {
                    if fsp > 0 {
                        let digits = format!("{us:06}");
                        t.push('.');
                        t.push_str(&digits[..usize::from(fsp)]);
                    }
                };
                match kind {
                    Kind::Date => {
                        let _ = write!(t, "{y:04}-{mo:02}-{d:02}");
                    }
                    Kind::Time(fsp) => {
                        let _ = write!(
                            t,
                            "{}{hours:02}:{mi:02}:{s:02}",
                            if *neg { "-" } else { "" }
                        );
                        frac(fsp, &mut t);
                    }
                    Kind::DateTime(fsp) | Kind::Timestamp(fsp) => {
                        let _ = write!(t, "{y:04}-{mo:02}-{d:02}T{hours:02}:{mi:02}:{s:02}");
                        frac(fsp, &mut t);
                        if matches!(kind, Kind::Timestamp(_)) {
                            t.push_str("+00:00");
                        }
                    }
                    _ => {
                        let _ =
                            write!(t, "{y:04}-{mo:02}-{d:02}T{hours:02}:{mi:02}:{s:02}.{us:06}");
                    }
                }
                t
            }
        })
    }

    /// The value as a statement parameter that compares like the column
    /// (the snapshot's chunk bound), as text.
    pub fn param_text(&self, kind: Kind) -> Option<String> {
        match (self, kind) {
            (Cell::Bin(b), _) => Some(hex::encode(b)),
            (
                Cell::Temporal {
                    y,
                    mo,
                    d,
                    hours,
                    mi,
                    s,
                    us,
                    ..
                },
                Kind::DateTime(_) | Kind::Timestamp(_),
            ) => Some(format!(
                "{y:04}-{mo:02}-{d:02} {hours:02}:{mi:02}:{s:02}.{us:06}"
            )),
            (
                Cell::Temporal {
                    neg,
                    hours,
                    mi,
                    s,
                    us,
                    ..
                },
                Kind::Time(_),
            ) => Some(format!(
                "{}{hours}:{mi:02}:{s:02}.{us:06}",
                if *neg { "-" } else { "" }
            )),
            _ => self.text(kind),
        }
    }
}

/// A parameter rebuilt from [`Cell::param_text`].
pub fn param_value(kind: Kind, text: &str) -> Result<Value> {
    let bad = || {
        Error::fatal(
            "pointer_corrupt",
            format!("snapshot key {text:?} ({kind:?})"),
        )
    };
    Ok(match kind {
        Kind::Int => match text.parse::<i64>() {
            Ok(i) => Value::Int(i),
            Err(_) => Value::UInt(text.parse::<u64>().map_err(|_| bad())?),
        },
        Kind::Bit => Value::UInt(text.parse::<u64>().map_err(|_| bad())?),
        Kind::Year => Value::Int(text.parse::<i64>().map_err(|_| bad())?),
        Kind::Float | Kind::Double => Value::Double(text.parse::<f64>().map_err(|_| bad())?),
        Kind::Bin | Kind::BinFixed(_) => Value::Bytes(hex::decode(text).map_err(|_| bad())?),
        Kind::Text(Charset::Latin1) | Kind::Char(Charset::Latin1) => Value::Bytes(
            text.chars()
                .map(latin1_byte)
                .collect::<Option<Vec<u8>>>()
                .ok_or_else(bad)?,
        ),
        _ => Value::Bytes(text.as_bytes().to_vec()),
    })
}

/// The placeholder a key column's bound takes, cast so the comparison is the
/// column's own (a DECIMAL against a string would compare as a double).
pub fn placeholder(kind: Kind, column_type: &str) -> String {
    match kind {
        Kind::Decimal => format!("CAST(? AS {})", decimal_cast(column_type)),
        Kind::Date => "CAST(? AS DATE)".into(),
        Kind::DateTime(_) | Kind::Timestamp(_) => "CAST(? AS DATETIME(6))".into(),
        Kind::Time(_) => "CAST(? AS TIME(6))".into(),
        Kind::Bin | Kind::BinFixed(_) => "CAST(? AS BINARY)".into(),
        _ => "?".into(),
    }
}

/// `binary(16)` → 16.
fn type_len(column_type: &str) -> Option<u16> {
    column_type
        .split_once('(')
        .and_then(|(_, r)| r.split_once(')'))
        .and_then(|(n, _)| n.trim().parse().ok())
}

/// A STRING column's byte length from its table map metadata (two bytes:
/// the real type with the length's high bits, the length's low byte), as
/// MySQL's `Field_string::do_save_field_metadata` writes it.
fn string_len(meta: &[u8]) -> u16 {
    match meta {
        [b0, b1, ..] if (b0 & 0x30) != 0x30 => {
            u16::from(*b1) | ((u16::from(b0 & 0x30) ^ 0x30) << 4)
        }
        [_, b1, ..] => u16::from(*b1),
        _ => 1,
    }
}

/// `decimal(12,2) unsigned` → `DECIMAL(12,2)`.
fn decimal_cast(column_type: &str) -> String {
    let inner = column_type
        .split_once('(')
        .and_then(|(_, r)| r.split_once(')'))
        .map(|(a, _)| a)
        .filter(|a| a.chars().all(|c| c.is_ascii_digit() || c == ','))
        .unwrap_or("65,30");
    format!("DECIMAL({inner})")
}

fn utf8(b: Vec<u8>) -> String {
    match String::from_utf8(b) {
        Ok(s) => s,
        Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
    }
}

/// Windows-1252 (MySQL's latin1): 0x80..0x9F are the punctuation block, the
/// five holes map to the C1 controls as MySQL does.
const CP1252_HIGH: [char; 32] = [
    '\u{20AC}', '\u{0081}', '\u{201A}', '\u{0192}', '\u{201E}', '\u{2026}', '\u{2020}', '\u{2021}',
    '\u{02C6}', '\u{2030}', '\u{0160}', '\u{2039}', '\u{0152}', '\u{008D}', '\u{017D}', '\u{008F}',
    '\u{0090}', '\u{2018}', '\u{2019}', '\u{201C}', '\u{201D}', '\u{2022}', '\u{2013}', '\u{2014}',
    '\u{02DC}', '\u{2122}', '\u{0161}', '\u{203A}', '\u{0153}', '\u{009D}', '\u{017E}', '\u{0178}',
];

fn latin1(b: &[u8]) -> String {
    b.iter()
        .map(|&c| match c {
            0x80..=0x9F => CP1252_HIGH[usize::from(c - 0x80)],
            _ => char::from(c),
        })
        .collect()
}

fn latin1_byte(c: char) -> Option<u8> {
    if let Some(i) = CP1252_HIGH.iter().position(|&h| h == c) {
        return Some(0x80 + i as u8);
    }
    u8::try_from(u32::from(c))
        .ok()
        .filter(|b| !(0x80..=0x9F).contains(b))
}

/// `-?(0|[1-9][0-9]*)(\.[0-9]+)?`: what a DECIMAL prints, and valid JSON.
fn is_json_number(s: &str) -> bool {
    let b = s.strip_prefix('-').unwrap_or(s).as_bytes();
    let (int, frac) = match b.iter().position(|&c| c == b'.') {
        Some(i) => (&b[..i], Some(&b[i + 1..])),
        None => (b, None),
    };
    !int.is_empty()
        && int.iter().all(u8::is_ascii_digit)
        && (int.len() == 1 || int[0] != b'0')
        && frac.is_none_or(|f| !f.is_empty() && f.iter().all(u8::is_ascii_digit))
}

/// Days since 1970-01-01 → (year, month, day), proleptic Gregorian.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(kind: Kind, c: &Cell) -> String {
        let mut s = String::new();
        c.append_json(kind, &mut s);
        s
    }

    #[test]
    fn numbers_render_exactly() {
        assert_eq!(
            json(Kind::Int, &Cell::Int(-9_223_372_036_854_775_808)),
            "-9223372036854775808"
        );
        assert_eq!(
            json(Kind::Int, &Cell::UInt(u64::MAX)),
            "18446744073709551615"
        );
        assert_eq!(json(Kind::Float, &Cell::F32(0.1)), "0.1");
        assert_eq!(json(Kind::Double, &Cell::F64(1.0)), "1");
        assert_eq!(json(Kind::Decimal, &Cell::Num("-0.50".into())), "-0.50");
        assert_eq!(
            json(Kind::Decimal, &Cell::Num("12345678901234567890.123".into())),
            "12345678901234567890.123"
        );
        // Not a JSON number: a string, never a broken payload.
        assert_eq!(json(Kind::Decimal, &Cell::Num(".5".into())), "\".5\"");
        assert_eq!(json(Kind::Year, &Cell::Year(0)), "0");
    }

    #[test]
    fn temporal_kinds_keep_their_fraction_digits() {
        let v = Value::Date(2026, 11, 12, 10, 0, 1, 123_000);
        let dt = Cell::from_value(Kind::DateTime(3), v.clone()).unwrap();
        assert_eq!(json(Kind::DateTime(3), &dt), "\"2026-11-12T10:00:01.123\"");
        assert_eq!(json(Kind::DateTime(0), &dt), "\"2026-11-12T10:00:01\"");
        let ts = Cell::from_value(Kind::Timestamp(6), v).unwrap();
        assert_eq!(
            json(Kind::Timestamp(6), &ts),
            "\"2026-11-12T10:00:01.123000+00:00\""
        );
        let d = Cell::from_value(Kind::Date, Value::Date(2026, 1, 2, 0, 0, 0, 0)).unwrap();
        assert_eq!(json(Kind::Date, &d), "\"2026-01-02\"");
        let t =
            Cell::from_value(Kind::Time(1), Value::Time(true, 34, 22, 59, 59, 500_000)).unwrap();
        assert_eq!(json(Kind::Time(1), &t), "\"-838:59:59.5\"");
        let zero = Cell::from_value(Kind::DateTime(0), Value::Date(0, 0, 0, 0, 0, 0, 0)).unwrap();
        assert_eq!(json(Kind::DateTime(0), &zero), "\"0000-00-00T00:00:00\"");
    }

    #[test]
    fn a_binlog_timestamp_is_seconds_since_the_epoch() {
        let ts = Cell::from_binlog(
            Kind::Timestamp(3),
            &BinlogValue::Value(Value::Bytes(b"1762941601.123000".to_vec())),
            &[],
        )
        .unwrap();
        // 2025-11-12T10:00:01Z
        assert_eq!(
            json(Kind::Timestamp(3), &ts),
            "\"2025-11-12T10:00:01.123+00:00\""
        );
        let same = Cell::from_value(
            Kind::Timestamp(3),
            Value::Date(2025, 11, 12, 10, 0, 1, 123_000),
        )
        .unwrap();
        assert_eq!(ts, same);
        let zero = Cell::from_binlog(
            Kind::Timestamp(0),
            &BinlogValue::Value(Value::Bytes(b"0".to_vec())),
            &[],
        )
        .unwrap();
        assert_eq!(
            json(Kind::Timestamp(0), &zero),
            "\"0000-00-00T00:00:00+00:00\""
        );
    }

    #[test]
    fn enum_and_set_names_come_from_the_table_map() {
        let names: Vec<String> = ["new", "paid", "gone"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let e = Cell::from_binlog(Kind::Enum, &BinlogValue::Value(Value::Int(2)), &names).unwrap();
        assert_eq!(e, Cell::Str("paid".into()));
        let e0 = Cell::from_binlog(Kind::Enum, &BinlogValue::Value(Value::Int(0)), &names).unwrap();
        assert_eq!(e0, Cell::Str(String::new()));
        let s = Cell::from_binlog(
            Kind::Set,
            &BinlogValue::Value(Value::Bytes(vec![0b101])),
            &names,
        )
        .unwrap();
        assert_eq!(s, Cell::Str("new,gone".into()));
        let row = Cell::from_value(Kind::Set, Value::Bytes(b"new,gone".to_vec())).unwrap();
        assert_eq!(s, row);
    }

    #[test]
    fn strings_bytes_and_padding() {
        let c =
            Cell::from_value(Kind::Char(Charset::Utf8), Value::Bytes(b"ab  ".to_vec())).unwrap();
        assert_eq!(c, Cell::Str("ab".into()));
        let l = Cell::from_value(
            Kind::Text(Charset::Latin1),
            Value::Bytes(vec![0x80, b'a', 0xE9]),
        )
        .unwrap();
        assert_eq!(l, Cell::Str("€aé".into()));
        let b = Cell::from_value(Kind::Bin, Value::Bytes(vec![0, 0xAB])).unwrap();
        assert_eq!(json(Kind::Bin, &b), "\"\\\\x00ab\"");
        let bits = Cell::from_value(Kind::Bit, Value::Bytes(vec![1, 2])).unwrap();
        assert_eq!(json(Kind::Bit, &bits), "258");
        let j = Cell::from_value(
            Kind::Json,
            Value::Bytes(br#"{"b": [1, 2.5], "a": null}"#.to_vec()),
        )
        .unwrap();
        assert_eq!(json(Kind::Json, &j), r#"{"a":null,"b":[1,2.5]}"#);
    }

    #[test]
    fn params_round_trip_through_their_text() {
        let cases = [
            (Kind::Int, Cell::Int(-5)),
            (Kind::Int, Cell::UInt(u64::MAX)),
            (Kind::Bin, Cell::Bin(vec![0, 1, 255])),
            (Kind::Text(Charset::Latin1), Cell::Str("€é".into())),
        ];
        for (k, c) in cases {
            let t = c.param_text(k).unwrap();
            let v = param_value(k, &t).unwrap();
            match (&c, v) {
                (Cell::Int(i), Value::Int(j)) => assert_eq!(*i, j),
                (Cell::UInt(i), Value::UInt(j)) => assert_eq!(*i, j),
                (Cell::Bin(a), Value::Bytes(b)) => assert_eq!(a, &b),
                (Cell::Str(_), Value::Bytes(b)) => assert_eq!(b, vec![0x80, 0xE9]),
                (c, v) => panic!("{c:?} {v:?}"),
            }
        }
        assert_eq!(
            placeholder(Kind::Decimal, "decimal(12,2) unsigned"),
            "CAST(? AS DECIMAL(12,2))"
        );
        assert_eq!(placeholder(Kind::Int, "bigint"), "?");
    }

    #[test]
    fn kinds_agree_between_the_catalog_and_the_binlog() {
        use ColumnType::*;
        let pairs = [
            (
                Kind::from_info("int", "int", None, None),
                Kind::from_binlog(MYSQL_TYPE_LONG, &[], 0),
            ),
            (
                Kind::from_info("decimal", "decimal(12,2)", None, None),
                Kind::from_binlog(MYSQL_TYPE_NEWDECIMAL, &[12, 2], 0),
            ),
            (
                Kind::from_info("datetime", "datetime(3)", None, Some(3)),
                Kind::from_binlog(MYSQL_TYPE_DATETIME2, &[3], 0),
            ),
            (
                Kind::from_info("timestamp", "timestamp(6)", None, Some(6)),
                Kind::from_binlog(MYSQL_TYPE_TIMESTAMP2, &[6], 0),
            ),
            (
                Kind::from_info("varchar", "varchar(20)", Some("utf8mb4"), None),
                Kind::from_binlog(MYSQL_TYPE_VARCHAR, &[], 255),
            ),
            (
                Kind::from_info("char", "char(5)", Some("latin1"), None),
                Kind::from_binlog(MYSQL_TYPE_STRING, &[], 8),
            ),
            (
                Kind::from_info("varbinary", "varbinary(8)", None, None),
                Kind::from_binlog(MYSQL_TYPE_VARCHAR, &[], 63),
            ),
            (
                Kind::from_info("longblob", "longblob", None, None),
                Kind::from_binlog(MYSQL_TYPE_BLOB, &[4], 63),
            ),
            (
                Kind::from_info("text", "text", Some("utf8mb3"), None),
                Kind::from_binlog(MYSQL_TYPE_BLOB, &[2], 33),
            ),
            (
                Kind::from_info("json", "json", None, None),
                Kind::from_binlog(MYSQL_TYPE_JSON, &[4], 0),
            ),
            (
                Kind::from_info("binary", "binary(4)", None, None),
                Kind::from_binlog(MYSQL_TYPE_STRING, &[0xFE, 4], 63),
            ),
        ];
        for (a, b) in pairs {
            assert_eq!(a.unwrap(), b.unwrap());
        }
        assert!(Kind::from_info("frobnitz", "frobnitz", None, None).is_err());
    }
}
