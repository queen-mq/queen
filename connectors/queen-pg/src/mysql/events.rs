//! The MySQL source's messages: payloads, partitions and ids.
//!
//! The payload is the Postgres source's (`op`, `table`, `key`, `after`,
//! `before`, `ts`, `seq`) with `gtid` in place of `lsn` and `xid`:
//!
//! ```json
//! {"op":"u","table":"shop.orders","key":{"id":1},
//!  "after":{"id":1,"status":"paid"},"before":{"id":1,"status":"new"},
//!  "gtid":"3e11fa47-71ca-11e1-9e33-c80aa9429562:23","ts":"2026-11-12T10:00:00.123456Z","seq":0}
//! ```
//!
//! `before` is the whole old row (`binlog_row_image=FULL`); an `r` (a row
//! copied by the snapshot) has no `gtid`. Ids are built from the binlog
//! alone: `my:<epoch>:<uuid>:<gno>:<seq>` (`…:<seq>.<k>` for the second
//! event of one change; `my:<epoch>:<uuid>:<tag>:<gno>:<seq>` for a tagged
//! GTID), `my:<epoch>:r:<n>` for the n-th snapshot row of the epoch.

use std::fmt::Write as _;

use crate::source::events::render_partition;
use crate::values::append_json_string;

use super::gtid::Gtid;
use super::values::{Cell, Kind};

/// A row's columns as a renderer sees them.
pub struct Cols<'a> {
    pub names: &'a [&'a str],
    pub kinds: &'a [Kind],
}

impl Cols<'_> {
    /// `{"a":…,"b":…}` of the columns at `pos`, in that order.
    pub fn object_at(&self, cells: &[Cell], pos: &[usize]) -> String {
        let mut out = String::with_capacity(16 * pos.len() + 2);
        out.push('{');
        for (n, &i) in pos.iter().enumerate() {
            let (Some(name), Some(kind)) = (self.names.get(i), self.kinds.get(i)) else {
                continue;
            };
            if n > 0 {
                out.push(',');
            }
            append_json_string(name, &mut out);
            out.push(':');
            cells
                .get(i)
                .unwrap_or(&Cell::Null)
                .append_json(*kind, &mut out);
        }
        out.push('}');
        out
    }

    pub fn object_all(&self, cells: &[Cell]) -> String {
        let all: Vec<usize> = (0..self.names.len()).collect();
        self.object_at(cells, &all)
    }

    fn texts(&self, cells: &[Cell], pos: &[usize]) -> Vec<Option<String>> {
        pos.iter()
            .map(|&i| {
                cells
                    .get(i)
                    .and_then(|c| c.text(self.kinds.get(i).copied().unwrap_or(Kind::Bin)))
            })
            .collect()
    }

    /// The partition (`None` = `single`: `all`).
    pub fn partition(&self, cells: &[Cell], part: Option<&[usize]>) -> String {
        match part {
            None => "all".to_string(),
            Some(pos) => {
                let t = self.texts(cells, pos);
                let v: Vec<Option<&str>> = t.iter().map(|o| o.as_deref()).collect();
                render_partition(&v)
            }
        }
    }

    /// The key's texts joined by NUL: what the snapshot matches rows by.
    pub fn key_code(&self, cells: &[Cell], key: &[usize]) -> String {
        let mut s = String::new();
        for (n, t) in self.texts(cells, key).into_iter().enumerate() {
            if n > 0 {
                s.push('\0');
            }
            if let Some(t) = t {
                s.push_str(&t);
            }
        }
        s
    }
}

/// An event before its place (gtid, seq) is known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Body {
    pub op: &'static str,
    pub key: String,
    pub after: Option<String>,
    pub before: Option<String>,
}

impl Body {
    pub fn render(&self, table: &str, gtid: Option<&Gtid>, ts_us: i64, seq: u64) -> String {
        let mut out = String::with_capacity(
            128 + self.key.len()
                + self.after.as_ref().map_or(4, String::len)
                + self.before.as_ref().map_or(4, String::len),
        );
        out.push_str("{\"op\":\"");
        out.push_str(self.op);
        out.push_str("\",\"table\":");
        append_json_string(table, &mut out);
        out.push_str(",\"key\":");
        out.push_str(&self.key);
        out.push_str(",\"after\":");
        out.push_str(self.after.as_deref().unwrap_or("null"));
        out.push_str(",\"before\":");
        out.push_str(self.before.as_deref().unwrap_or("null"));
        if let Some(g) = gtid {
            out.push_str(",\"gtid\":");
            append_json_string(&g.to_string(), &mut out);
        }
        out.push_str(",\"ts\":");
        append_json_string(&crate::values::iso_utc_micros(ts_us), &mut out);
        let _ = write!(out, ",\"seq\":{seq}}}");
        out
    }
}

/// What one row change becomes: one event, or two for a partition move (a
/// `d` to the old partition, a `c` to the new one), and the keys it touched
/// (for the snapshot's chunk filter).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub events: Vec<(String, Body)>,
    pub touched: Vec<String>,
}

pub fn insert(c: &Cols<'_>, key: &[usize], part: Option<&[usize]>, new: &[Cell]) -> Change {
    Change {
        events: vec![(
            c.partition(new, part),
            Body {
                op: "c",
                key: c.object_at(new, key),
                after: Some(c.object_all(new)),
                before: None,
            },
        )],
        touched: vec![c.key_code(new, key)],
    }
}

pub fn update(
    c: &Cols<'_>,
    key: &[usize],
    part: Option<&[usize]>,
    old: &[Cell],
    new: &[Cell],
) -> Change {
    let (old_code, new_code) = (c.key_code(old, key), c.key_code(new, key));
    let touched = if old_code == new_code {
        vec![new_code]
    } else {
        vec![new_code, old_code]
    };
    let (old_part, new_part) = (c.partition(old, part), c.partition(new, part));
    let before = c.object_all(old);
    if old_part != new_part {
        return Change {
            events: vec![
                (
                    old_part,
                    Body {
                        op: "d",
                        key: c.object_at(old, key),
                        after: None,
                        before: Some(before),
                    },
                ),
                (
                    new_part,
                    Body {
                        op: "c",
                        key: c.object_at(new, key),
                        after: Some(c.object_all(new)),
                        before: None,
                    },
                ),
            ],
            touched,
        };
    }
    Change {
        events: vec![(
            new_part,
            Body {
                op: "u",
                key: c.object_at(new, key),
                after: Some(c.object_all(new)),
                before: Some(before),
            },
        )],
        touched,
    }
}

pub fn delete(c: &Cols<'_>, key: &[usize], part: Option<&[usize]>, old: &[Cell]) -> Change {
    Change {
        events: vec![(
            c.partition(old, part),
            Body {
                op: "d",
                key: c.object_at(old, key),
                after: None,
                before: Some(c.object_all(old)),
            },
        )],
        touched: vec![c.key_code(old, key)],
    }
}

/// `t`: a `TRUNCATE` of `tables` (emitted with `onTruncate: emit`).
pub fn truncate_event(tables: &[&str], gtid: &Gtid, ts_us: i64, seq: u64) -> String {
    let mut out = String::from("{\"op\":\"t\",\"tables\":[");
    for (i, t) in tables.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        append_json_string(t, &mut out);
    }
    out.push_str("],\"gtid\":");
    append_json_string(&gtid.to_string(), &mut out);
    out.push_str(",\"ts\":");
    append_json_string(&crate::values::iso_utc_micros(ts_us), &mut out);
    let _ = write!(out, ",\"seq\":{seq}}}");
    out
}

pub fn stream_id(epoch: &str, g: &Gtid, seq: u64, k: usize) -> String {
    let mut s = format!("my:{epoch}:{}:", g.uuid_compact());
    if !g.tag.is_empty() {
        s.push_str(&g.tag);
        s.push(':');
    }
    let _ = write!(s, "{}:{seq}", g.gno);
    if k > 0 {
        let _ = write!(s, ".{k}");
    }
    s
}

pub fn snapshot_id(epoch: &str, n: u64) -> String {
    format!("my:{epoch}:r:{n}")
}

#[cfg(test)]
mod tests {
    use super::*;

    const U: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

    fn cols() -> (Vec<&'static str>, Vec<Kind>) {
        (
            vec!["id", "region", "v"],
            vec![
                Kind::Int,
                Kind::Text(super::super::values::Charset::Utf8),
                Kind::Int,
            ],
        )
    }

    fn row(id: i64, region: &str, v: i64) -> Vec<Cell> {
        vec![Cell::Int(id), Cell::Str(region.into()), Cell::Int(v)]
    }

    #[test]
    fn a_change_renders_the_postgres_shape_with_a_gtid() {
        let (n, k) = cols();
        let c = Cols {
            names: &n,
            kinds: &k,
        };
        let ch = update(&c, &[0], Some(&[0]), &row(1, "eu", 1), &row(1, "eu", 2));
        assert_eq!(ch.events.len(), 1);
        let g = Gtid {
            uuid: U.into(),
            tag: String::new(),
            gno: 23,
        };
        let (part, body) = &ch.events[0];
        assert_eq!(part, "1");
        assert_eq!(
            body.render("shop.orders", Some(&g), 0, 4),
            format!(
                "{{\"op\":\"u\",\"table\":\"shop.orders\",\"key\":{{\"id\":1}},\
                 \"after\":{{\"id\":1,\"region\":\"eu\",\"v\":2}},\
                 \"before\":{{\"id\":1,\"region\":\"eu\",\"v\":1}},\
                 \"gtid\":\"{U}:23\",\"ts\":\"1970-01-01T00:00:00.000000Z\",\"seq\":4}}"
            )
        );
        assert_eq!(ch.touched, vec!["1".to_string()]);
    }

    #[test]
    fn a_partition_move_is_a_delete_and_a_create() {
        let (n, k) = cols();
        let c = Cols {
            names: &n,
            kinds: &k,
        };
        let ch = update(&c, &[0], Some(&[1]), &row(1, "eu", 1), &row(1, "us", 1));
        let ops: Vec<(&str, &str)> = ch.events.iter().map(|(p, b)| (p.as_str(), b.op)).collect();
        assert_eq!(ops, vec![("eu", "d"), ("us", "c")]);
        let key_move = update(&c, &[0], Some(&[0]), &row(1, "eu", 1), &row(2, "eu", 1));
        assert_eq!(key_move.touched, vec!["2".to_string(), "1".to_string()]);
        let single = insert(&c, &[0, 1], None, &row(3, "a|b", 0));
        assert_eq!(single.events[0].0, "all");
        assert_eq!(single.touched, vec!["3\0a|b".to_string()]);
    }

    #[test]
    fn ids_come_from_the_binlog() {
        let g = Gtid {
            uuid: U.into(),
            tag: String::new(),
            gno: 7,
        };
        assert_eq!(
            stream_id("0a1b2c3d", &g, 2, 0),
            format!("my:0a1b2c3d:{}:7:2", U.replace('-', ""))
        );
        assert_eq!(
            stream_id("0a1b2c3d", &g, 2, 1),
            format!("my:0a1b2c3d:{}:7:2.1", U.replace('-', ""))
        );
        let t = Gtid {
            tag: "batch".into(),
            ..g
        };
        assert!(stream_id("e", &t, 0, 0).ends_with(":batch:7:0"));
        assert_eq!(snapshot_id("e", 41), "my:e:r:41");
    }
}
