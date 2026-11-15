//! The MySQL source's pointer: the set of transactions Queen has.
//!
//! Same rules as the Postgres pointer (`crate::source::pointer`): one KV
//! document, `src:<name>:pointer`, moved only inside the `/transaction` that
//! pushes what it covers, fenced by the version this owner holds. What it
//! says: every transaction in `gtids` is in Queen, plus the first
//! `inTxn.done` row changes of the one split transaction in progress, plus
//! the snapshot rows up to `snapshot.after`.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::source::pointer::{Doc, SnapshotProgress};

use super::gtid::GtidSet;

/// The document's format version (`"v"`).
pub const FORMAT: u32 = 1;

/// A transaction pushed in parts: its GTID and how many of its row changes
/// (in binlog order, mapped or not) are already in Queen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InTxn {
    pub gtid: String,
    pub done: u64,
}

/// Where the data in Queen ends (what a bundle's cover is).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MyPosition {
    pub gtids: GtidSet,
    pub in_txn: Option<InTxn>,
    pub snapshot: Option<SnapshotProgress>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Wire {
    v: u32,
    engine: String,
    epoch: String,
    server_uuid: String,
    gtids: String,
    #[serde(default)]
    in_txn: Option<InTxn>,
    #[serde(default)]
    snapshot: Option<SnapshotProgress>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resynced_at: Option<String>,
    #[serde(default)]
    updated_at: String,
}

/// The document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MyPointer {
    /// 8 hex characters, new on the first start and on every resync: message
    /// ids never meet the ids of an earlier life.
    pub epoch: String,
    /// The server the source started on (`@@server_uuid`): for reading only,
    /// a promoted replica has another and carries on.
    pub server_uuid: String,
    pub pos: MyPosition,
    pub resynced_at: Option<String>,
    pub updated_at: String,
}

impl MyPointer {
    /// This pointer moved to `pos`, stamped now.
    pub fn at(&self, pos: &MyPosition) -> MyPointer {
        MyPointer {
            pos: pos.clone(),
            updated_at: crate::values::iso_utc_micros(crate::status::now_us()),
            ..self.clone()
        }
    }
}

impl Doc for MyPointer {
    fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(Wire {
            v: FORMAT,
            engine: "mysql".into(),
            epoch: self.epoch.clone(),
            server_uuid: self.server_uuid.clone(),
            gtids: self.pos.gtids.to_string(),
            in_txn: self.pos.in_txn.clone(),
            snapshot: self.pos.snapshot.clone(),
            resynced_at: self.resynced_at.clone(),
            updated_at: self.updated_at.clone(),
        })
        .unwrap_or(serde_json::Value::Null)
    }

    fn from_value(v: &serde_json::Value) -> Result<MyPointer> {
        let corrupt = |why: String| {
            Error::fatal(
                "pointer_corrupt",
                format!(
                    "the source pointer cannot be read ({why}); POST \
                     /api/v1/connectors/<name>/resync to start over"
                ),
            )
        };
        if v.get("engine").and_then(serde_json::Value::as_str) != Some("mysql") {
            return Err(corrupt(
                "it is a Postgres source's pointer: the document changed engine".into(),
            ));
        }
        let w: Wire = serde_json::from_value(v.clone()).map_err(|e| corrupt(e.to_string()))?;
        if w.v != FORMAT {
            return Err(corrupt(format!(
                "format {} (this broker reads {FORMAT}); upgrade the broker or resync",
                w.v
            )));
        }
        let gtids = GtidSet::parse(&w.gtids).map_err(|e| corrupt(e.to_string()))?;
        Ok(MyPointer {
            epoch: w.epoch,
            server_uuid: w.server_uuid,
            pos: MyPosition {
                gtids,
                in_txn: w.in_txn,
                snapshot: w.snapshot,
            },
            resynced_at: w.resynced_at,
            updated_at: w.updated_at,
        })
    }

    fn same_place(&self, other: &MyPointer) -> bool {
        self.epoch == other.epoch && self.pos == other.pos
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ptr() -> MyPointer {
        MyPointer {
            epoch: "0a1b2c3d".into(),
            server_uuid: "3e11fa47-71ca-11e1-9e33-c80aa9429562".into(),
            pos: MyPosition {
                gtids: GtidSet::parse("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-42").unwrap(),
                in_txn: Some(InTxn {
                    gtid: "3e11fa47-71ca-11e1-9e33-c80aa9429562:43".into(),
                    done: 1000,
                }),
                snapshot: SnapshotProgress::start(vec!["shop.orders".into()]),
            },
            resynced_at: None,
            updated_at: "2026-11-12T10:00:00.000000Z".into(),
        }
    }

    #[test]
    fn round_trips_and_compares_places() {
        let p = ptr();
        let v = p.to_value();
        assert_eq!(v["engine"], "mysql");
        assert_eq!(v["gtids"], "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-42");
        let back = MyPointer::from_value(&v).unwrap();
        assert_eq!(back, p);
        let later = p.at(&MyPosition {
            in_txn: None,
            ..p.pos.clone()
        });
        assert!(!p.same_place(&later));
        assert!(p.same_place(&MyPointer {
            updated_at: "x".into(),
            ..p.clone()
        }));
    }

    #[test]
    fn a_postgres_pointer_is_refused_with_the_fix() {
        let e = MyPointer::from_value(&serde_json::json!({"v": 1, "lsn": "0/1"})).unwrap_err();
        assert_eq!(e.code(), "pointer_corrupt");
        assert!(e.to_string().contains("resync"), "{e}");
    }
}
