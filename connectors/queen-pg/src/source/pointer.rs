//! The pointer: the exactly-once marker of a source (PLAN §4.2).
//!
//! One KV document per source, `src:<name>:pointer` in the connector's own
//! tenant, namespace `queen-pg`, kept forever. It says how far into the WAL
//! Queen already has every change: every source transaction whose END LSN is
//! `<= lsn` is in Queen, plus the first `inTxn.done` events of the one split
//! transaction in progress, plus the snapshot rows up to `snapshot.after`.
//!
//! It moves ONLY inside the same `/transaction` as the pushes it covers (or
//! alone, for idle advancement), as a put with `expect: <the version this
//! owner holds>` and `required: true` (I1). A bundle that loses that
//! precondition rolls back WHOLE, so a pointer can never get ahead of the data
//! or behind it, and two owners can never both move it from the same version.
//!
//! Versions are opaque and never re-issued: they are compared for equality
//! only. A version this owner did not write means another owner moved the
//! pointer (or an earlier attempt of ours committed while its answer was
//! lost) — [`super::bundle`] reads the document and decides which (I5).

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::queen::{KvAnswer, KvOp, KvResult, QueenApi};
use crate::repl::Lsn;

/// The document's format version (`"v"`).
pub const POINTER_FORMAT: u32 = 1;

/// `src:<name>:pointer`.
pub fn pointer_key(name: &str) -> String {
    format!("src:{name}:pointer")
}

/// `src:<name>:lease`.
pub fn lease_key(name: &str) -> String {
    format!("src:{name}:lease")
}

/// A source transaction pushed in chunks (PLAN §4.4 "Split transactions"):
/// its commit LSN and how many of its RAW changes (pgoutput Insert, Update,
/// Delete and Truncate messages, in decode order, mapped or not) are already
/// in Queen. Counted in changes, not events, so the number means the same
/// after a restart under another configuration (an update is one or two
/// events depending on `partitionBy`; a truncate zero or more depending on
/// `onTruncate`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InTxn {
    pub commit_lsn: Lsn,
    pub done: u64,
}

/// How far the snapshot got (PLAN §4.5). `tables` is the whole run, fixed when
/// it starts; `table` the one being read; `after` the last key of `table`
/// whose rows are in Queen (`None`: none yet); `rows` the rows pushed so far
/// in the run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotProgress {
    pub tables: Vec<String>,
    pub table: String,
    #[serde(default)]
    pub after: Option<Vec<String>>,
    #[serde(default)]
    pub rows: u64,
}

impl SnapshotProgress {
    /// A run over `tables` from the first one, `None` when there is nothing to
    /// read.
    pub fn start(tables: Vec<String>) -> Option<SnapshotProgress> {
        let first = tables.first()?.clone();
        Some(SnapshotProgress {
            tables,
            table: first,
            after: None,
            rows: 0,
        })
    }

    /// How many tables of the run are finished.
    pub fn tables_done(&self) -> usize {
        self.tables
            .iter()
            .position(|t| *t == self.table)
            .unwrap_or(self.tables.len())
    }

    /// The progress once `table` is finished: the next table from its start,
    /// or `None` when it was the last one.
    pub fn next_table(&self, rows: u64) -> Option<SnapshotProgress> {
        let i = self.tables.iter().position(|t| *t == self.table)?;
        let next = self.tables.get(i + 1)?.clone();
        Some(SnapshotProgress {
            tables: self.tables.clone(),
            table: next,
            after: None,
            rows,
        })
    }
}

/// Where the data in Queen ends: the part of the pointer an owner compares to
/// decide whether a bundle committed. Everything else in the document
/// (`slot`, `systemId`, `updatedAt`) is identity or bookkeeping.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Position {
    pub lsn: Lsn,
    pub in_txn: Option<InTxn>,
    pub snapshot: Option<SnapshotProgress>,
}

/// The document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pointer {
    pub v: u32,
    /// 8 lowercase hex characters; new on first start and on every resync, so
    /// message ids never collide across a restored database whose LSNs repeat.
    pub epoch: String,
    pub system_id: String,
    pub slot: String,
    pub lsn: Lsn,
    #[serde(default)]
    pub in_txn: Option<InTxn>,
    #[serde(default)]
    pub snapshot: Option<SnapshotProgress>,
    /// The `resyncRequestedAt` of the document this pointer's epoch answered,
    /// so a resync request is carried out once, not on every start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resynced_at: Option<String>,
    #[serde(default)]
    pub updated_at: String,
}

impl Pointer {
    pub fn position(&self) -> Position {
        Position {
            lsn: self.lsn,
            in_txn: self.in_txn.clone(),
            snapshot: self.snapshot.clone(),
        }
    }

    /// This pointer moved to `pos`, stamped now.
    pub fn at(&self, pos: &Position) -> Pointer {
        Pointer {
            lsn: pos.lsn,
            in_txn: pos.in_txn.clone(),
            snapshot: pos.snapshot.clone(),
            updated_at: crate::values::iso_utc_micros(crate::status::now_us()),
            ..self.clone()
        }
    }

    /// Whether `other` is exactly this pointer's position in the same epoch:
    /// the test of "my bundle committed" after a lost answer (I5). Equality,
    /// not "at least": a pointer beyond ours was moved by someone else, and the
    /// safe answer to that is to start again from what it says.
    pub fn same_place(&self, other: &Pointer) -> bool {
        self.epoch == other.epoch && self.position() == other.position()
    }

    pub fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }

    /// Parse a stored document. One this crate cannot read is not guessed at:
    /// the operator decides (resync).
    pub fn from_value(v: &serde_json::Value) -> Result<Pointer> {
        let p: Pointer = serde_json::from_value(v.clone()).map_err(|e| {
            Error::fatal(
                "pointer_corrupt",
                format!(
                    "the source pointer cannot be read ({e}); POST /api/v1/connectors/<name>/resync \
                     to start over"
                ),
            )
        })?;
        if p.v != POINTER_FORMAT {
            return Err(Error::fatal(
                "pointer_corrupt",
                format!(
                    "the source pointer has format {} (this broker reads {POINTER_FORMAT}); \
                     upgrade the broker or resync",
                    p.v
                ),
            ));
        }
        Ok(p)
    }
}

/// The pointer and the version this owner holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Held {
    pub doc: Pointer,
    pub version: i64,
}

/// The result of operation `index`, by the index each answer carries.
pub fn result_at(results: &[KvResult], index: usize) -> Option<&KvResult> {
    results
        .iter()
        .find(|r| r.index == index)
        .or_else(|| results.get(index))
}

/// One linearizable read of the pointer: `None` when absent.
pub async fn read(api: &dyn QueenApi, key: &str) -> Result<Option<Held>> {
    let a = api.kv(vec![KvOp::get(key)]).await?;
    let Some(r) = result_at(&a.results, 0) else {
        return Err(Error::io(format!("the KV read of {key} answered nothing")));
    };
    match r.value_if_found() {
        Some(v) => Ok(Some(Held {
            doc: Pointer::from_value(v)?,
            version: r.version,
        })),
        None => Ok(None),
    }
}

/// The first write: the key must not exist (`expect: 0`, required). Losing it
/// means another owner created the pointer in between: [`Error::Fenced`].
pub async fn create(api: &dyn QueenApi, key: &str, doc: &Pointer) -> Result<i64> {
    let op = KvOp::Put {
        key: key.to_string(),
        value: doc.to_value(),
        ttl_seconds: None,
        expect: Some(0),
        required: true,
    };
    let a = api.kv(vec![op]).await?;
    written(&a, key, "created")
}

/// A fenced replacement outside a bundle (resync): `expect` the version held.
pub async fn replace(api: &dyn QueenApi, key: &str, doc: &Pointer, expect: i64) -> Result<i64> {
    let a = api
        .kv(vec![KvOp::fence(key, doc.to_value(), expect)])
        .await?;
    written(&a, key, "replaced")
}

/// A fenced delete (teardown).
pub async fn delete(api: &dyn QueenApi, key: &str, expect: i64) -> Result<()> {
    let a = api
        .kv(vec![KvOp::Delete {
            key: key.to_string(),
            expect: Some(expect),
            required: true,
        }])
        .await?;
    if !a.ok {
        return Err(Error::Fenced(format!(
            "the pointer {key} moved before it could be deleted"
        )));
    }
    Ok(())
}

fn written(a: &KvAnswer, key: &str, what: &str) -> Result<i64> {
    if !a.ok {
        return Err(Error::Fenced(format!(
            "the pointer {key} was written by another owner before it could be {what}"
        )));
    }
    match result_at(&a.results, 0) {
        Some(r) if r.did_apply() && r.version != 0 => Ok(r.version),
        _ => Err(Error::Fenced(format!(
            "the pointer {key} could not be {what} (the write did not apply)"
        ))),
    }
}

/// 8 lowercase hex characters.
pub fn new_epoch() -> String {
    format!("{:08x}", rand::random::<u32>())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Pointer {
        Pointer {
            v: 1,
            epoch: "4f1c2a9b".into(),
            system_id: "7431125789012345678".into(),
            slot: "queen_orders_src".into(),
            lsn: "0/16B3748".parse().unwrap(),
            in_txn: Some(InTxn {
                commit_lsn: "0/1700000".parse().unwrap(),
                done: 5000,
            }),
            snapshot: Some(SnapshotProgress {
                tables: vec!["public.a".into(), "public.b".into()],
                table: "public.a".into(),
                after: Some(vec!["42".into()]),
                rows: 12000,
            }),
            resynced_at: None,
            updated_at: "2026-10-02T10:00:00.000000Z".into(),
        }
    }

    /// The stored shape is PLAN §4.2's, field for field.
    #[test]
    fn the_document_has_the_plan_shape() {
        let v = sample().to_value();
        assert_eq!(v["v"], 1);
        assert_eq!(v["epoch"], "4f1c2a9b");
        assert_eq!(v["systemId"], "7431125789012345678");
        assert_eq!(v["slot"], "queen_orders_src");
        assert_eq!(v["lsn"], "0/16B3748");
        assert_eq!(v["inTxn"]["commitLsn"], "0/1700000");
        assert_eq!(v["inTxn"]["done"], 5000);
        assert_eq!(v["snapshot"]["tables"][1], "public.b");
        assert_eq!(v["snapshot"]["table"], "public.a");
        assert_eq!(v["snapshot"]["after"][0], "42");
        assert_eq!(v["snapshot"]["rows"], 12000);
        assert!(v.get("resyncedAt").is_none(), "absent until a resync");
        assert_eq!(Pointer::from_value(&v).unwrap(), sample());

        let mut idle = sample();
        idle.in_txn = None;
        idle.snapshot = None;
        idle.resynced_at = Some("2026-10-02T11:00:00Z".into());
        let v = idle.to_value();
        assert!(v["inTxn"].is_null() && v["snapshot"].is_null());
        assert_eq!(v["resyncedAt"], "2026-10-02T11:00:00Z");
        assert_eq!(Pointer::from_value(&v).unwrap(), idle);
    }

    #[test]
    fn an_unreadable_or_future_pointer_is_an_operator_decision() {
        let e = Pointer::from_value(&serde_json::json!({"v": 1})).unwrap_err();
        assert_eq!(e.code(), "pointer_corrupt");
        let mut v = sample().to_value();
        v["v"] = 2.into();
        assert_eq!(
            Pointer::from_value(&v).unwrap_err().code(),
            "pointer_corrupt"
        );
    }

    #[test]
    fn same_place_compares_the_position_and_the_epoch_only() {
        let a = sample();
        let mut b = sample();
        b.updated_at = "later".into();
        assert!(a.same_place(&b), "updatedAt is bookkeeping");
        b.epoch = "00000000".into();
        assert!(!a.same_place(&b), "another epoch is another stream");
        let mut c = sample();
        c.in_txn.as_mut().unwrap().done = 5001;
        assert!(!a.same_place(&c));
        let mut d = sample();
        d.snapshot.as_mut().unwrap().rows += 1;
        assert!(!a.same_place(&d));
    }

    #[test]
    fn snapshot_progress_walks_the_tables_in_order() {
        let p = SnapshotProgress::start(vec!["public.a".into(), "public.b".into()]).unwrap();
        assert_eq!(p.table, "public.a");
        assert_eq!(p.tables_done(), 0);
        let q = p.next_table(10).unwrap();
        assert_eq!(
            (q.table.as_str(), q.after.clone(), q.rows),
            ("public.b", None, 10)
        );
        assert_eq!(q.tables_done(), 1);
        assert!(
            q.next_table(20).is_none(),
            "the last table ends the snapshot"
        );
        assert!(SnapshotProgress::start(Vec::new()).is_none());
    }

    #[test]
    fn epochs_are_eight_lowercase_hex_characters() {
        for _ in 0..100 {
            let e = new_epoch();
            assert_eq!(e.len(), 8);
            assert!(e
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
        }
        assert_eq!(pointer_key("orders-src"), "src:orders-src:pointer");
        assert_eq!(lease_key("orders-src"), "src:orders-src:lease");
    }
}
