//! The progress table (PLAN §5.1) and the batch arithmetic around it.
//!
//! One row per `(sink, partition_id)` holds the highest offset of that
//! partition whose effects are in the target table. It is written in the SAME
//! transaction as those effects, so after any crash the two agree: a message
//! at or below `last_offset` is in the table, one above it is not. That is the
//! whole exactly-once mechanism of the sink — redeliveries (a lost ack, an
//! expired lease, a node that died mid-batch) are recognized here and dropped.
//!
//! The key is the pop's NUMERIC `partitionId`, not the partition's name: the
//! broker gives a partition that died (30 idle days) and was recreated a new
//! id, and its offsets start again. Keyed by name, the new incarnation's first
//! messages would sit below the old `last_offset` and be dropped as already
//! applied.
//!
//! Every transaction takes its rows in ascending `partition_id` order — the
//! insert of missing rows and the `FOR UPDATE` alike — so two workers whose
//! batches share partitions (a lease that expired under a slow batch) queue on
//! the first shared row instead of deadlocking, and the second one reads the
//! first one's progress once it commits (READ COMMITTED re-reads a row it
//! waited for).

use std::collections::{BTreeMap, HashMap};

use crate::error::{Error, Result};
use crate::pg::catalog::{quote_ident, table_info, TableInfo, TableName};
use crate::pg::connect::classify;

/// `CREATE SCHEMA` + `CREATE TABLE`, as an operator runs them by hand when the
/// connector may not (`createProgressTable: false`, or a role without CREATE).
pub fn ddl(p: &TableName) -> [String; 2] {
    [
        format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(&p.schema)),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (\
             sink text NOT NULL, \
             partition_id bigint NOT NULL, \
             partition text NOT NULL, \
             last_offset bigint NOT NULL, \
             updated_at timestamptz NOT NULL DEFAULT now(), \
             PRIMARY KEY (sink, partition_id))",
            p.quoted()
        ),
    ]
}

/// The DDL as one line for an error message.
pub fn ddl_text(p: &TableName) -> String {
    let [a, b] = ddl(p);
    format!("{a}; {b};")
}

/// `$1` sink, `$2` partition ids (ascending), `$3` their names: a row at
/// `-1` for every partition that has none yet.
pub fn ensure_sql(p: &TableName) -> String {
    format!(
        "INSERT INTO {} (sink, partition_id, partition, last_offset) \
         SELECT $1::text, q.id, q.name, -1 FROM unnest($2::bigint[], $3::text[]) AS q(id, name) \
         ORDER BY q.id ON CONFLICT (sink, partition_id) DO NOTHING",
        p.quoted()
    )
}

/// `$1` sink, `$2` partition ids: their progress, locked, in id order.
pub fn lock_sql(p: &TableName) -> String {
    format!(
        "SELECT partition_id, last_offset FROM {} \
         WHERE sink = $1::text AND partition_id = ANY($2::bigint[]) \
         ORDER BY partition_id FOR UPDATE",
        p.quoted()
    )
}

/// `$1` sink, `$2` partition ids, `$3` the highest offset of each in the
/// chunk. Never moves backwards.
pub fn advance_sql(p: &TableName) -> String {
    format!(
        "UPDATE {} AS q_p SET last_offset = q.off, updated_at = now() \
         FROM unnest($2::bigint[], $3::bigint[]) AS q(id, off) \
         WHERE q_p.sink = $1::text AND q_p.partition_id = q.id AND q_p.last_offset < q.off",
        p.quoted()
    )
}

/// Check an existing progress table has the columns and the key the
/// statements above need.
pub fn verify(info: &TableInfo) -> Result<()> {
    const TEXT: u32 = 25;
    const VARCHAR: u32 = 1043;
    const INT8: u32 = 20;
    const TIMESTAMPTZ: u32 = 1184;
    const TIMESTAMP: u32 = 1114;
    let need: [(&str, &[u32], &str); 5] = [
        ("sink", &[TEXT, VARCHAR], "text"),
        ("partition_id", &[INT8], "bigint"),
        ("partition", &[TEXT, VARCHAR], "text"),
        ("last_offset", &[INT8], "bigint"),
        ("updated_at", &[TIMESTAMPTZ, TIMESTAMP], "timestamptz"),
    ];
    let mut wrong = Vec::new();
    for (name, oids, ty) in need {
        match info.column(name) {
            Some(c) if oids.contains(&c.type_oid) && !c.generated => {}
            Some(c) => wrong.push(format!("{name} is {} (needs {ty})", c.type_name)),
            None => wrong.push(format!("{name} {ty} is missing")),
        }
    }
    let mut pk = info.primary_key.clone();
    pk.sort();
    if pk != ["partition_id", "sink"] {
        wrong.push("the primary key must be (sink, partition_id)".to_string());
    }
    if wrong.is_empty() {
        Ok(())
    } else {
        Err(Error::fatal(
            "progress_table",
            format!(
                "{} is not a sink progress table: {}; the expected shape: {}",
                info.name,
                wrong.join(", "),
                ddl_text(&info.name)
            ),
        ))
    }
}

/// Make sure the progress table is there and usable: verified when it
/// exists; created when it does not and `create` allows (the schema first,
/// only when missing — `CREATE SCHEMA IF NOT EXISTS` needs CREATE on the
/// database even for a schema that exists); a role that may not create it, or
/// may not write it, gets a [`Error::Fatal`] `progress_table` naming the
/// statement to run.
pub async fn prepare_table(c: &tokio_postgres::Client, p: &TableName, create: bool) -> Result<()> {
    let info = match table_info(c, p).await? {
        Some(info) => info,
        None if !create => {
            return Err(Error::fatal(
                "progress_table",
                format!(
                    "{p} does not exist and createProgressTable is false; create it: {}",
                    ddl_text(p)
                ),
            ))
        }
        None => {
            // Every node of a cluster starts its sink at about the same
            // moment, so two of them create the table concurrently, and the
            // loser's `IF NOT EXISTS` does not save it: it fails on the
            // catalog's unique indexes (23505), on the table's row type
            // (42710), on the relation (42P07)... Whatever the error, the
            // catalog is asked again: a table that now exists was created by
            // someone else and is verified like any other.
            let [schema_ddl, table_ddl] = ddl(p);
            if !schema_exists(c, &p.schema).await? {
                if let Err(e) = c.batch_execute(&schema_ddl).await {
                    if !schema_exists(c, &p.schema).await? {
                        return Err(creation_error(&e, p));
                    }
                }
            }
            if let Err(e) = c.batch_execute(&table_ddl).await {
                if table_info(c, p).await?.is_none() {
                    return Err(creation_error(&e, p));
                }
            }
            table_info(c, p).await?.ok_or_else(|| {
                Error::fatal(
                    "progress_table",
                    format!("{p} was created but cannot be read back"),
                )
            })?
        }
    };
    verify(&info)?;
    // `has_table_privilege` with a list answers whether ANY is held: ask
    // each one.
    let row = c
        .query_one(
            "SELECT has_table_privilege($1::text::regclass, 'SELECT') \
             AND has_table_privilege($1::text::regclass, 'INSERT') \
             AND has_table_privilege($1::text::regclass, 'UPDATE')",
            &[&p.quoted()],
        )
        .await
        .map_err(|e| classify(&e))?;
    if !row.try_get::<_, bool>(0).unwrap_or(false) {
        return Err(Error::fatal(
            "progress_table",
            format!("the connector's role needs SELECT, INSERT and UPDATE on {p}: GRANT SELECT, INSERT, UPDATE ON {} TO <role>", p.quoted()),
        ));
    }
    Ok(())
}

async fn schema_exists(c: &tokio_postgres::Client, schema: &str) -> Result<bool> {
    Ok(c.query_opt(
        "SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1",
        &[&schema],
    )
    .await
    .map_err(|e| classify(&e))?
    .is_some())
}

/// A creation that failed and left nothing behind: a role that may not
/// create is told the statement to run as one that may.
fn creation_error(e: &tokio_postgres::Error, p: &TableName) -> Error {
    match e.code().map(|s| s.code()) {
        Some("42501") => Error::fatal(
            "progress_table",
            format!(
                "cannot create {p}: {}; run as a role that may: {}",
                e.as_db_error()
                    .map(|d| d.message())
                    .unwrap_or("permission denied"),
                ddl_text(p)
            ),
        ),
        _ => classify(e),
    }
}

/// The order a batch is applied in: partition ids ascending, each partition's
/// offsets ascending. A `(partition, offset)` met twice is listed once (the
/// first) and returned in the second list: applying it twice in one
/// transaction would slip past the progress filter, which only knows what
/// committed before.
pub fn apply_order(keys: &[(i64, i64)]) -> (Vec<usize>, Vec<usize>) {
    let mut idx: Vec<usize> = (0..keys.len()).collect();
    idx.sort_by_key(|&i| (keys[i].0, keys[i].1, i));
    let mut order = Vec::with_capacity(idx.len());
    let mut dups = Vec::new();
    for i in idx {
        match order.last() {
            Some(&p) if keys[p] == keys[i] => dups.push(i),
            _ => order.push(i),
        }
    }
    (order, dups)
}

/// The partitions of `chunk` (positions into `keys`), ascending: id, the
/// highest offset, and one member (for the name).
pub fn partitions_of(chunk: &[usize], keys: &[(i64, i64)]) -> Vec<(i64, i64, usize)> {
    let mut m: BTreeMap<i64, (i64, usize)> = BTreeMap::new();
    for &i in chunk {
        let (pid, off) = keys[i];
        m.entry(pid)
            .and_modify(|e| {
                if off > e.0 {
                    *e = (off, i)
                }
            })
            .or_insert((off, i));
    }
    m.into_iter().map(|(pid, (off, i))| (pid, off, i)).collect()
}

/// Split `chunk` by the locked progress: `(apply, skipped)`, both in chunk
/// order. A partition without a row (cannot happen after the insert; kept
/// total anyway) reads as `-1`.
pub fn filter(
    chunk: &[usize],
    keys: &[(i64, i64)],
    last: &HashMap<i64, i64>,
) -> (Vec<usize>, Vec<usize>) {
    chunk.iter().partition(|&&i| {
        let (pid, off) = keys[i];
        off > last.get(&pid).copied().unwrap_or(-1)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg::catalog::ColumnInfo;

    fn p() -> TableName {
        TableName {
            schema: "Ops".into(),
            name: "sink_progress".into(),
        }
    }

    #[test]
    fn statements_and_ddl_quote_the_table() {
        let [schema, table] = ddl(&p());
        assert_eq!(schema, "CREATE SCHEMA IF NOT EXISTS \"Ops\"");
        assert!(table.starts_with(
            "CREATE TABLE IF NOT EXISTS \"Ops\".\"sink_progress\" (sink text NOT NULL"
        ));
        assert!(table.ends_with("PRIMARY KEY (sink, partition_id))"));
        assert!(ensure_sql(&p()).contains("INSERT INTO \"Ops\".\"sink_progress\""));
        assert!(
            ensure_sql(&p()).contains("ORDER BY q.id ON CONFLICT (sink, partition_id) DO NOTHING")
        );
        assert!(lock_sql(&p()).ends_with("ORDER BY partition_id FOR UPDATE"));
        assert!(advance_sql(&p()).ends_with("q_p.last_offset < q.off"));
    }

    #[test]
    fn batches_apply_by_partition_then_offset_and_drop_repeats() {
        // (pid, offset) in pop order: two partitions interleaved, one repeat.
        let keys = [(9, 5), (3, 2), (9, 4), (3, 1), (3, 2), (1, 7)];
        let (order, dups) = apply_order(&keys);
        assert_eq!(order, vec![5, 3, 1, 2, 0]);
        assert_eq!(dups, vec![4]);
        assert_eq!(
            partitions_of(&order, &keys),
            vec![(1, 7, 5), (3, 2, 1), (9, 5, 0)]
        );
        assert_eq!(partitions_of(&[3], &keys), vec![(3, 1, 3)]);
    }

    #[test]
    fn the_filter_drops_what_progress_already_holds() {
        let keys = [(1, 10), (1, 11), (1, 12), (2, 0), (3, 5)];
        let last: HashMap<i64, i64> = [(1, 11), (2, -1)].into_iter().collect();
        let (apply, skipped) = filter(&[0, 1, 2, 3, 4], &keys, &last);
        assert_eq!(
            apply,
            vec![2, 3, 4],
            "above last_offset, and a fresh row (-1) takes offset 0"
        );
        assert_eq!(skipped, vec![0, 1], "at or below last_offset");
    }

    fn col(name: &str, oid: u32) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            attnum: 0,
            type_oid: oid,
            type_name: format!("oid{oid}"),
            not_null: true,
            generated: false,
        }
    }

    #[test]
    fn verify_names_what_is_wrong() {
        let mut info = TableInfo {
            oid: 1,
            name: p(),
            columns: vec![
                col("sink", 25),
                col("partition_id", 20),
                col("partition", 25),
                col("last_offset", 20),
                col("updated_at", 1184),
            ],
            primary_key: vec!["sink".into(), "partition_id".into()],
            replica_identity: b'd',
            identity_index: vec![],
        };
        assert!(verify(&info).is_ok());
        info.columns[3] = col("last_offset", 23);
        info.primary_key = vec!["sink".into()];
        let e = verify(&info).unwrap_err();
        assert_eq!(e.code(), "progress_table");
        let s = e.to_string();
        assert!(s.contains("last_offset is oid23 (needs bigint)"), "{s}");
        assert!(
            s.contains("primary key must be (sink, partition_id)"),
            "{s}"
        );
        assert!(s.contains("CREATE TABLE IF NOT EXISTS"), "{s}");
    }
}
