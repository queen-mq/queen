//! The SQL is the spec (§0.1, §8), so a column of it that no effect carries is
//! a silent feature loss — and this file reads the schema rather than trusting
//! that someone compared the two by eye.
//!
//! One table is checked: `queen.log_consumers`. It is the only row in the
//! catalogue that an effect claims to carry WHOLE — [`CursorRow`] is
//! documented as "the full `log_consumers` row … written whole on every
//! mutation" — which makes "every column has a slot" an exact rule rather than
//! a judgement call. The other tables are carried in part on purpose
//! (`log_partitions`'s counters are maintained by apply, D16; `log_segments`'s
//! blob lives in the segment files), and their coverage belongs to the
//! conformance suite of §13.2, not to a codec test.
//!
//! What it caught: `created_at` was missing. `log_partition_dead_v1`
//! (006 ≈623) vetoes deleting an empty, long-idle partition on
//! `c.created_at >= p_cutoff`, and for an autoAck-only group — whose
//! `lease_acquired_at` 004 NULLs — it is the only leg that can veto. The RSM
//! would have deleted a partition the oracle keeps (G-1), and the fix would
//! have cost a format change and a golden rewrite on the highest-rate effect
//! in the catalogue.
//!
//! The rule is one-directional: the RSM may carry MORE than the SQL row
//! (`delivered` is O16's, and has no column), never less.

use crate::rsm::effect::*;

/// The schema, embedded the way `schema.rs` embeds it: the test reads what the
/// binary ships, not a copy that could drift.
const LOG_SCHEMA: &str = include_str!("../../../sql/procedures/001_log_schema.sql");

/// Every column of `queen.<table>`: the `CREATE TABLE` block plus every later
/// `ALTER TABLE … ADD COLUMN` for it. The schema grows both ways — that is how
/// `lease_conflated` arrived — so a parser that read only the block would miss
/// exactly the columns added most recently.
fn sql_columns(sql: &str, table: &str) -> Vec<String> {
    let create = format!("CREATE TABLE IF NOT EXISTS {table} (");
    let mut cols: Vec<String> = Vec::new();
    let mut in_create = false;
    let mut altering_this_table = false;

    for raw in sql.lines() {
        // Drop line comments; the schema is heavily commented.
        let line = raw.split("--").next().unwrap_or("").trim();

        if in_create {
            if line.starts_with(')') {
                in_create = false;
                continue;
            }
            if let Some(first) = line.split_whitespace().next() {
                let keyword = first.to_ascii_uppercase();
                let is_constraint = matches!(
                    keyword.as_str(),
                    "PRIMARY" | "UNIQUE" | "FOREIGN" | "CONSTRAINT" | "CHECK" | "EXCLUDE"
                );
                if !is_constraint {
                    cols.push(first.trim_end_matches(',').to_string());
                }
            }
            continue;
        }

        // On the comment-stripped text, so a comment QUOTING the statement
        // (this schema does quote itself) cannot open a block.
        if line.contains(&create) {
            in_create = true;
            continue;
        }

        if line.starts_with("ALTER TABLE") {
            altering_this_table = line
                .split_whitespace()
                .nth(2)
                .map(|t| t.trim_end_matches(';'))
                == Some(table);
        }
        if altering_this_table {
            if let Some(at) = line.find("ADD COLUMN") {
                let mut rest = line[at + "ADD COLUMN".len()..].split_whitespace();
                let mut name = rest.next().unwrap_or("");
                if name.eq_ignore_ascii_case("IF") {
                    // IF NOT EXISTS <name>
                    rest.next();
                    rest.next();
                    name = rest.next().unwrap_or("");
                }
                if !name.is_empty() {
                    cols.push(name.to_string());
                }
            }
            if line.ends_with(';') {
                altering_this_table = false;
            }
        }
    }

    cols.sort();
    cols.dedup();
    cols
}

#[test]
fn every_log_consumers_column_has_a_slot_in_the_cursor_effect() {
    // (column, where it lives). The keys of the (pid, group) keyspace are on
    // the EFFECT; the rest are fields of the row it carries.
    let mapped: &[(&str, &str)] = &[
        ("partition_id", "Effect::CursorSet::pid (§6.1 keys by pid)"),
        ("consumer_group", "Effect::CursorSet::group"),
        ("committed", "CursorRow::committed"),
        ("batch_end", "CursorRow::batch_end"),
        ("worker_id", "CursorRow::worker"),
        ("lease_expires_at", "CursorRow::lease_expires_at_us"),
        ("lease_acquired_at", "CursorRow::lease_acquired_at_us"),
        ("batch_retry_count", "CursorRow::batch_retry_count"),
        ("attempt_offset", "CursorRow::attempt_offset"),
        ("attempt_count", "CursorRow::attempt_count"),
        ("total_consumed", "CursorRow::total_consumed"),
        ("lease_conflated", "CursorRow::lease_conflated"),
        ("created_at", "CursorRow::created_at_us"),
    ];

    let cols = sql_columns(LOG_SCHEMA, "queen.log_consumers");
    assert!(
        cols.len() >= 10,
        "the CREATE TABLE block was not found or was not parsed ({cols:?}): the failure is in \
         this test's reader, not in the schema"
    );

    for col in &cols {
        assert!(
            mapped.iter().any(|(name, _)| name == col),
            "queen.log_consumers.{col} has no slot in CursorSet/CursorRow. The SQL is the spec \
             (§8): carry the column, or say here why the RSM does not need it. Adding it later \
             is a format change on the highest-rate effect in the catalogue (§5.3)."
        );
    }
    for (name, place) in mapped {
        assert!(
            cols.iter().any(|c| c == name),
            "{name} is pinned to {place} but is no longer a column of queen.log_consumers"
        );
    }
    assert_eq!(cols.len(), mapped.len());
}

#[test]
fn the_cursor_row_carries_the_created_at_the_cleanup_veto_reads() {
    // The value survives the codec, and it is its own field: a row whose
    // `created_at` is inside the window but whose lease timestamps are NULL —
    // the autoAck-only group of 006 ≈596 — is exactly the case that has to
    // reach WP-2.7's cleanup intact.
    let row = CursorRow {
        committed: 12,
        batch_end: None,
        worker: None,
        lease_expires_at_us: None,
        lease_acquired_at_us: None,
        batch_retry_count: 0,
        attempt_offset: None,
        attempt_count: 0,
        total_consumed: 13,
        lease_conflated: false,
        delivered: vec![],
        created_at_us: 1_767_999_000_000_000,
        metadata: String::new(),
    };
    let e = Effect::CursorSet {
        pid: 7,
        group: "billing".into(),
        row,
    };
    let (back, _) = decode_effect(&encode_effect(&e)).expect("decode");
    assert_eq!(back, e);
    match back {
        Effect::CursorSet { row, .. } => {
            assert_eq!(row.created_at_us, 1_767_999_000_000_000);
            assert!(row.lease_acquired_at_us.is_none());
            assert!(row.lease_expires_at_us.is_none());
        }
        other => panic!("{other:?}"),
    }
}

/// The reader itself, on a schema written here: a parser that silently found
/// nothing would make the test above vacuous.
#[test]
fn the_column_reader_reads_blocks_and_later_alters() {
    let sql = "\
-- a comment mentioning queen.other
CREATE TABLE IF NOT EXISTS queen.demo (
    a UUID NOT NULL REFERENCES queen.x(id) ON DELETE CASCADE,
    b TEXT NOT NULL DEFAULT '__QUEUE_MODE__',  -- trailing comment
    -- a comment line
    c BIGINT,
    PRIMARY KEY (a, b)
);
ALTER TABLE queen.demo SET (fillfactor = 50);
ALTER TABLE queen.other
    ADD COLUMN IF NOT EXISTS not_ours BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE queen.demo
    ADD COLUMN IF NOT EXISTS d BOOLEAN NOT NULL DEFAULT FALSE;
";
    assert_eq!(sql_columns(sql, "queen.demo"), vec!["a", "b", "c", "d"]);
    assert!(sql_columns(sql, "queen.absent").is_empty());
}
