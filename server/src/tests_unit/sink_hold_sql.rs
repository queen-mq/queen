//! `SINK_HOLD_SQL` — the one statement the retention cycle runs against
//! `queen.kv` (PLAN_S3_SINK.md §5.3, decision D5).
//!
//! Wired into `src/db.rs`, which is where the statement lives, because the
//! §13.1 rule that keeps KV tenant-safe is "this file is the only code allowed
//! to bind the tenant" and this test is what makes that readable as an
//! assertion rather than as a comment.
//!
//! Everything here is TEXTUAL. The behavioural half — that the escaping this
//! statement rebuilds is byte-for-byte the sink's own, that an expired or
//! malformed pointer reads as absent, that the floor lands where §5.3 says —
//! is `tests/retention_sink_hold.rs`, against a live Postgres.

use super::*;

/// The statement's body, sliced out of the file so the assertions cannot be
/// satisfied by a comment somewhere else in `db.rs`.
fn hold_sql() -> &'static str {
    let src = include_str!("../db.rs");
    let (_, after) = src
        .split_once("const SINK_HOLD_SQL: &str =")
        .expect("SINK_HOLD_SQL is a named const");
    let (body, _) = after
        .split_once("/// One queue whose `retentionSinkHold`")
        .expect("the const ends before the SinkHold struct");
    body
}

/// THE lock-order assertion (024_kv.sql:15-30). The rule is about ACQUISITION
/// order — no lock on `queen.kv` after one on `queen.queues`,
/// `log_partitions` or `log_consumers` — and this read's whole claim to
/// legality is that it takes no row lock at all and runs before the work list.
/// A `FOR UPDATE` here, in any flavour, would make it an acquisition and put
/// the cycle inside the deadlock space the rule exists to keep it out of.
#[test]
fn the_hold_read_takes_no_row_lock() {
    let sql = hold_sql();
    for locker in [
        "FOR UPDATE",
        "FOR NO KEY UPDATE",
        "FOR SHARE",
        "FOR KEY SHARE",
    ] {
        assert!(
            !sql.contains(locker),
            "the hold read acquired a row lock: {locker}"
        );
    }
    // It must also stay a SELECT: an UPDATE of the pointer (a tempting way to
    // "mark it seen") is a write lock on queen.kv from the retention path.
    for write in [
        "UPDATE queen.kv",
        "INSERT INTO queen.kv",
        "DELETE FROM queen.kv",
    ] {
        assert!(!sql.contains(write), "the hold read writes: {write}");
    }
}

/// Tenant isolation, structurally: the KV row is matched on the QUEUE ROW'S own
/// tenant. Anything else — a parameter, a constant, a join through a name —
/// would let one tenant's commit pointer floor another tenant's retention, and
/// the failure would be silent in the safe-looking direction (data kept) until
/// the day it is not.
#[test]
fn the_kv_probe_is_scoped_by_the_queues_own_tenant() {
    let sql = hold_sql();
    assert!(sql.contains("ON k.tenant_id = h.tenant_id"));
    assert!(
        !sql.contains("$1"),
        "the hold read takes no parameters at all"
    );
    // The namespace is a literal, not a knob: the sink's namespace is part of
    // the protocol (§4.3), and an operator-settable one would be a way to point
    // retention at a document a tenant chose.
    assert!(sql.contains("k.namespace = 'queen-s3'"));
    // Expired pointers read as ABSENT, through the same predicate the KV
    // surface itself uses — a pointer whose TTL has run out must not hold
    // retention for ever.
    assert!(sql.contains("queen.kv_live_v1(k.expires_at, now())"));
}

/// The key shape of §4.3, and the explicit collation that lets it be compared
/// at all: `queen.kv.key` is `TEXT COLLATE "C"` and the key we build here is
/// concatenated from `queen.queues` columns, which carry the database default.
/// Two conflicting IMPLICIT collations is 42P21 at PARSE time (the verdict
/// `tests/kv_collation_42p22.rs` measured), i.e. a cycle that fails every time,
/// for everyone. The explicit `COLLATE "C"` is the correction.
#[test]
fn the_key_is_the_documented_shape_with_an_explicit_collation() {
    let sql = hold_sql();
    assert!(sql.contains("'s3:' || q.retention_sink_hold || ':' || e.esc || ':committed'"));
    assert!(sql.contains("k.key = h.kv_key COLLATE"));
    assert!(
        sql.contains("\\\"C\\\""),
        "the collation must be spelled, and it must be C"
    );
}

/// The escape set, byte for byte: `[A-Za-z0-9._-]` survives, everything else
/// becomes `%` + two UPPERCASE hex digits, per BYTE of the UTF-8 encoding.
/// It is the same set and the same function as
/// `connectors/queen-s3/src/layout.rs::escape` and as the Kafka offset store's
/// (`queen.kv_qk_unescape` inverts it, 010_log_admin).
///
/// Asserted numerically rather than by reading the connector's source: that
/// file is outside this package, so an `include_str!` of it would not survive
/// `cargo package`. The behavioural agreement — a queue named `a b/c` finding
/// the key `s3:default:a%20b%2Fc:committed` — is pinned live in
/// `tests/retention_sink_hold.rs`.
#[test]
fn the_escape_set_is_the_sinks_own() {
    let sql = hold_sql();
    // 48..57 = '0'..'9', 65..90 = 'A'..'Z', 97..122 = 'a'..'z'.
    for range in [
        "BETWEEN 48 AND 57",
        "BETWEEN 65 AND 90",
        "BETWEEN 97 AND 122",
    ] {
        assert!(sql.contains(range), "missing survivor range: {range}");
    }
    // 45 = '-', 46 = '.', 95 = '_' — and nothing else.
    assert!(sql.contains("IN (45, 46, 95)"));
    assert_eq!((b'-', b'.', b'_'), (45, 46, 95));
    assert_eq!(
        (b'0', b'9', b'A', b'Z', b'a', b'z'),
        (48, 57, 65, 90, 97, 122)
    );
    // UPPERCASE hex, zero-padded to two digits, one `%` per byte.
    assert!(sql.contains("'%' || upper(lpad(to_hex(get_byte(qn.nb, s.i)), 2, '0'))"));
    // Per BYTE of UTF-8, not per character: a non-ASCII name must produce one
    // escape per byte, which is what `escape` does and what makes the two
    // agree on anything outside ASCII.
    assert!(sql.contains("convert_to(q.name, 'UTF8')"));
    assert!(sql.contains("generate_series(0, octet_length(qn.nb) - 1)"));
}

/// `value` is tenant-writable JSONB, so `tEnd` can be anything. The regex is
/// what keeps this statement TOTAL: only the sink's own shape — ISO-8601
/// microseconds with a literal `Z`, which also removes every timezone
/// ambiguity from the cast — reaches the cast at all, and anything else reads
/// as NULL, i.e. as "this sink has not committed". That is the fail-safe
/// direction: the queue keeps everything younger than its cap.
#[test]
fn a_tenant_written_pointer_cannot_break_the_cast() {
    let sql = hold_sql();
    assert!(sql.contains("k.value->>'tEnd' ~"));
    assert!(
        sql.contains("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?Z$")
    );
    // The cast is downstream of the regex, never beside it.
    let (gate, rest) = sql.split_once("~").expect("the regex gate is present");
    assert!(
        !gate.contains("::timestamptz"),
        "something casts before the gate"
    );
    assert!(rest.contains("THEN (k.value->>'tEnd')::timestamptz END AS t_end"));
    // A `Z` is REQUIRED. Without it `::timestamptz` reads the value in the
    // session's TimeZone, which is how a floor silently moves by hours — the
    // same class as the to_char/AT TIME ZONE sweep.
    assert!(sql.contains("Z$"));
}

/// The slack is subtracted twice in this statement, and both must be the same
/// number: once to build the floor an operator reads in the log line, once to
/// decide whether the CAP produced it. A drift between them makes the line say
/// "by-sink" while the cap is what is holding.
#[test]
fn the_floor_and_the_cap_verdict_use_the_same_slack() {
    let sql = hold_sql();
    let term = format!("ce.t_end - make_interval(secs => {SINK_HOLD_SLACK_SECS})");
    assert_eq!(sql.matches(term.as_str()).count(), 2);
    assert_eq!(
        sql.matches("now() - make_interval(secs => h.cap_s)")
            .count(),
        2,
        "the cap term likewise"
    );
    // The floor is GREATEST(pointer, cap) and the verdict is "the cap is the
    // greater one", so they cannot disagree by construction.
    assert!(sql.contains("GREATEST(ce.t_end - make_interval"));
    assert!(sql.contains("(ce.t_end IS NULL"));
}

/// `AS MATERIALIZED` on the key CTE, and it is a MEASURED keyword rather than a
/// stylistic one. A CTE referenced once is inlined since PostgreSQL 12, and the
/// inlined form pushes the built key out of the index cond and into a join
/// filter — so each held queue then reads every pointer the tenant has in the
/// `queen-s3` namespace instead of doing one PK probe, which is quadratic in
/// the number of queues feeding a sink, every retention cycle. The const's own
/// header carries the two plans.
#[test]
fn the_key_probe_stays_a_pk_lookup() {
    let sql = hold_sql();
    assert!(
        sql.contains("WITH h AS MATERIALIZED ("),
        "the key CTE must be MATERIALIZED, or the PK probe degrades to a namespace scan"
    );
    // The three PK columns, all three bound in the join, in the order the index
    // has them.
    assert!(sql.contains("ON k.tenant_id = h.tenant_id"));
    assert!(sql.contains("AND k.namespace = 'queen-s3'"));
    assert!(sql.contains("AND k.key = h.kv_key COLLATE"));
}

/// Only queues that NAME a sink are read at all. Without this the statement
/// would build a key and probe queen.kv for every queue in the cell, every
/// cycle — Θ(#queues) index probes for a feature nobody switched on.
#[test]
fn a_queue_with_no_sink_is_not_probed() {
    assert!(hold_sql().contains("WHERE q.retention_sink_hold <> ''"));
}
