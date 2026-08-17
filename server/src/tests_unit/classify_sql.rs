//! The ONE SQLSTATE classifier (PLAN_KV_TIMERS §7.6), extracted from
//! `file_buffer.rs::classify_push_error` into `db.rs` and split three ways.
//!
//! Wired from `src/db.rs` — see `src/tests_unit/README.md` for the three-line block.
//!
//! Two things are under test and they are not the same thing:
//!
//!  1. **Parity.** The spool's drain loop has classified push errors for a long time and its
//!     behaviour must not move by a single code. `Transient` here must be exactly the old
//!     transient set (`40001`, `40P01`, classes `08`/`53`/`57`/`58`, and no SQLSTATE at all);
//!     everything the old code called permanent must stay in `Config | Permanent`, which the
//!     spool collapses back to `DrainErr::Permanent`. A drift in either direction changes what
//!     gets quarantined on disk, silently.
//!  2. **The new split.** Class `42` becomes `Config`, because "the destination queue name is
//!     malformed" and "this payload violates a constraint" are different operational events:
//!     an operator can repair the first and nobody can repair the second, and the timer path
//!     logs them differently (`queen_timers_fire_failures_total{class}` has three values,
//!     §14.2). `Config` still consumes the DLQ budget — it is a permanent-shaped failure with a
//!     different name — which is why the parity assertion above is written over
//!     `Transient` vs `not Transient` and not over three-way equality.
//!
//! The classifier under test is the pure `classify_sqlstate(Option<&str>)`, not the
//! `&tokio_postgres::Error` wrapper: `tokio_postgres::DbError` has no public constructor, so a
//! classifier that only accepts an `Error` cannot be unit-tested at all. That is a constraint on
//! the implementation, not a convenience for the test.

use super::*;

/// Every code the transient set names explicitly, plus a spread over each transient CLASS —
/// including codes that do not exist, because the rule is on the two-character class and a
/// future PostgreSQL adding `53400` must land transient without anyone editing this list.
const TRANSIENT: &[&str] = &[
    "40001", // serialization_failure
    "40P01", // deadlock_detected
    // class 08 — connection exception
    "08000", "08003", "08006", "08001", "08004", "08007", "08P01", "08999",
    // class 53 — insufficient resources (out of memory, too many connections, disk full)
    "53000", "53100", "53200", "53300", "53400", "53999",
    // class 57 — operator intervention (query_canceled, admin_shutdown, crash_shutdown)
    "57000", "57014", "57P01", "57P02", "57P03", "57P04", "57999",
    // class 58 — system error (io_error, undefined_file, duplicate_file)
    "58000", "58030", "58P01", "58P02", "58999",
];

/// Class 42 and nothing else. Every one of these is reachable from this feature:
/// `42883` is the broker-new/database-old case the plan declares (§6.3), `42P22` is the
/// collation ambiguity §2.4 C3 tells us to go looking for on the rig, `42703` is the missing
/// column a `CREATE TABLE IF NOT EXISTS` silently keeps (§3.2), and `42601` is a syntax error
/// in a generated statement.
const CONFIG: &[&str] = &[
    "42000", "42601", "42703", "42P01", "42P22", "42883", "42804", "42P02", "42999",
];

/// A spread of what must stay permanent, chosen so each one would be a plausible mistake to
/// classify otherwise.
const PERMANENT: &[&str] = &[
    "23514", // check_violation — the `required` escalation of a lost KV precondition (§6.1)
    "23503", // foreign_key_violation — the queue deleted mid-fire (§12)
    "23505", // unique_violation
    "22001", // string_data_right_truncation — the KV value ceiling (§9.2)
    "22023", // invalid_parameter_value — a `tenant` field smuggled into an op (§6.1 point 6)
    "P0001", // raise_exception — every RAISE the SPs make
    "22P02", // invalid_text_representation
    "25006", // read_only_sql_transaction — a hot standby, NOT retryable in place
    "55P03", // lock_not_available — class 55 was never in the transient set; keep it out
    "55006", // object_in_use
    "40002", // integrity_constraint_violation: class 40 is NOT transient wholesale
    "40003", // statement_completion_unknown
    "3D000", // invalid_catalog_name
    "28P01", // invalid_password
];

fn c(code: &str) -> SqlClass {
    classify_sqlstate(Some(code))
}

#[test]
fn transient_set_is_exactly_the_historical_one() {
    for code in TRANSIENT {
        assert_eq!(
            c(code),
            SqlClass::Transient,
            "{code} must be transient: the spool leaves the file and retries, and the timer \
             path must NOT consume an attempt for it (§12: the DLQ budget is never spent on \
             infrastructure)"
        );
    }
}

#[test]
fn no_sqlstate_is_transient() {
    // A connection-level failure carries no DbError at all. It is the single most common
    // transient in a rolling restart, and reading it as permanent would quarantine a healthy
    // spool file / burn a timer's attempts because a pod moved.
    assert_eq!(classify_sqlstate(None), SqlClass::Transient);
}

#[test]
fn class_42_is_config_and_only_class_42() {
    for code in CONFIG {
        assert_eq!(
            c(code),
            SqlClass::Config,
            "{code} is class 42: an operator can repair it, so it gets its own counter label \
             and an unsampled WARN naming queue and timer key (§4.5)"
        );
    }
    for code in TRANSIENT.iter().chain(PERMANENT.iter()) {
        assert_ne!(c(code), SqlClass::Config, "{code} must not be Config");
    }
}

#[test]
fn everything_else_is_permanent() {
    for code in PERMANENT {
        assert_eq!(
            c(code),
            SqlClass::Permanent,
            "{code} must be permanent: retrying the same data fails the same way, so the spool \
             quarantines it and the timer spends an attempt"
        );
    }
}

#[test]
fn config_and_permanent_are_both_non_transient() {
    // The parity assertion that actually protects the spool: file_buffer maps
    // `Transient => DrainErr::Transient` and everything else to `DrainErr::Permanent`, so what
    // must not move is the BOUNDARY, not the three-way split.
    for code in CONFIG.iter().chain(PERMANENT.iter()) {
        assert_ne!(
            c(code),
            SqlClass::Transient,
            "{code} was permanent before the extraction and must not become retryable"
        );
    }
}

#[test]
fn short_and_malformed_codes_do_not_panic() {
    // The old implementation sliced with `&code[..2.min(code.len())]` precisely because a code
    // shorter than two characters is possible in principle. `panic = "abort"` means a panic in
    // the sweeper task kills the broker, so this is a liveness test, not a tidiness one.
    //
    // "\u{e9}a" is the one that actually bites: it is three BYTES, so `&code[..2]` splits a
    // char and panics. The class prefix must be taken over bytes (`code.as_bytes()`) or with
    // `char_indices`, never with a naive byte range.
    for code in ["", "4", "0", "?", "4\u{0}", "\u{e9}", "\u{e9}a", "\u{1f600}"] {
        let _ = c(code);
    }
    assert_eq!(c(""), SqlClass::Permanent, "an empty code is not a reason to retry forever");
}

#[test]
fn matching_is_case_sensitive_by_design() {
    // PostgreSQL sends SQLSTATEs uppercase. Adding case folding to a classifier on the fire's
    // error path would be pure cost for an input that cannot occur, and it would make the
    // transient set fuzzy. Pinned so nobody "fixes" it.
    //
    // Only the two whole-code comparisons can see case at all — every class prefix in the
    // ruleset is digits — so `40p01` is the whole test surface, and it must NOT be read as the
    // deadlock code. (`42p22` still lands Config: its class is the digits `42`.)
    assert_eq!(c("40P01"), SqlClass::Transient);
    assert_eq!(c("40p01"), SqlClass::Permanent);
    assert_eq!(c("40001"), SqlClass::Transient);
}

#[test]
fn the_classifier_is_pure() {
    // No clock, no env, no interior mutability: the fire's poison-isolation branch (§7.6) calls
    // it once per segment per retry and must get the same verdict every time, or a batch could
    // oscillate between isolated and batched replay.
    for code in TRANSIENT.iter().chain(CONFIG).chain(PERMANENT) {
        assert_eq!(c(code), c(code), "{code} classified differently on two calls");
    }
}
