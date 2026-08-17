//! Source-level pins for PLAN_KV_TIMERS. No database, no broker, no `#[ignore]`: these read the
//! repository's own text and run in the ordinary `cargo test`.
//!
//! Three of them are **self-arming**. They are written before `024_kv.sql`, `025_log_timers.sql`
//! and the extracted SQLSTATE classifier exist, and they must not go red on a tree that has not
//! reached that phase yet — so each one states its precondition, skips loudly with a `SKIPPED`
//! line while the precondition is unmet, and turns into a hard assertion the moment the artifact
//! it guards appears. Nobody has to remember to enable them.
//!
//! Why file text and not behaviour, for each one, is written at the test.
//!
//! ```bash
//! cargo test --test kv_timers_source_pins -- --nocapture
//! ```

use std::path::{Path, PathBuf};

fn server_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(rel: &str) -> Option<String> {
    std::fs::read_to_string(server_dir().join(rel)).ok()
}

fn skip(what: &str, why: &str) {
    println!("SKIPPED {what}: {why}");
}

/// Every `.sql` under a directory, by file name, sorted.
fn sql_files(dir: &Path) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(dir)
        .expect("sql/procedures must exist")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".sql"))
        .collect();
    out.sort();
    out
}

/// The gotcha that costs half a day and produces no error message: the schema applier walks the
/// `PROCEDURES` list in `schema.rs`, **not** the directory. A new `.sql` file that nobody added
/// to that list is never applied — the boot is green, the tables are simply absent, and the
/// first call fails at runtime with `42883`, classified as configuration and therefore not
/// retried. This is the whole failure mode of §3.1, made mechanical.
///
/// Not self-arming: it holds today and must keep holding for 024 and 025 the day they land.
#[test]
fn every_procedures_file_is_in_the_schema_rs_list() {
    let schema_rs = read("src/schema.rs").expect("src/schema.rs");
    let list_start = schema_rs.find("const PROCEDURES").expect("the PROCEDURES list");
    let list = &schema_rs[list_start..];
    let list_end = list.find("];").map(|i| i + 2).unwrap_or(list.len());
    let list = &list[..list_end];

    for name in sql_files(&server_dir().join("sql/procedures")) {
        assert!(
            list.contains(&format!("(\"{name}\"")),
            "sql/procedures/{name} exists but is not in the PROCEDURES list of src/schema.rs, so \
             it is NEVER applied at boot. Add:\n    (\"{name}\", \
             include_str!(\"../sql/procedures/{name}\")),"
        );
        assert!(
            list.contains(&format!("include_str!(\"../sql/procedures/{name}\")")),
            "PROCEDURES names {name} but does not include_str! that exact path — the entry would \
             embed some other file's text"
        );
    }
}

/// §7.6: **one** SQLSTATE classifier in the whole broker. The extraction moves the rules from
/// `file_buffer.rs` into `db.rs` and splits class 42 out as `Config`; what makes the extraction
/// real rather than a copy is that the old literals stop existing at the old site. A second
/// table of SQLSTATEs does not fail loudly — it drifts, and the drift shows up as a spool that
/// quarantines something the timer path retries, months later.
///
/// Self-arming: silent until `classify_sqlstate` lands in `db.rs`.
#[test]
fn the_sqlstate_rules_live_in_exactly_one_place() {
    let db = read("src/db.rs").expect("src/db.rs");
    if !db.contains("fn classify_sqlstate") {
        skip("one-classifier pin", "db.rs has no classify_sqlstate yet (F3, §7.6)");
        return;
    }
    let fb = read("src/file_buffer.rs").expect("src/file_buffer.rs");
    for code in ["40001", "40P01", "\"08\"", "\"53\"", "\"57\"", "\"58\""] {
        assert!(
            !fb.contains(code),
            "file_buffer.rs still carries its own SQLSTATE rule ({code}) after db.rs grew the \
             shared classifier. §7.6 wants ONE classifier: file_buffer must call \
             db::classify_sql and map Transient => DrainErr::Transient, Config | Permanent => \
             DrainErr::Permanent."
        );
    }
    assert!(
        fb.contains("classify_sql"),
        "file_buffer.rs must call the shared classifier once it exists"
    );
}

// The "grep meccanico" row of §15 — no handler may name `queen.kv` or `queen.log_timers` — is
// NOT here on purpose: `tests/kv_handler_isolation.rs` already owns it. Two files asserting one
// rule means one of them gets relaxed by whoever hits it first.

/// §0 and §17.1: both features ship OFF, and OFF means the routes are **not registered at all**
/// (404 from the JSON fallback), not registered-and-guarded. A guarded route is a route: it
/// still costs a matchit entry, it still answers differently from a broker that never heard of
/// the feature, and "default off" stops being a property of the binary and becomes a property of
/// a runtime check somebody can invert in one line.
///
/// Self-arming: silent until `main.rs` learns the word `timers`.
#[test]
fn the_new_routes_are_registered_behind_a_boot_flag() {
    let main_rs = read("src/main.rs").expect("src/main.rs");
    if !main_rs.contains("/api/v1/timers") && !main_rs.contains("/api/v1/kv") {
        skip("flag-gated route registration", "main.rs registers no kv/timer route yet");
        return;
    }
    for (route, flag) in
        [("/api/v1/kv", "QUEEN_KV_ENABLED"), ("/api/v1/timers", "QUEEN_TIMERS_ENABLED")]
    {
        if !main_rs.contains(route) {
            continue;
        }
        assert!(
            main_rs.contains(flag),
            "{route} is registered but {flag} is never read in main.rs: with the flag off the \
             route must not be registered at all (§0), or a cloud cell that never enabled the \
             feature answers something other than 404"
        );
    }
    // The two flags are independent (§0): one env var gating both would make "timers on, KV off"
    // unexpressible, and that is a shipping configuration.
    if main_rs.contains("/api/v1/kv") && main_rs.contains("/api/v1/timers") {
        assert!(
            main_rs.contains("QUEEN_KV_ENABLED") && main_rs.contains("QUEEN_TIMERS_ENABLED"),
            "the two features are independent on the release path (§0): two flags, not one"
        );
    }
}
