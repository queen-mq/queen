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

/// THIS PIN IS THE INVERSE OF THE ONE IT REPLACES, and the reversal is the decision itself.
///
/// It used to assert that `/api/v1/kv` and `/api/v1/timers` were registered BEHIND a boot flag,
/// because §0 and §17.1 shipped both features off and "off" had to mean "the route does not
/// exist", not "the route exists and refuses". That is superseded (Alice, 2026-08-18): kv and
/// timers are the DEFAULT, part of the engine like push and pop, and there is no
/// `QUEEN_PUSH_ENABLED`. The existence of a boot flag is itself the claim that a surface is
/// optional, so flipping the defaults to `true` was not enough — the flags had to go.
///
/// The pin stays a source pin for the same reason it always was one: nothing OBSERVABLE
/// distinguishes "registered unconditionally" from "registered behind a flag that happens to be
/// on in this test process", so behaviour cannot catch a reintroduction.
///
/// WHAT IT FORBIDS IS THE QUOTED NAME, not the word. Reading an environment variable requires
/// its name as a string literal — `env_bool("QUEEN_KV_ENABLED", …)` — so the quoted form is
/// exactly the shape of a reintroduction and nothing else has that shape. Prose is deliberately
/// left free to name them, and the module headers do: somebody who meets one of these in an old
/// runbook, a client's README or a Helm values file will grep the source for it, and finding
/// the paragraph that says it was removed and what replaced it is the whole point. A pin that
/// banned the word too would delete the only answer that grep can give them.
#[test]
fn the_kv_and_timer_routes_have_no_boot_flag() {
    let main_rs = read("src/main.rs").expect("src/main.rs");
    assert!(
        main_rs.contains("/api/v1/kv") && main_rs.contains("/api/v1/timers"),
        "main.rs must register both surfaces unconditionally"
    );
    let mut offenders = Vec::new();
    for entry in walk_rs(&server_dir().join("src")) {
        let text = std::fs::read_to_string(&entry).unwrap_or_default();
        for flag in ["\"QUEEN_KV_ENABLED\"", "\"QUEEN_TIMERS_ENABLED\""] {
            if text.contains(flag) {
                offenders.push(format!("{} reads {flag}", entry.display()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "the kv/timers boot flags are gone and must stay gone — kv and timers are the default, \
         not a feature (the RUNTIME kill switches in src/switches.rs are a different thing and \
         are keyed on queen.system_state, never on the environment):\n  {}",
        offenders.join("\n  ")
    );
}

/// Every `.rs` under a directory, recursively.
fn walk_rs(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for e in entries.filter_map(|e| e.ok()) {
        let p = e.path();
        if p.is_dir() {
            out.extend(walk_rs(&p));
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
    out
}
