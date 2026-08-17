//! MECHANICAL ISOLATION GATE — the "Grep meccanico" row of `PLAN_KV_TIMERS.md`
//! §15, which closes phase F2 together with the KV semantics suite.
//!
//! §13.1 makes tenant isolation for these two features structural rather than
//! reviewed: `p_tenant` is an ARGUMENT of every KV/timer stored procedure and is
//! never read from an operation (§6.1 point 6), and the only code allowed to
//! bind that argument is the wrapper layer in `src/db.rs`. The moment a handler
//! writes its own SQL against `queen.kv` or `queen.log_timers`, that argument
//! becomes something a request body can influence, and the failure is silent:
//! rows of the wrong tenant, no error anywhere.
//!
//! Today that rule is documentary. This file makes it mechanical, which is the
//! entire point — ten lines of grep are cheaper than a reviewer remembering.
//!
//! Unlike the semantics suite this needs no Postgres, so it runs in the plain
//! `cargo test` matrix (the `rust` suite of `test/run.sh`) with no registration.

use std::path::{Path, PathBuf};

/// Table names, not function names: a handler that reaches for either table by
/// any route — SELECT, JOIN, a hand-written CTE — is the thing being forbidden.
const FORBIDDEN_IN_HANDLERS: [&str; 2] = ["queen.kv", "queen.log_timers"];

fn manifest(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(rel)
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()));
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

#[test]
fn handlers_never_name_the_kv_or_timer_tables() {
    let dir = manifest("src/handlers");
    let mut files = Vec::new();
    rust_files(&dir, &mut files);
    assert!(!files.is_empty(), "no handler sources found under {}", dir.display());

    let mut offenders = Vec::new();
    for f in &files {
        let body = std::fs::read_to_string(f).expect("read handler source");
        for (n, line) in body.lines().enumerate() {
            for needle in FORBIDDEN_IN_HANDLERS {
                if line.contains(needle) {
                    offenders.push(format!(
                        "{}:{}  {}",
                        f.file_name().unwrap().to_string_lossy(),
                        n + 1,
                        line.trim()
                    ));
                }
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "PLAN_KV_TIMERS §13.1/§15: no file under src/handlers/ may name \
         `queen.kv` or `queen.log_timers`. The SQL for these two features lives \
         behind the wrappers in src/db.rs, which are the only place that binds \
         `p_tenant`. Offending lines:\n  {}",
        offenders.join("\n  ")
    );
}

/// The other half of the same rule, and the half a pure negative grep cannot
/// give: the wrapper has to actually exist somewhere, or "no handler names the
/// table" is satisfied by a feature that was never written.
#[test]
fn the_kv_wrapper_lives_in_db_rs() {
    let db = std::fs::read_to_string(manifest("src/db.rs")).expect("read src/db.rs");
    assert!(
        db.contains("queen.kv_apply_v1"),
        "PLAN_KV_TIMERS §13.1: `queen.kv_apply_v1` must be called from src/db.rs \
         — that wrapper is the single place allowed to bind `p_tenant`, and the \
         handler-side grep above is only meaningful once it exists."
    );
}
