//! Unit SQL for the PURE helpers of `024_kv.sql` (PLAN_KV_TIMERS.md §6.1, test
//! plan §15 row 1 — the row that closes F1).
//!
//! These five functions touch no table, so they are the only part of the feature
//! that can be pinned without any data. They are also the part most likely to be
//! silently rewritten at the seventh call site, which is exactly why §6.1 says
//! they are written once. This file is that contract.
//!
//! WHY A RUST TEST AND NOT A `.sql` FILE. The repo has no SQL-test convention to
//! follow (there is no `server/sql/test/`, and the only `.sql` under `test-perf/`
//! are bench scripts). It does have a DB-backed convention — `embedded_smoke.rs`,
//! `hotlist_repairs.rs`, `hotlist_reseed_window.rs`: `#[ignore]`, a throwaway
//! Postgres named by `QUEEN_EMBEDDED_TEST_PG`, `Broker::start` purely to apply
//! the real schema, then a plain `tokio_postgres` client for the assertions. That
//! convention is followed here, and it is followed for a reason that a bare psql
//! script cannot give: the SQL is `include_str!`-embedded, so booting the broker
//! is the ONLY way to prove that the `.sql` file on disk is the one the binary
//! carries. A psql script would happily pass against a stale `cargo build`.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`:
//!
//! ```bash
//! docker run --rm -d --name queen-kvt-pg -e POSTGRES_PASSWORD=postgres -p 5471:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5471 cargo test --test kv_sql_helpers -- --ignored --nocapture
//! ```
//!
//! HOW THIS FILE FAILS. Every case is collected, never asserted in place, and the
//! whole report is raised at the end. Before the SPs exist that report is a wall
//! of `42883 undefined_function`, which is the point: a wall of 42883 says "not
//! written yet", a single `42601 syntax_error` in the middle would say "the test
//! itself is wrong". The two are distinguishable without editing anything.
//!
//! NO FLAG TO SET, AND THAT WAS ALREADY TRUE. This file never read
//! `QUEEN_KV_ENABLED` because §0 created the tables and functions at every boot
//! whatever the flags said — a deployment model with two possible schemas is not
//! a deployment model. The flag has since been removed entirely (kv is the
//! default, not a feature), so the schema and the surface finally agree.

use queen::{Broker, BrokerConfig};

/// One fixed instant for the whole file. Every expiry case is expressed relative
/// to it in SQL, so nothing here depends on the clock, on chrono, or on how long
/// the test took to reach a given line. It is also the discipline the feature
/// itself follows: `p_now` is a parameter, one instant per call (§5.7).
const NOW: &str = "TIMESTAMPTZ '2026-08-17 12:00:00+00'";

/// `(identity arguments, result)` exactly as §6.1 spells them, plus the two
/// properties that sentence claims for all of them: "IMMUTABLE PARALLEL SAFE".
struct HelperSpec {
    name: &'static str,
    args: &'static str,
    /// `None` where the plan does not pin the return type (`kv_check_names_v1`
    /// only ever RAISEs, so void and boolean are both defensible).
    result: Option<&'static str>,
    /// `Some(false)` where a STRICT declaration would silently break a
    /// documented total function; `None` where the plan does not say.
    strict: Option<bool>,
    why: &'static str,
}

const HELPERS: &[HelperSpec] = &[
    HelperSpec {
        name: "kv_live_v1",
        args: "timestamp with time zone, timestamp with time zone",
        result: Some("boolean"),
        // kv_live_v1(NULL, now) must be TRUE — NULL expires_at is "forever"
        // (§1.6). A STRICT declaration turns that into NULL, and a NULL in
        // `WHERE ... AND queen.kv_live_v1(...)` is a row that has vanished:
        // every "forever" key would read as absent, everywhere, silently.
        strict: Some(false),
        why: "§6.1: expires IS NULL OR expires > now; now is a PARAMETER, never a call",
    },
    HelperSpec {
        name: "kv_ver_v1",
        args: "bigint, timestamp with time zone, timestamp with time zone",
        result: Some("bigint"),
        // version(absent) = 0 is what makes expect:0 mean "must not exist"
        // without a magic flag (§6.1). STRICT would make it NULL instead.
        strict: Some(false),
        why: "§6.1: effective version under the expiry rule; version(absent) = 0, a TOTAL function",
    },
    HelperSpec {
        name: "kv_num_v1",
        args: "jsonb, timestamp with time zone, timestamp with time zone",
        result: Some("numeric"),
        // Same argument: an absent row counts as zero, so STRICT is wrong.
        strict: Some(false),
        why: "§5.4 repair 1: the effective numeric, zero when expired — inline in the WHERE, three times",
    },
    HelperSpec {
        name: "kv_prefix_end_v1",
        args: "text",
        result: Some("text"),
        // NULL prefix is not a case the plan describes; do not over-pin it.
        strict: None,
        why: "§6.1: byte-range upper bound; NULL = no computable bound, caller drops it and leans on starts_with",
    },
    HelperSpec {
        name: "kv_check_names_v1",
        args: "text, text",
        result: None,
        strict: None,
        why: "§6.1: namespace charset, non-empty key, QUEEN_KV_MAX_KEY_BYTES cap, no NUL. RAISE, not CHECK",
    },
];

/// A scalar case: `expr` rendered `::text`, compared against `want` (`None` = SQL NULL).
struct Case {
    expr: String,
    want: Option<&'static str>,
    why: &'static str,
}

fn case(expr: String, want: Option<&'static str>, why: &'static str) -> Case {
    Case { expr, want, why }
}

fn sqlstate(e: &tokio_postgres::Error) -> String {
    e.code()
        .map(|c| c.code().to_string())
        .unwrap_or_else(|| "<no sqlstate>".to_string())
}

/// `Display` for a tokio_postgres error is the useless string "db error"; the
/// server's own message lives on the DbError. A report that only says "db error"
/// makes a missing function and a typo in this file look identical, which is the
/// one distinction this file promises to keep.
fn detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(d) => format!("{}: {}", sqlstate(e), d.message()),
        None => format!("{}: {e}", sqlstate(e)),
    }
}

/// Every failure in this file lands here; nothing panics before the end.
struct Report(Vec<String>);

impl Report {
    fn fail(&mut self, what: &str, why: &str, detail: String) {
        self.0.push(format!("  ✗ {what}\n      plan: {why}\n      got:  {detail}"));
    }

    fn finish(self, heading: &str) {
        if !self.0.is_empty() {
            panic!(
                "\n{heading}\n{}\n\n{} failing case(s). A wall of 42883/42P01 means 024_kv.sql is \
                 not written yet — that is the expected state before F1. Any 42601 in that list \
                 is a bug in THIS file, not in the schema.\n",
                self.0.join("\n"),
                self.0.len()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn kv_pure_helpers_hold_their_contract() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // Boot the real broker purely to apply the real schema — the SQL under test
    // is include_str!-embedded, so this is also what proves the file compiled in.
    // Background loops off: this test reads nothing they write and they only add
    // DDL-vs-live-traffic noise to the apply.
    let broker = Broker::start(
        BrokerConfig::new()
            .pg(host.clone(), port, "postgres", "postgres", "postgres")
            .pool_size(4)
            .retention(false)
            .stats_refresh(false)
            .system_metrics(false)
            .log_reports(false),
    )
    .await
    .expect("broker start");

    let (c, conn) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres password=postgres dbname=postgres"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut r = Report(Vec::new());

    inventory_and_properties(&c, &mut r).await;
    live_cases(&c, &mut r).await;
    ver_cases(&c, &mut r).await;
    num_cases(&c, &mut r).await;
    prefix_end_cases(&c, &mut r).await;
    check_names_cases(&c, &mut r).await;

    broker.shutdown().await;
    r.finish("PLAN_KV_TIMERS §6.1 pure helpers — unit SQL (§15 row 1, F1)");
}

// ---------------------------------------------------------------- inventory

/// The five helpers exist, with the signature §6.1 writes, IMMUTABLE and
/// PARALLEL SAFE as §6.1 claims for all of them.
///
/// This runs first so that "not written yet" reads as one line per helper
/// instead of as thirty failed behaviour cases.
async fn inventory_and_properties(c: &tokio_postgres::Client, r: &mut Report) {
    for h in HELPERS {
        let rows = c
            .query(
                "SELECT pg_get_function_identity_arguments(p.oid),
                        pg_get_function_result(p.oid),
                        p.provolatile::text, p.proparallel::text, p.proisstrict
                   FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                  WHERE n.nspname = 'queen' AND p.proname = $1
                  ORDER BY 1",
                &[&h.name],
            )
            .await
            .expect("pg_proc lookup");

        if rows.is_empty() {
            r.fail(
                &format!("queen.{}: not defined", h.name),
                h.why,
                "no such function in schema `queen` — 024_kv.sql missing from PROCEDURES, \
                 or not written"
                    .to_string(),
            );
            continue;
        }

        // Exactly one overload. Two would mean the "written once" rule of §6.1
        // already failed, and every call site would resolve by luck.
        if rows.len() > 1 {
            let sigs: Vec<String> = rows.iter().map(|x| x.get::<_, String>(0)).collect();
            r.fail(
                &format!("queen.{}: {} overloads", h.name, rows.len()),
                "§6.1: the helpers are written ONCE — the risk is the seventh call site \
                 rewriting them differently",
                sigs.join(" | "),
            );
        }

        let row = &rows[0];
        let (args, result): (String, String) = (row.get(0), row.get(1));
        let (volatile, parallel): (String, String) = (row.get(2), row.get(3));
        let strict: bool = row.get(4);

        if args != h.args {
            r.fail(
                &format!("queen.{}: argument list", h.name),
                h.why,
                format!("want ({}), got ({args})", h.args),
            );
        }
        if let Some(want) = h.result {
            if result != want {
                r.fail(
                    &format!("queen.{}: return type", h.name),
                    h.why,
                    format!("want {want}, got {result}"),
                );
            }
        }
        if volatile != "i" {
            r.fail(
                &format!("queen.{}: not IMMUTABLE", h.name),
                "§6.1: \"Helper puri, tutti IMMUTABLE PARALLEL SAFE e inlinabili\" — a \
                 non-immutable helper cannot be inlined, so it is re-executed per row \
                 inside every predicate that names it",
                format!("provolatile = {volatile:?} (want \"i\")"),
            );
        }
        if parallel != "s" {
            r.fail(
                &format!("queen.{}: not PARALLEL SAFE", h.name),
                "§6.1: all five are PARALLEL SAFE",
                format!("proparallel = {parallel:?} (want \"s\")"),
            );
        }
        if let Some(want_strict) = h.strict {
            if strict != want_strict {
                r.fail(
                    &format!("queen.{}: STRICT is wrong", h.name),
                    "a STRICT declaration makes the documented total function return NULL \
                     on its most important input (absent row / forever key), and a NULL in \
                     a WHERE is a row that silently vanished",
                    format!("proisstrict = {strict} (want {want_strict})"),
                );
            }
        }
    }
}

// ------------------------------------------------------------- kv_live_v1

/// `expires IS NULL OR expires > now` — and the boundary, which is the half of
/// the contract that decides whether reader and sweeper agree (§5.7: "lo sweeper
/// cancella con expires_at <= cutoff, i lettori accettano con expires_at > now:
/// il confine coincide, una riga esattamente a now() e' morta per entrambi").
async fn live_cases(c: &tokio_postgres::Client, r: &mut Report) {
    let f = |e: &str| format!("queen.kv_live_v1({e}, {NOW})");
    let cases = vec![
        case(
            f("NULL::timestamptz"),
            Some("true"),
            "§1.6: NULL in column means FOREVER — an explicit, greppable opt-in, never a default",
        ),
        case(
            f(&format!("{NOW} + interval '1 hour'")),
            Some("true"),
            "§6.1: expires > now",
        ),
        case(
            f(&format!("{NOW} + interval '1 microsecond'")),
            Some("true"),
            "§6.1: strictly greater, at the smallest step Postgres can represent",
        ),
        case(
            f(NOW),
            Some("false"),
            "§5.7: a row exactly at now() is dead for BOTH the reader and the sweeper — \
             the boundary has to coincide or a key is alive for one and pruned by the other",
        ),
        case(
            f(&format!("{NOW} - interval '1 microsecond'")),
            Some("false"),
            "§5.7: an expired key is never returned and never counts as existing",
        ),
        case(
            f(&format!("{NOW} - interval '90 days'")),
            Some("false"),
            "§5.7: same verdict however long ago it died — the sweeper's lateness is not the truth",
        ),
    ];
    run_scalar(c, r, cases).await;
}

// -------------------------------------------------------------- kv_ver_v1

/// The version under the expiry rule. `version(absent) = 0` is the whole reason
/// `expect:0` can mean "must not exist" without a magic flag (§6.1), and the
/// expired case is what makes `put expect:0` win against a not-yet-pruned row —
/// the resurrection of §5.3.
async fn ver_cases(c: &tokio_postgres::Client, r: &mut Report) {
    let f = |v: &str, e: &str| format!("queen.kv_ver_v1({v}, {e}, {NOW})");
    let cases = vec![
        case(
            f("NULL::bigint", "NULL::timestamptz"),
            Some("0"),
            "§15 row 1 (absent): version(absent) = 0, a TOTAL function — this IS the definition \
             that lets expect:0 mean \"must not exist\"",
        ),
        case(
            f("NULL::bigint", &format!("{NOW} + interval '1 hour'")),
            Some("0"),
            "absent stays absent whatever the expiry column says",
        ),
        case(
            f("91005", "NULL::timestamptz"),
            Some("91005"),
            "§3.2: a forever key keeps its version; the token is opaque and is compared only \
             for equality, so it is returned verbatim, never recomputed",
        ),
        case(
            f("91005", &format!("{NOW} + interval '1 hour'")),
            Some("91005"),
            "live key keeps its version",
        ),
        case(
            f("91005", &format!("{NOW} - interval '1 second'")),
            Some("0"),
            "§15 row 1 (expired): an expired row reads as version 0, i.e. as ABSENT, so a \
             CAS against the dead lineage cannot succeed",
        ),
        case(
            f("91005", NOW),
            Some("0"),
            "§5.7: exactly at now() is dead — the same boundary as kv_live_v1, not a second one",
        ),
        case(
            f("9223372036854775807", "NULL::timestamptz"),
            Some("9223372036854775807"),
            "§3.2: the version is BIGINT from a sequence with CACHE 1000; the top of the range \
             must not be lost to an int cast",
        ),
    ];
    run_scalar(c, r, cases).await;
}

// -------------------------------------------------------------- kv_num_v1

/// The effective numeric. Exists because `v_next` cannot be a plpgsql variable
/// (§5.4 repair 1); its job is to be TOTAL, because the alternative — raising on
/// a non-numeric datum — puts a 22P02 inside the `WHERE` of the rate limiter's
/// UPDATE, and a rate limiter that throws is a customer at 100% rejection.
async fn num_cases(c: &tokio_postgres::Client, r: &mut Report) {
    // trim_scale so that a 0 produced as 0.00 still reads as "0": the contract is
    // the numeric value, not its scale.
    let f = |v: &str, e: &str| format!("trim_scale(queen.kv_num_v1({v}, {e}, {NOW}))");
    let live = format!("{NOW} + interval '1 hour'");
    let dead = format!("{NOW} - interval '1 second'");
    let cases = vec![
        case(f("'5'::jsonb", "NULL::timestamptz"), Some("5"), "§5.4: a forever counter reads its value"),
        case(f("'5'::jsonb", &live), Some("5"), "§5.4: a live counter reads its value"),
        case(f("'0'::jsonb", &live), Some("0"), "zero is a value, not an absence"),
        case(f("'-3.25'::jsonb", &live), Some("-3.25"), "§5.4: the type is numeric, so no overflow and no truncation server-side"),
        case(
            f("'123456789012345678901234567890.5'::jsonb", &live),
            Some("123456789012345678901234567890.5"),
            "§5.4: numeric, EXACT — a detour through double precision loses this and the loss is silent",
        ),
        case(
            f("'5'::jsonb", &dead),
            Some("0"),
            "§5.4: an expired counter is ZERO, and that is what makes a fixed-window limiter \
             one call instead of two",
        ),
        case(
            f("'5'::jsonb", NOW),
            Some("0"),
            "§5.7: exactly at now() is dead here too — one boundary for the whole feature",
        ),
        case(
            f("NULL::jsonb", "NULL::timestamptz"),
            Some("0"),
            "absent row = 0, so the INSERT branch and the UPDATE branch agree on the base",
        ),
        // §15 row 1 names "kv_num_v1 sul non numerico" as mandatory. The verdict
        // has to be 0 AND NOT an exception: the naive `(value->>0)::numeric`
        // raises 22P02 on every one of these.
        case(
            f("'\"abc\"'::jsonb", &live),
            Some("0"),
            "§15 row 1 (non-numeric): a string datum is 0, NOT a 22P02 — the guard of §5.4 \
             repair 3 already keeps this case out of incr's SET, so the helper's only job \
             here is to be total",
        ),
        case(f("'true'::jsonb", &live), Some("0"), "§15 row 1 (non-numeric): a boolean datum is 0, not an exception"),
        case(
            f("'null'::jsonb", &live),
            Some("0"),
            "§3.2: 'null'::jsonb is a LEGAL value — it must read as 0 and must not raise",
        ),
        case(f("'{\"count\": 1}'::jsonb", &live), Some("0"), "§15 row 1 (non-numeric): an object is 0, not its inner field and not an exception"),
        case(f("'[1, 2]'::jsonb", &live), Some("0"), "§15 row 1 (non-numeric): an array is 0, not its first element"),
        case(
            f("'\"abc\"'::jsonb", &dead),
            Some("0"),
            "§5.4 repair 3: the exact shape of the bug — someone put({count:0}), it expired, \
             and every incr must resume from zero instead of answering reason:'type' forever",
        ),
    ];
    run_scalar(c, r, cases).await;
}

// ------------------------------------------------------- kv_prefix_end_v1

/// The byte-range upper bound. The three cases §15 row 1 names by hand — empty
/// prefix, U+D7FF, the last code point — are the three that decide whether this
/// function is total. The range is only the index driver; `starts_with` carries
/// the semantics (§5.5), so returning NULL is always SAFE and never wrong, which
/// is why "no computable bound" is a value and not an error.
async fn prefix_end_cases(c: &tokio_postgres::Client, r: &mut Report) {
    // (id, prefix SQL, expected SQL, why)
    let cases: Vec<(&str, String, String, &str)> = vec![
        (
            "empty prefix",
            "''".into(),
            "NULL::text".into(),
            "§15 row 1: no string is an upper bound for every string, so the bound is NULL and \
             the caller drops it. getPrefix rejects an empty prefix at the edge (§5.5, \
             kv_prefix_required) — that is a SEPARATE guard, and this helper must still be total",
        ),
        ("single ascii", "'a'".into(), "'b'".into(), "§5.5: smallest text strictly above every 'a…'"),
        ("two ascii", "'ab'".into(), "'ac'".into(), "only the LAST code point advances"),
        (
            "colon",
            "'quota:'".into(),
            "'quota;'".into(),
            "the real shape of a key namespace (quota:<customer>:<hour>): ':' is 0x3A, so the \
             bound is ';'",
        ),
        (
            "percent is a byte",
            "'a%'".into(),
            "'a&'".into(),
            "§5.5/§1.5: '%' is a literal byte here, not a metacharacter — the whole reason the \
             predicate is starts_with() and never LIKE, which is the unescaped hole \
             009_streams_state_get_v1.sql carries today",
        ),
        (
            "underscore is a byte",
            "'a_'".into(),
            "'a' || chr(96)".into(),
            "§5.5: '_' is 0x5F, so the bound is 0x60 — LIKE's other metacharacter, also just a byte",
        ),
        (
            "multibyte",
            "'caf' || chr(233)".into(),
            "'caf' || chr(234)".into(),
            "§3.2: order is BYTE order under COLLATE \"C\", and for UTF-8 that is code point \
             order, so advancing the code point is the correct bound",
        ),
        (
            "U+D7FF alone (the chr() surrogate trap)",
            "chr(55295)".into(),
            "chr(57344)".into(),
            "§6.1: chr() REFUSES a surrogate in UTF8 (verified on PG16: chr(55296) => \
             \"requested character not valid for encoding\"), so the D800-DFFF block must be \
             skipped to U+E000. Without the guard this input does not return a wrong answer, \
             it RAISES, on a legal key",
        ),
        (
            "U+D7FF at the end",
            "'a' || chr(55295)".into(),
            "'a' || chr(57344)".into(),
            "§6.1: same jump when the surrogate boundary is not the first character",
        ),
        (
            "last code point at the end",
            "'a' || chr(1114111)".into(),
            "'b'".into(),
            "§15 row 1: U+10FFFF cannot advance (chr(1114112) raises \"too large\"), so the \
             helper backtracks and advances the previous code point instead",
        ),
        (
            "last code point, twice",
            "'a' || chr(1114111) || chr(1114111)".into(),
            "'b'".into(),
            "backtracking is a loop, not one step",
        ),
        (
            "last code point alone",
            "chr(1114111)".into(),
            "NULL::text".into(),
            "§15 row 1: nothing is above every 'U+10FFFF…', so the bound is NULL — the same \
             total-function answer as the empty prefix",
        ),
    ];

    for (id, prefix, want, why) in cases {
        // IS NOT DISTINCT FROM so NULL == NULL, and hex renderings so a failure
        // report is readable for inputs that no terminal will print.
        let sql = format!(
            "SELECT (queen.kv_prefix_end_v1({prefix}) IS NOT DISTINCT FROM ({want})),
                    coalesce(encode(convert_to(queen.kv_prefix_end_v1({prefix}), 'UTF8'), 'hex'), '<NULL>'),
                    coalesce(encode(convert_to(({want}), 'UTF8'), 'hex'), '<NULL>')"
        );
        match c.query_one(&sql, &[]).await {
            Ok(row) => {
                let ok: bool = row.get(0);
                if !ok {
                    let (got, wanted): (String, String) = (row.get(1), row.get(2));
                    r.fail(
                        &format!("kv_prefix_end_v1: {id}"),
                        why,
                        format!("want utf8[{wanted}], got utf8[{got}]"),
                    );
                }
            }
            Err(e) => r.fail(
                &format!("kv_prefix_end_v1: {id}"),
                why,
                detail(&e),
            ),
        }
    }

    // A property, not a constant: whatever bound the helper picks, it must
    // actually BOUND. This is what the range clause of §5.5 relies on, and it
    // catches an off-by-one that a table of literals could agree with.
    let probes = [
        ("'a'", "'a'"),
        ("'quota:'", "'quota:acme'"),
        ("'caf' || chr(233)", "'caf' || chr(233) || 'x'"),
        ("chr(55295)", "chr(55295) || chr(1114111)"),
        ("'a' || chr(55295)", "'a' || chr(55295) || chr(55295)"),
    ];
    for (p, x) in probes {
        let sql = format!(
            "SELECT queen.kv_prefix_end_v1({p}) IS NULL OR ({x}) COLLATE \"C\" < queen.kv_prefix_end_v1({p})"
        );
        match c.query_one(&sql, &[]).await {
            Ok(row) => {
                let ok: bool = row.get(0);
                if !ok {
                    r.fail(
                        &format!("kv_prefix_end_v1: bound property for prefix {p}"),
                        "§5.5: `k.key < v_end` is the index driver for the prefix scan — if a \
                         string starting with the prefix is NOT below the bound, the scan drops \
                         live rows and getPrefix silently under-reports",
                        format!("{x} is not below kv_prefix_end_v1({p})"),
                    );
                }
            }
            Err(e) => r.fail(
                &format!("kv_prefix_end_v1: bound property for prefix {p}"),
                "§5.5",
                detail(&e),
            ),
        }
    }
}

// ---------------------------------------------------- kv_check_names_v1

/// Namespace charset `^[a-z0-9][a-z0-9._-]{0,63}$`, non-empty key, at most
/// `QUEEN_KV_MAX_KEY_BYTES` (512, §9.2) bytes, no NUL. RAISE, not CHECK (§3.4).
///
/// The point of the charset is written at 024's header: a namespace is not
/// registered anywhere, so a typo does not fail — it MINTS a phantom namespace
/// you then read empty from, forever. The validation is what turns that silent
/// class of typo into an error.
async fn check_names_cases(c: &tokio_postgres::Client, r: &mut Report) {
    // (id, ns SQL, key SQL, why)
    let accepted: Vec<(&str, &str, &str, &str)> = vec![
        ("minimal", "'a'", "'k'", "one lowercase letter is a legal namespace"),
        ("digit first", "'0'", "'k'", "^[a-z0-9] — a digit is a legal first character"),
        ("full charset", "'a.b_c-d0'", "'k'", "[a-z0-9._-] after the first character"),
        ("64 chars", "'a' || repeat('z', 63)", "'k'", "{0,63} after the first = 64 characters total"),
        (
            "key is opaque",
            "'saga'",
            "'@p/orders/p3/workers/cursor: 100% _done_'",
            "§5.2: the SDK state handle mints keys like @p/<queue>/<partition>/<group>/<name>; \
             the key is NOT charset-restricted, only bounded — and LIKE metacharacters in it \
             are ordinary bytes (§5.5)",
        ),
        (
            "key at the byte cap",
            "'saga'",
            "repeat('k', 512)",
            "§9.2: QUEEN_KV_MAX_KEY_BYTES = 512, chosen so uuid(16) + ns(64) + key(512) sits at \
             a fifth of the ~2704-byte btree tuple ceiling",
        ),
        (
            "multibyte key under the cap",
            "'saga'",
            "repeat(chr(8364), 170)",
            "§9.2: the cap is BYTES — 170 euro signs are 510 bytes and must pass",
        ),
    ];

    for (id, ns, key, why) in accepted {
        let sql = format!("SELECT queen.kv_check_names_v1({ns}, {key})");
        if let Err(e) = c.query(&sql, &[]).await {
            r.fail(
                &format!("kv_check_names_v1 accepts: {id}"),
                why,
                format!("raised {}", detail(&e)),
            );
        }
    }

    let rejected: Vec<(&str, &str, &str, &str)> = vec![
        ("empty namespace", "''", "'k'", "^[a-z0-9] requires at least one character"),
        (
            "uppercase namespace",
            "'Saga'",
            "'k'",
            "§3.2: the charset exists so the COMMON typo classes cannot mint a phantom \
             namespace that reads empty forever; case is the first of them",
        ),
        ("leading dot", "'.saga'", "'k'", "^[a-z0-9] — a separator cannot lead"),
        ("leading dash", "'-saga'", "'k'", "^[a-z0-9] — a separator cannot lead"),
        ("leading underscore", "'_saga'", "'k'", "'_' is not in the charset at all"),
        ("space", "'sa ga'", "'k'", "not in [a-z0-9._-]"),
        ("slash", "'sa/ga'", "'k'", "not in [a-z0-9._-] — and a path separator invites a hierarchy that does not exist"),
        ("colon", "'sa:ga'", "'k'", "not in [a-z0-9._-]"),
        ("non-ascii", "chr(225) || 'bc'", "'k'", "^[a-z0-9] is ASCII"),
        ("65 chars", "'a' || repeat('z', 64)", "'k'", "{0,63} after the first = 64 total, so 65 is out"),
        ("empty key", "'saga'", "''", "§6.1: the key must be non-empty"),
        (
            "key one byte over the cap",
            "'saga'",
            "repeat('k', 513)",
            "§9.2: without the cap the failure is a 54000 at the first INSERT — runtime instead \
             of validation, and only for the unlucky key",
        ),
        (
            "multibyte key over the cap in BYTES, not characters",
            "'saga'",
            "repeat(chr(8364), 200)",
            "§9.2: 200 euro signs are 200 characters but 600 BYTES — an implementation that \
             wrote length() instead of octet_length() accepts this and then hits 54000 on the \
             btree tuple, which is precisely the failure the cap exists to move earlier",
        ),
    ];

    for (id, ns, key, why) in rejected {
        let sql = format!("SELECT queen.kv_check_names_v1({ns}, {key})");
        match c.query(&sql, &[]).await {
            Ok(_) => r.fail(
                &format!("kv_check_names_v1 rejects: {id}"),
                why,
                "accepted it".to_string(),
            ),
            Err(e) => {
                // Once the function exists, a class-42 error means the call did not
                // reach the validation at all (missing function, wrong arity, bad
                // SQL) — that is not a rejection, it is a different failure wearing
                // a rejection's clothes.
                let code = sqlstate(&e);
                if code.starts_with("42") {
                    r.fail(
                        &format!("kv_check_names_v1 rejects: {id}"),
                        why,
                        format!(
                            "{} — this is a resolution error, not a validation verdict",
                            detail(&e)
                        ),
                    );
                }
            }
        }
    }

    // NUL: §6.1 lists "nessun NUL" among the checks, and it cannot be tested the
    // way the other cases are — PostgreSQL text cannot CARRY a NUL, so the value
    // never reaches the function. Pinned as the property that actually holds:
    // the attempt fails, whoever refuses it. If a future encoding change ever
    // makes a NUL representable, this case starts passing vacuously, so the
    // assertion is on the refusal and the reason is written here.
    let sql = "SELECT queen.kv_check_names_v1('saga', $1::text)";
    if c.query(sql, &[&"a\0b"]).await.is_ok() {
        r.fail(
            "kv_check_names_v1 rejects: NUL in key",
            "§6.1: no NUL. Postgres text cannot represent one, so today the refusal comes from \
             the protocol layer rather than from the helper — either way the value must never \
             land in queen.kv",
            "accepted a key containing a NUL byte".to_string(),
        );
    }
}

// ------------------------------------------------------------------ runner

async fn run_scalar(c: &tokio_postgres::Client, r: &mut Report, cases: Vec<Case>) {
    for k in cases {
        let sql = format!("SELECT ({})::text", k.expr);
        match c.query_one(&sql, &[]).await {
            Ok(row) => {
                let got: Option<String> = row.get(0);
                let got_ref = got.as_deref();
                if got_ref != k.want {
                    r.fail(
                        &k.expr,
                        k.why,
                        format!(
                            "want {}, got {}",
                            k.want.map(|w| format!("{w:?}")).unwrap_or("NULL".into()),
                            got_ref.map(|g| format!("{g:?}")).unwrap_or("NULL".into())
                        ),
                    );
                }
            }
            Err(e) => r.fail(&k.expr, k.why, detail(&e)),
        }
    }
}
