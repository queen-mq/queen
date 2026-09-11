//! MERGE SEMANTICS of `queen.configure_queue_v1` — `PLAN_DASHBOARD_ACTIONS.md`
//! §2.2, the SQL half of W2.
//!
//! WHAT CHANGED AND WHY IT NEEDS A TEST AT ALL. Until 1.6.0 this SP parsed every
//! one of its 21 options with a hard-coded default and assigned all 21 columns
//! from `EXCLUDED`, so a body that mentioned only `retryLimit` reset
//! `dedupWindowSeconds` to 3600, `leaseTime` to 300, retention to off and the
//! sink hold to off. Every client sends only the options its caller set (Go's
//! `omitempty`, the CLI's flags, the dashboard's edit form), so that reset was
//! reachable from one command line. The rule now is: absent = keep, an explicit
//! `null` = the default, a value = set, and `replace: true` (or a queue that
//! does not exist yet) = the old behaviour for every column.
//!
//! WHY THIS LEVEL. The rule lives in SQL, once, and every surface inherits it
//! from there: the HTTP handler, the embedded broker (which never passes through
//! a socket), the transaction wire and seven clients. A test driven through HTTP
//! would prove it for one of those and nothing about the rest, so most cases
//! here call the SP directly — on the same connection kind the broker uses.
//!
//! The handler's own share of the feature is the `mode` -> `replace`
//! translation, and it gets the last two cases, driven through
//! `Broker::configure` (which IS `handle_configure` with the socket taken out).
//! The pure halves have unit tests next to them
//! (`handlers::queues::configure_mode`); what only a real call can show is that
//! the flag those functions produce actually reaches the SP, and what happens to
//! the queue when the handler refuses.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`,
//! same convention as `kv_semantics` and `timers_count`:
//!
//! ```bash
//! docker run --rm -d --name queen-w2-pg -e POSTGRES_PASSWORD=postgres -p 5472:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5472 cargo test --test configure_merge_semantics -- --ignored --nocapture
//! ```
//!
//! ONE test function on purpose (a Broker is booted to apply the real,
//! `include_str!`-embedded schema, and the admission arbiter is process-global).
//! The cases are still reported one by one: each returns `Result<(), String>`
//! and the runner prints a PASS/FAIL line per case before failing, so a red run
//! names every broken rule instead of only the first.

use queen::{Broker, BrokerConfig};
use serde_json::{json, Value};
use tokio_postgres::Client;

/// `config::DEFAULT_TENANT`. The SP takes the tenant as an argument and never
/// reads it from the options bag, so every call here passes it explicitly.
const DEFAULT_TENANT: &str = "00000000-0000-0000-0000-000000000001";

/// The 21 options `/configure` accepts, spelled as the SP parses and echoes
/// them. The length is the point as much as the names: a 22nd option added to
/// 012_configure.sql without a line here fails `agrees` on every case.
const OPTION_KEYS: [&str; 21] = [
    "namespace",
    "task",
    "priority",
    "leaseTime",
    "retryLimit",
    "retryDelay",
    "maxSize",
    "ttl",
    "deadLetterQueue",
    "dlqAfterMaxRetries",
    "delayedProcessing",
    "windowBuffer",
    "retentionSeconds",
    "completedRetentionSeconds",
    "retentionEnabled",
    "encryptionEnabled",
    "maxWaitTimeSeconds",
    "minPopWaitTime",
    "dedupWindowSeconds",
    "retentionSinkHold",
    "retentionSinkHoldMaxSeconds",
];

/// The value each option lands on when the body does not carry it AND there is
/// nothing to keep — i.e. on create, and on `replace: true`. These are the
/// defaults written in 012_configure.sql, NOT the DDL defaults of
/// `queen.queues`: the two deliberately disagree on `leaseTime` (300 here, 60 in
/// the column, see the schema's comment), and this SP's number is the one a
/// configured queue gets. Same order as `OPTION_KEYS`.
fn defaults() -> Vec<(&'static str, Value)> {
    let values = [
        json!(""),      // namespace
        json!(""),      // task
        json!(0),       // priority
        json!(300),     // leaseTime
        json!(3),       // retryLimit
        json!(1000),    // retryDelay
        json!(0),       // maxSize
        json!(3600),    // ttl
        json!(true),    // deadLetterQueue
        json!(true),    // dlqAfterMaxRetries
        json!(0),       // delayedProcessing
        json!(0),       // windowBuffer
        json!(0),       // retentionSeconds
        json!(0),       // completedRetentionSeconds
        json!(false),   // retentionEnabled
        json!(false),   // encryptionEnabled
        json!(0),       // maxWaitTimeSeconds
        json!(0),       // minPopWaitTime
        json!(3600),    // dedupWindowSeconds
        json!(""),      // retentionSinkHold
        json!(604_800), // retentionSinkHoldMaxSeconds
    ];
    OPTION_KEYS.into_iter().zip(values).collect()
}

/// An options bag as a `(key, value)` list in `OPTION_KEYS` order, so it can be
/// handed to `agrees` and edited key by key.
fn as_expected(bag: &Value) -> Vec<(&'static str, Value)> {
    OPTION_KEYS
        .into_iter()
        .map(|k| (k, bag[k].clone()))
        .collect()
}

/// A non-default value for every option, so "did this key move?" is answerable
/// for all 21 at once. Every one is legal: the two sink-hold options are
/// validated by the SP and would otherwise fail the call.
fn all_set() -> Value {
    json!({
        "namespace": "billing",
        "task": "ingest",
        "priority": 7,
        "leaseTime": 60,
        "retryLimit": 9,
        "retryDelay": 250,
        "maxSize": 100_000,
        "ttl": 1200,
        "deadLetterQueue": false,
        "dlqAfterMaxRetries": false,
        "delayedProcessing": 30,
        "windowBuffer": 15,
        "retentionSeconds": 86_400,
        "completedRetentionSeconds": 3_600,
        "retentionEnabled": true,
        "encryptionEnabled": true,
        "maxWaitTimeSeconds": 120,
        "minPopWaitTime": 25,
        "dedupWindowSeconds": 900,
        "retentionSinkHold": "lake",
        "retentionSinkHoldMaxSeconds": 172_800
    })
}

type Case = Result<(), String>;

macro_rules! chk {
    ($cond:expr, $($arg:tt)*) => {
        if !($cond) { return Err(format!($($arg)*)); }
    };
}

/// One key of an options map, as a value or as a case FAILURE — never as a
/// panic. `serde_json::Map`'s own `Index` panics with "no entry found for key",
/// which aborts the run on the first missing key and prints no PASS/FAIL table
/// at all: the report this file exists to produce is the first casualty of the
/// bug it is trying to describe (an option dropped from the 21-key echo).
macro_rules! at {
    ($map:expr, $key:expr) => {
        $map.get($key).ok_or_else(|| {
            format!(
                "{} is missing from the {} keys returned ({:?})",
                $key,
                $map.len(),
                $map.keys().collect::<Vec<_>>()
            )
        })?
    };
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

async fn connect(host: &str, port: u16) -> Client {
    let (c, conn) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres password=postgres dbname=postgres"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

/// `tokio_postgres::Error`'s Display is "db error" and nothing else, which turns
/// a red suite into identical lines. Unwrap the SQLSTATE and message.
fn pg_err(e: tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{} {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

/// One `configure_queue_v1` call. `$2::text::jsonb` and `$3::text::uuid` are the
/// house idiom (the driver has no jsonb/uuid types here).
async fn configure(c: &Client, queue: &str, options: Value) -> Result<Value, String> {
    let row = c
        .query_one(
            "SELECT queen.configure_queue_v1($1, $2::text::jsonb, $3::text::uuid)::text",
            &[&queue, &options.to_string(), &DEFAULT_TENANT],
        )
        .await
        .map_err(pg_err)?;
    let txt: String = row.get(0);
    Ok(serde_json::from_str(&txt).unwrap_or(Value::Null))
}

/// The `options` object of a configure echo, with `namespace` and `task` — which
/// the echo carries at the top level — folded in, so one map answers for all 21.
fn echoed(v: &Value) -> Result<serde_json::Map<String, Value>, String> {
    let mut m = v
        .get("options")
        .and_then(|o| o.as_object())
        .cloned()
        .ok_or_else(|| format!("no options object in the echo: {v}"))?;
    for key in ["namespace", "task"] {
        let top = v
            .get(key)
            .cloned()
            .ok_or_else(|| format!("no top-level {key} in the echo: {v}"))?;
        m.insert(key.to_string(), top);
    }
    Ok(m)
}

/// The queue's 21 options READ BACK FROM THE COLUMNS, not from the echo. The
/// echo is built from the same variables the upsert wrote, so on its own it
/// cannot tell a kept value from a value that was written and then lost.
/// `queen.get_queue_v2`'s own `options` block is what the dashboard's editor
/// prefills from, so reading through it tests both halves of W2 at once.
async fn stored(c: &Client, queue: &str) -> Result<serde_json::Map<String, Value>, String> {
    let row = c
        .query_one(
            "SELECT queen.get_queue_v2($1, $2::text::uuid)::text",
            &[&queue, &DEFAULT_TENANT],
        )
        .await
        .map_err(pg_err)?;
    let txt: String = row.get(0);
    let v: Value = serde_json::from_str(&txt).unwrap_or(Value::Null);
    chk!(
        v.get("error").is_none(),
        "get_queue_v2({queue}) says {}",
        v["error"]
    );
    v.get("options")
        .and_then(|o| o.as_object())
        .cloned()
        .ok_or_else(|| format!("get_queue_v2 carries no options block: {v}"))
}

/// Every key of `expected` must equal the stored value, and the stored map must
/// carry all 21 keys and nothing else.
fn agrees(what: &str, got: &serde_json::Map<String, Value>, expected: &[(&str, Value)]) -> Case {
    for (k, want) in expected {
        let got_v = got
            .get(*k)
            .ok_or_else(|| format!("{what}: key {k} is missing"))?;
        chk!(got_v == want, "{what}: {k} is {got_v}, expected {want}");
    }
    chk!(
        got.len() == OPTION_KEYS.len(),
        "{what}: expected all {} options, got {} ({:?})",
        OPTION_KEYS.len(),
        got.len(),
        got.keys().collect::<Vec<_>>()
    );
    Ok(())
}

/// `expected` = the defaults, with the listed keys overridden.
fn with(over: &[(&str, Value)]) -> Vec<(&'static str, Value)> {
    let mut out = defaults();
    for (k, v) in over {
        let slot = out
            .iter_mut()
            .find(|(name, _)| name == k)
            .unwrap_or_else(|| panic!("{k} is not one of the 21 options"));
        slot.1 = v.clone();
    }
    out
}

// ===========================================================================
// CREATE — the path that did not change: a body that mentions three options
// gets today's defaults for the other eighteen, because there is nothing to
// keep. This is also what pins the default VALUES, which every "keep" case
// below is measured against.
// ===========================================================================
async fn case_create_takes_todays_defaults(c: &Client) -> Case {
    let q = unique("cfg-create");
    let echo = configure(c, &q, json!({ "leaseTime": 60, "retryLimit": 9 })).await?;
    chk!(
        echo["configured"] == json!(true),
        "create did not report configured: {echo}"
    );
    let want = with(&[("leaseTime", json!(60)), ("retryLimit", json!(9))]);
    agrees("create echo", &echoed(&echo)?, &want)?;
    agrees("create stored", &stored(c, &q).await?, &want)
}

// ===========================================================================
// THE FIX ITSELF — `queenctl queue configure orders --lease-time 60` used to
// reset the other twenty columns. A second partial body must move exactly the
// key it names.
// ===========================================================================
async fn case_partial_edit_keeps_the_other_columns(c: &Client) -> Case {
    let q = unique("cfg-merge");
    configure(c, &q, all_set()).await?;
    let echo = configure(c, &q, json!({ "leaseTime": 45 })).await?;

    let mut want = as_expected(&all_set());
    want.iter_mut().find(|(k, _)| *k == "leaseTime").unwrap().1 = json!(45);

    agrees("merge echo", &echoed(&echo)?, &want)?;
    agrees("merge stored", &stored(c, &q).await?, &want)
}

// ===========================================================================
// An explicit `null` is how a caller asks for the DEFAULT back without knowing
// what it is — and it is the only way, since absent now means keep. (It is
// also, unavoidably, what a client that serialises an unset field as null
// sends; the two are the same request and mean the same thing.)
// ===========================================================================
async fn case_explicit_null_restores_the_default(c: &Client) -> Case {
    let q = unique("cfg-null");
    configure(c, &q, all_set()).await?;
    let echo = configure(
        c,
        &q,
        json!({ "leaseTime": null, "dedupWindowSeconds": null, "retentionSinkHold": null }),
    )
    .await?;

    let got = echoed(&echo)?;
    for (k, want) in [
        ("leaseTime", json!(300)),
        ("dedupWindowSeconds", json!(3600)),
        ("retentionSinkHold", json!("")),
    ] {
        chk!(
            at!(got, k) == &want,
            "null {k} restored {}, expected {want}",
            at!(got, k)
        );
    }
    // ...and the keys it did NOT mention are still the configured ones.
    chk!(
        at!(got, "retryLimit") == &json!(9) && at!(got, "encryptionEnabled") == &json!(true),
        "a null for three options touched the other eighteen: {echo}"
    );
    let stored = stored(c, &q).await?;
    chk!(
        at!(stored, "leaseTime") == &json!(300) && at!(stored, "retryLimit") == &json!(9),
        "the columns disagree with the echo: {stored:?}"
    );
    Ok(())
}

// ===========================================================================
// `replace: true` is the old behaviour, kept for the declarative callers: a
// manifest means the WHOLE configuration, so what it omits goes back to the
// default. `queenctl apply -f` sends it.
// ===========================================================================
async fn case_replace_resets_everything_not_given(c: &Client) -> Case {
    let q = unique("cfg-replace");
    configure(c, &q, all_set()).await?;
    let echo = configure(c, &q, json!({ "replace": true, "retryLimit": 7 })).await?;
    let want = with(&[("retryLimit", json!(7))]);
    agrees("replace echo", &echoed(&echo)?, &want)?;
    agrees("replace stored", &stored(c, &q).await?, &want)?;

    // `replace: false` is the default spelled out, not a third mode.
    configure(c, &q, all_set()).await?;
    let echo = configure(c, &q, json!({ "replace": false, "retryLimit": 7 })).await?;
    let got = echoed(&echo)?;
    chk!(
        at!(got, "leaseTime") == &json!(60) && at!(got, "retryLimit") == &json!(7),
        "replace:false must merge: {echo}"
    );
    Ok(())
}

// ===========================================================================
// namespace and task follow the same rule as the other nineteen. They are the
// two that do NOT live in the options object of the echo (they are top-level,
// and have been since before this SP had an options echo), which is exactly why
// they are easy to leave behind.
// ===========================================================================
async fn case_namespace_and_task_follow_the_rule(c: &Client) -> Case {
    let q = unique("cfg-ns");
    configure(c, &q, json!({ "namespace": "billing", "task": "ingest" })).await?;

    let keep = configure(c, &q, json!({ "leaseTime": 45 })).await?;
    chk!(
        keep["namespace"] == json!("billing") && keep["task"] == json!("ingest"),
        "a partial edit dropped the namespace/task: {keep}"
    );
    let stored_keep = stored(c, &q).await?;
    chk!(
        at!(stored_keep, "namespace") == &json!("billing")
            && at!(stored_keep, "task") == &json!("ingest"),
        "the columns lost the namespace/task: {stored_keep:?}"
    );

    let cleared = configure(c, &q, json!({ "namespace": null })).await?;
    chk!(
        cleared["namespace"] == json!("") && cleared["task"] == json!("ingest"),
        "an explicit null must clear the namespace and only it: {cleared}"
    );

    let replaced = configure(c, &q, json!({ "replace": true })).await?;
    chk!(
        replaced["task"] == json!(""),
        "replace must reset the task like every other option: {replaced}"
    );
    Ok(())
}

// ===========================================================================
// A queue created by a PUSH has never been through this SP, and what it holds
// is NOT blank: every implicit-create path derives the two discovery labels from
// the dotted queue name (003_log_push.sql:108, and the same INSERT in
// 004_log_pop, 005_log_ack, 025_log_timers), so `orders.created` arrives here
// namespaced `orders` / `created`. Until 1.6.0 the first /configure that did not
// resend them reset both to '' and the queue silently stopped matching
// `/pop?namespace=orders&task=created`. That is the reachable consequence of
// this merge and it is what this case pins — seeded exactly as the push path
// seeds it, because a hand-written row is not a shape the broker can produce.
// ===========================================================================
async fn case_a_push_created_queue_keeps_its_derived_labels(c: &Client) -> Case {
    let q = format!("{}.created", unique("cfg-push-made"));
    // 003_log_push.sql's provisioning INSERT, expression for expression. The
    // only difference is the explicit ::text casts: there the queue name is
    // already a TEXT parameter of the SP, here it is an untyped placeholder used
    // in three positions at once, which Postgres refuses to deduce (42P08).
    c.execute(
        "INSERT INTO queen.queues (tenant_id, name, namespace, task)
         VALUES ($1::text::uuid, $2::text,
                 split_part($2::text, '.', 1),
                 CASE WHEN position('.' in $2::text) > 0
                      THEN split_part($2::text, '.', 2) ELSE '' END)",
        &[&DEFAULT_TENANT, &q],
    )
    .await
    .map_err(pg_err)?;

    let echo = configure(c, &q, json!({ "retryLimit": 4 })).await?;
    let (ns, task) = (q.split('.').next().unwrap().to_string(), "created");
    chk!(
        echo["namespace"] == json!(ns) && echo["task"] == json!(task),
        "a partial configure reset the labels the push derived: {echo}"
    );
    let got = echoed(&echo)?;
    // The DDL defaults, kept — NOT this SP's parse defaults. leaseTime is the
    // one where the two disagree (60 in the column, 300 here), which makes it
    // the case that can tell "kept" from "reset".
    chk!(
        at!(got, "leaseTime") == &json!(60),
        "merge must keep the column's 60, not write this SP's 300: {echo}"
    );
    chk!(
        at!(got, "retryLimit") == &json!(4),
        "the edit itself did not land: {echo}"
    );

    // ...and `replace` is still the way back to the old behaviour, labels
    // included: a manifest that does not name a namespace means "no namespace".
    let replaced = configure(c, &q, json!({ "replace": true })).await?;
    chk!(
        replaced["namespace"] == json!("") && replaced["task"] == json!(""),
        "replace must reset the derived labels like every other option: {replaced}"
    );
    Ok(())
}

// ===========================================================================
// The other shape of an unconfigured queue: NULL labels, which only a database
// written before that derivation existed (or a hand-written row) can hold. The
// columns are the only two nullable ones in the set, and the echo has only ever
// carried strings, so a kept NULL must come back as '' rather than as null.
// ===========================================================================
async fn case_a_legacy_null_label_is_kept_as_empty_string(c: &Client) -> Case {
    let q = unique("cfg-null-labels");
    c.execute(
        "INSERT INTO queen.queues (tenant_id, name) VALUES ($1::text::uuid, $2)",
        &[&DEFAULT_TENANT, &q],
    )
    .await
    .map_err(pg_err)?;

    let echo = configure(c, &q, json!({ "retryLimit": 4 })).await?;
    chk!(
        echo["namespace"] == json!("") && echo["task"] == json!(""),
        "a NULL namespace must echo as \"\", not null: {echo}"
    );
    let stored = stored(c, &q).await?;
    chk!(
        at!(stored, "namespace") == &json!("") && at!(stored, "task") == &json!(""),
        "the columns kept a NULL where every reader expects a string: {stored:?}"
    );
    Ok(())
}

// ===========================================================================
// TWO CONCURRENT MERGES of one queue, each naming a different key. Without the
// SELECT ... FOR UPDATE the second caller would merge onto the row it read
// before the first one committed, and one of the two edits would vanish with no
// error anywhere. This is the case the lock exists for.
//
// The race is FORCED rather than hoped for: a third connection holds the row's
// lock while both merges start, so both are guaranteed to be inside the SP and
// past its entry when the lock is released. With the lock on the SELECT they
// serialise there and the second one reads the first one's result; with the read
// unlocked they would both read the pre-image and block later, on the upsert,
// where the last writer silently wins. That is the difference this case has to
// be able to see, every run, and not one run in fifty.
// ===========================================================================
async fn case_concurrent_merges_both_land(host: &str, port: u16) -> Case {
    let q = unique("cfg-race");
    let c = connect(host, port).await;
    configure(&c, &q, all_set()).await?;

    let blocker = connect(host, port).await;
    blocker.batch_execute("BEGIN").await.map_err(pg_err)?;
    blocker
        .query(
            "SELECT 1 FROM queen.queues WHERE tenant_id = $1::text::uuid AND name = $2 FOR UPDATE",
            &[&DEFAULT_TENANT, &q],
        )
        .await
        .map_err(pg_err)?;

    let mut tasks = Vec::new();
    for (key, value) in [("leaseTime", json!(11)), ("retryLimit", json!(22))] {
        let (q, host) = (q.clone(), host.to_string());
        tasks.push(tokio::spawn(async move {
            let c = connect(&host, port).await;
            configure(&c, &q, json!({ key: value })).await
        }));
    }
    // Long enough for both to have connected and reached the lock; the assertion
    // does not depend on the duration, only on both being underway before the
    // blocker lets go.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    blocker.batch_execute("COMMIT").await.map_err(pg_err)?;

    for t in tasks {
        t.await.map_err(|e| format!("join: {e}"))??;
    }

    let got = stored(&c, &q).await?;
    chk!(
        at!(got, "leaseTime") == &json!(11) && at!(got, "retryLimit") == &json!(22),
        "one of two concurrent merges was lost: leaseTime={} retryLimit={}",
        at!(got, "leaseTime"),
        at!(got, "retryLimit")
    );
    // And neither of them dragged the rest of the configuration back to the
    // defaults on its way through.
    chk!(
        at!(got, "dedupWindowSeconds") == &json!(900)
            && at!(got, "retentionSinkHold") == &json!("lake"),
        "a concurrent merge reset an option it never mentioned: {got:?}"
    );
    Ok(())
}

// ===========================================================================
// The echo is the EFFECTIVE row — all 21 options, whatever the body carried —
// and `replace` is a directive, so it must not come back looking like a 22nd
// option a caller could store.
// ===========================================================================
async fn case_the_echo_is_the_effective_row(c: &Client) -> Case {
    let q = unique("cfg-echo");
    let echo = configure(c, &q, json!({ "replace": true, "leaseTime": 60 })).await?;
    let opts = echo["options"]
        .as_object()
        .ok_or_else(|| format!("no options object: {echo}"))?;
    chk!(
        !opts.contains_key("replace"),
        "`replace` leaked into the options echo: {echo}"
    );
    chk!(
        echo.get("replace").is_none(),
        "`replace` leaked into the echo's top level: {echo}"
    );
    chk!(
        echo["queue"] == json!(q) && echo["storage"] == json!("segments"),
        "the echo lost a wire-compat key: {echo}"
    );
    agrees("echo", &echoed(&echo)?, &with(&[("leaseTime", json!(60))]))
}

// ===========================================================================
// A REFUSED body must leave the configuration alone. The two sink-hold options
// are rejected out of range rather than clamped (012_configure.sql explains
// why), and the validation runs before the upsert — so the queue keeps what it
// had, and the merge cannot be used to half-apply a body.
// ===========================================================================
async fn case_a_refused_body_changes_nothing(c: &Client) -> Case {
    let q = unique("cfg-refused");
    configure(c, &q, all_set()).await?;
    let refused = configure(
        c,
        &q,
        json!({ "leaseTime": 5, "retentionSinkHoldMaxSeconds": 1 }),
    )
    .await?;
    chk!(
        refused["error"].as_str().is_some_and(|e| e.contains("retentionSinkHoldMaxSeconds")),
        "expected the out-of-range refusal, got {refused}"
    );
    let got = stored(c, &q).await?;
    chk!(
        at!(got, "leaseTime") == &json!(60)
            && at!(got, "retentionSinkHoldMaxSeconds") == &json!(172_800),
        "a refused body still moved the queue: {got:?}"
    );
    Ok(())
}

// ===========================================================================
// Tenant scoping of the merge: the row a merge keeps must be THIS tenant's. A
// same-named queue in another tenant is a different queue, and a body that
// mentions nothing must create it with the defaults rather than inherit the
// neighbour's configuration.
// ===========================================================================
const OTHER_TENANT: &str = "00000000-0000-0000-0000-0000000000ff";

async fn case_merge_is_tenant_scoped(c: &Client) -> Case {
    let q = unique("cfg-tenant");
    configure(c, &q, all_set()).await?;

    let row = c
        .query_one(
            "SELECT queen.configure_queue_v1($1, $2::text::jsonb, $3::text::uuid)::text",
            &[&q, &json!({ "retryLimit": 2 }).to_string(), &OTHER_TENANT],
        )
        .await
        .map_err(pg_err)?;
    let echo: Value = serde_json::from_str(&row.get::<_, String>(0)).unwrap_or(Value::Null);
    let got = echoed(&echo)?;
    chk!(
        at!(got, "leaseTime") == &json!(300) && at!(got, "dedupWindowSeconds") == &json!(3600),
        "the other tenant's create inherited this tenant's options: {echo}"
    );
    chk!(
        at!(got, "retryLimit") == &json!(2),
        "the create itself did not land: {echo}"
    );

    // And the original is untouched.
    let mine = stored(c, &q).await?;
    chk!(
        at!(mine, "leaseTime") == &json!(60),
        "the other tenant's configure reached into this one: {mine:?}"
    );
    Ok(())
}

// ===========================================================================
// THE HANDLER'S HALF, driven through the embedded broker — which is
// `handle_configure` with the socket taken out, the same function the HTTP
// route calls. `mode` decides nothing by itself: it decides only through the
// `replace` key the handler puts in the options bag, and that one insertion is
// the whole wiring. Deleting it used to leave the entire suite green while
// `mode:"replace"` silently merged, i.e. while `queenctl apply -f` stopped
// being declarative and the product lost every reset path it has.
// ===========================================================================
async fn case_the_handler_wires_mode_to_replace(broker: &Broker, c: &Client) -> Case {
    use queen::protocol::{ConfigureRequest, QueueOptions};

    let q = unique("cfg-handler");
    configure(c, &q, all_set()).await?;

    // No `mode` — the shape every released SDK sends. It must merge.
    let merged = ConfigureRequest::new(q.clone()).options(QueueOptions {
        lease_time: Some(45),
        ..Default::default()
    });
    broker
        .configure(&merged)
        .await
        .map_err(|e| format!("a modeless configure failed: {e}"))?;
    let got = stored(c, &q).await?;
    chk!(
        at!(got, "leaseTime") == &json!(45) && at!(got, "dedupWindowSeconds") == &json!(900),
        "a modeless request through the handler must merge: {got:?}"
    );

    // `mode: "replace"` — the declarative caller's request, which must reach the
    // SP as the flag that re-parses all 21 options from defaults.
    let replaced = ConfigureRequest::new(q.clone())
        .options(QueueOptions {
            retry_limit: Some(7),
            ..Default::default()
        })
        .replace(true);
    broker
        .configure(&replaced)
        .await
        .map_err(|e| format!("a replacing configure failed: {e}"))?;
    agrees(
        "replace through the handler",
        &stored(c, &q).await?,
        &with(&[("retryLimit", json!(7))]),
    )
}

// ===========================================================================
// The two refusals the handler owns, and the status each one arrives on. An
// unknown `mode` is a 400 that writes NOTHING — doing the other thing to a
// queue's whole configuration is the damage this feature exists to prevent —
// and the SP's own option refusals now carry `invalid`, which the handler turns
// into a 400 as well: they are a mistyped body, not a broken broker, and a 500
// reaches an operator as an outage (and a cloud proxy as an unbilled 5xx).
// ===========================================================================
async fn case_the_handler_refuses_on_the_right_status(broker: &Broker, c: &Client) -> Case {
    use queen::protocol::{ConfigureRequest, QueueOptions};

    let q = unique("cfg-handler-refuse");
    configure(c, &q, all_set()).await?;

    let mut bad_mode = ConfigureRequest::new(q.clone()).options(QueueOptions {
        retry_limit: Some(1),
        ..Default::default()
    });
    bad_mode.mode = Some("patch".to_string());
    match broker.configure(&bad_mode).await {
        Ok(v) => return Err(format!("mode \"patch\" was accepted: {v:?}")),
        Err(e) => chk!(
            e.status() == Some(400) && format!("{e}").contains("\"merge\" or \"replace\""),
            "an unknown mode must be a 400 naming the two spellings, got {e}"
        ),
    }
    let after_mode = stored(c, &q).await?;
    chk!(
        at!(after_mode, "retryLimit") == &json!(9),
        "a refused mode still configured the queue"
    );

    let bad_option = ConfigureRequest::new(q.clone()).options(QueueOptions {
        retention_sink_hold_max_seconds: Some(1),
        ..Default::default()
    });
    match broker.configure(&bad_option).await {
        Ok(v) => {
            return Err(format!(
                "an out-of-range sink hold ceiling was accepted: {v:?}"
            ))
        }
        Err(e) => chk!(
            e.status() == Some(400) && format!("{e}").contains("retentionSinkHoldMaxSeconds"),
            "the SP's option refusal must arrive as a 400 naming the option, got {e}"
        ),
    }
    let after_option = stored(c, &q).await?;
    chk!(
        at!(after_option, "retentionSinkHoldMaxSeconds") == &json!(172_800),
        "a refused option still moved the queue"
    );
    Ok(())
}

// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn configure_merge_semantics() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // Boot the real broker purely to apply the real schema — the SQL under test
    // is include_str!-embedded, so an edit to 012_configure.sql that was never
    // rebuilt would be invisible to a test that applied the file itself.
    let broker = Broker::start(
        BrokerConfig::new()
            .pg(host.clone(), port, "postgres", "postgres", "postgres")
            .pool_size(4),
    )
    .await
    .expect("broker start");

    let c = connect(&host, port).await;

    let mut report: Vec<(&str, Case)> = Vec::new();
    report.push(("create_takes_todays_defaults", case_create_takes_todays_defaults(&c).await));
    report.push(("partial_edit_keeps_the_other_columns", case_partial_edit_keeps_the_other_columns(&c).await));
    report.push(("explicit_null_restores_the_default", case_explicit_null_restores_the_default(&c).await));
    report.push(("replace_resets_everything_not_given", case_replace_resets_everything_not_given(&c).await));
    report.push(("namespace_and_task_follow_the_rule", case_namespace_and_task_follow_the_rule(&c).await));
    report.push(("a_push_created_queue_keeps_its_derived_labels", case_a_push_created_queue_keeps_its_derived_labels(&c).await));
    report.push(("a_legacy_null_label_is_kept_as_empty_string", case_a_legacy_null_label_is_kept_as_empty_string(&c).await));
    report.push(("concurrent_merges_both_land", case_concurrent_merges_both_land(&host, port).await));
    report.push(("the_echo_is_the_effective_row", case_the_echo_is_the_effective_row(&c).await));
    report.push(("a_refused_body_changes_nothing", case_a_refused_body_changes_nothing(&c).await));
    report.push(("merge_is_tenant_scoped", case_merge_is_tenant_scoped(&c).await));
    report.push(("the_handler_wires_mode_to_replace", case_the_handler_wires_mode_to_replace(&broker, &c).await));
    report.push(("the_handler_refuses_on_the_right_status", case_the_handler_refuses_on_the_right_status(&broker, &c).await));

    println!("\n============ configure merge semantics (PLAN_DASHBOARD_ACTIONS §2.2) ============");
    let mut failed = 0;
    for (name, r) in &report {
        match r {
            Ok(()) => println!("PASS  {name}"),
            Err(e) => {
                failed += 1;
                println!("FAIL  {name}\n        {e}");
            }
        }
    }
    println!("=========================== {}/{} passed ===========================\n",
             report.len() - failed, report.len());
    assert_eq!(failed, 0, "{failed} configure merge case(s) failed — see the table above");
}
