//! KV SEMANTICS — the contract of `PLAN_KV_TIMERS.md` §5, tested against a real
//! Postgres. Closes the "Semantica KV" row of §15 (phase F2).
//!
//! WHY THIS LEVEL. Every rule under test lives in `queen.kv_apply_v1`, and that
//! is deliberate (§9.2): the shape rules are written in SQL exactly once so all
//! seven clients, the HTTP routes, the transaction wire and the embedded broker
//! — which never passes through an HTTP handler — inherit them without a
//! per-language copy. A test that went through HTTP would prove the rule for one
//! of those four surfaces and prove nothing about the other three. So the tests
//! call the SP directly, on the same connection kind the broker uses.
//!
//! WHY `p_now` IS A PARAMETER AND WHY THAT MATTERS HERE. §5.7 says an expired
//! row is never returned and never counts as existing, *even before the sweeper
//! prunes it*, and §6.1 makes `now` a parameter of every helper so a single call
//! sees a single instant. That is what lets the expiry cases below run with zero
//! sleeps: they write at `now()` and read at `now() + interval '10 seconds'`.
//! The physical row is still there — several cases assert exactly that — so what
//! is under test is the predicate, not the sweeper.
//!
//! WHAT IS PINNED, AND WHAT IS NOT. The plan pins the operation names (§5,
//! seven of them), the field names (§5.5 tables, §5.1 `ttlSeconds`/`forever`,
//! §5.3 `expect`, §5.4 `delta`/`min`/`max`), the closed `reason` taxonomy
//! (`exists`, `absent`, `version`, `limit`, `type`) and the SQLSTATEs. It does
//! NOT pin whether `kv_apply_v1` returns a bare JSON array of per-op results or
//! an object wrapping one; §6.4 only says the results are index-aligned to the
//! input array. `results()` below therefore accepts both shapes, and NOTHING
//! else in this file is loose.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`,
//! same convention as `embedded_smoke` and `hotlist_reseed_window`:
//!
//! ```bash
//! docker run --rm -d --name queen-kv-pg -e POSTGRES_PASSWORD=postgres -p 5466:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5466 cargo test --test kv_semantics -- --ignored --nocapture
//! ```
//!
//! ONE test function on purpose (the admission arbiter is process-global and a
//! Broker is booted to apply the real, `include_str!`-embedded schema). The
//! cases are still reported one by one: each returns `Result<(), String>` and
//! the runner prints a PASS/FAIL line per case before failing, so a red run
//! names every broken rule instead of only the first.

use queen::{Broker, BrokerConfig};
use serde_json::{json, Value};
use tokio_postgres::Client;

/// `config::DEFAULT_TENANT`. `queen.kv.tenant_id` defaults to it (§3.2) but the
/// SP takes it as an argument and never reads it from an op (§6.1 point 6), so
/// every call here passes it explicitly.
const DEFAULT_TENANT: &str = "00000000-0000-0000-0000-000000000001";

/// §5.5: `limit` default 100, clamped to `QUEEN_KV_PREFIX_LIMIT`, never rejected.
const PREFIX_LIMIT_DEFAULT: usize = 100;
const PREFIX_LIMIT_CAP: usize = 1000;
/// §5.5: `QUEEN_KV_MAX_READ_BYTES`, applied to the aggregate INSIDE the SP.
const MAX_READ_BYTES: usize = 4 * 1024 * 1024;
/// §9.2: `QUEEN_KV_MAX_VALUE_BYTES`. Only used as the tolerance for the row that
/// straddles the byte cap.
const MAX_VALUE_BYTES: usize = 65536;

type Case = Result<(), String>;

macro_rules! chk {
    ($cond:expr, $($arg:tt)*) => {
        if !($cond) { return Err(format!($($arg)*)); }
    };
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}{nanos}")
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

/// One `kv_apply_v1` call. `now_expr` is interpolated (not bound) because it is
/// a SQL expression — `now()`, or `now() + interval '10 seconds'` for the
/// expiry cases. Test-only input, no injection surface.
async fn apply_raw(
    c: &Client,
    now_expr: &str,
    in_wire: bool,
    ops: Value,
) -> Result<Value, tokio_postgres::Error> {
    // Explicit casts on every bind: `$1::text::jsonb` and `$2::text::uuid` are the
    // house idiom (the driver has no uuid/jsonb types here), and `$3::boolean`
    // keeps overload resolution unambiguous.
    let sql = format!(
        "SELECT queen.kv_apply_v1($1::text::jsonb, $2::text::uuid, {now_expr}, $3::boolean)::text"
    );
    let row = c
        .query_one(sql.as_str(), &[&ops.to_string(), &DEFAULT_TENANT, &in_wire])
        .await?;
    let txt: String = row.get(0);
    Ok(serde_json::from_str(&txt).unwrap_or(Value::Null))
}

/// `tokio_postgres::Error`'s Display is "db error" and nothing else, which turns
/// a red suite into twelve identical lines. Unwrap the SQLSTATE and message.
fn pg_err(e: tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!(
            "{} {}{}",
            db.code().code(),
            db.message(),
            db.detail().map(|d| format!(" | DETAIL: {d}")).unwrap_or_default()
        ),
        None => format!("{e}"),
    }
}

/// §6.4: results are index-aligned to the input array. The wrapper shape is not
/// pinned by the plan, so accept a bare array or `{"results":[...]}`.
fn results(v: &Value) -> Result<Vec<Value>, String> {
    if let Some(a) = v.as_array() {
        return Ok(a.clone());
    }
    if let Some(a) = v.get("results").and_then(|r| r.as_array()) {
        return Ok(a.clone());
    }
    Err(format!("kv_apply_v1 returned a non index-aligned shape: {v}"))
}

async fn apply(c: &Client, ops: Value) -> Result<Vec<Value>, String> {
    let n = ops.as_array().map(|a| a.len()).unwrap_or(0);
    let v = apply_raw(c, "now()", false, ops)
        .await
        .map_err(pg_err)?;
    let r = results(&v)?;
    if r.len() != n {
        return Err(format!("expected {n} index-aligned results, got {}", r.len()));
    }
    Ok(r)
}

async fn apply_at(c: &Client, now_expr: &str, ops: Value) -> Result<Vec<Value>, String> {
    let n = ops.as_array().map(|a| a.len()).unwrap_or(0);
    let v = apply_raw(c, now_expr, false, ops)
        .await
        .map_err(pg_err)?;
    let r = results(&v)?;
    if r.len() != n {
        return Err(format!("expected {n} index-aligned results, got {}", r.len()));
    }
    Ok(r)
}

async fn one(c: &Client, op: Value) -> Result<Value, String> {
    Ok(apply(c, json!([op])).await?[0].clone())
}

async fn one_at(c: &Client, now_expr: &str, op: Value) -> Result<Value, String> {
    Ok(apply_at(c, now_expr, json!([op])).await?[0].clone())
}

/// The call is expected to RAISE. Returns the SQLSTATE + message + detail.
async fn expect_raise(
    c: &Client,
    now_expr: &str,
    in_wire: bool,
    ops: Value,
) -> Result<(String, String, String), String> {
    match apply_raw(c, now_expr, in_wire, ops).await {
        Ok(v) => Err(format!("expected a RAISE, got {v}")),
        Err(e) => {
            let db = e
                .as_db_error()
                .ok_or_else(|| format!("expected a database error, got {e}"))?;
            Ok((
                db.code().code().to_string(),
                db.message().to_string(),
                db.detail().unwrap_or("").to_string(),
            ))
        }
    }
}

async fn row_count(c: &Client, ns: &str, key: &str) -> Result<i64, String> {
    c.query_one(
        "SELECT count(*) FROM queen.kv \
         WHERE tenant_id = $1::text::uuid AND namespace = $2 AND key = $3",
        &[&DEFAULT_TENANT, &ns, &key],
    )
    .await
    .map(|r| r.get::<_, i64>(0))
    .map_err(|e| format!("row_count: {e}"))
}

/// A physical column, read straight from the table — used only where the point
/// of the case is that the SP did (or did not) touch a column the API hides.
async fn col_text(c: &Client, ns: &str, key: &str, col: &str) -> Result<Option<String>, String> {
    let sql = format!(
        "SELECT {col}::text FROM queen.kv \
         WHERE tenant_id = $1::text::uuid AND namespace = $2 AND key = $3"
    );
    let rows = c
        .query(sql.as_str(), &[&DEFAULT_TENANT, &ns, &key])
        .await
        .map_err(|e| format!("col_text({col}): {e}"))?;
    Ok(rows.first().and_then(|r| r.get::<_, Option<String>>(0)))
}

fn applied(r: &Value) -> bool {
    r.get("applied") == Some(&Value::Bool(true))
}
fn reason(r: &Value) -> String {
    r.get("reason")
        .and_then(|x| x.as_str())
        .unwrap_or("<none>")
        .to_string()
}
fn version(r: &Value) -> Option<i64> {
    r.get("version").and_then(|x| x.as_i64())
}
fn keys_of(rows: &Value) -> Vec<String> {
    rows.as_array()
        .map(|a| {
            a.iter()
                .map(|r| {
                    r.get("key")
                        .and_then(|k| k.as_str())
                        .unwrap_or("<nokey>")
                        .to_string()
                })
                .collect()
        })
        .unwrap_or_default()
}

// ===========================================================================
// §5.3 — exactly one winner among N concurrent putIfAbsent, and the LOSER gets
// the winner's value AND version back (that is the whole point of the marker:
// the loser must not need a second round trip).
// ===========================================================================
async fn case_one_winner(host: &str, port: u16) -> Case {
    const N: usize = 16;
    let ns = unique("kvrace");
    let key = "order-9f1";

    let mut tasks = Vec::new();
    for i in 0..N {
        let (ns, host) = (ns.clone(), host.to_string());
        tasks.push(tokio::spawn(async move {
            let c = connect(&host, port).await;
            apply_raw(
                &c,
                "now()",
                false,
                json!([{ "op": "putIfAbsent", "ns": ns, "key": key,
                         "value": {"holder": i}, "ttlSeconds": 60 }]),
            )
            .await
            .map_err(pg_err)
        }));
    }

    let mut out = Vec::new();
    for t in tasks {
        let r = t.await.map_err(|e| format!("join: {e}"))?;
        out.push(results(&r?)?[0].clone());
    }

    let winners: Vec<&Value> = out.iter().filter(|r| applied(r)).collect();
    chk!(
        winners.len() == 1,
        "expected exactly 1 winner out of {N}, got {}: {out:?}",
        winners.len()
    );
    let w = winners[0].clone();
    let wv = version(&w).ok_or("winner carries no version")?;
    let wval = w.get("value").cloned().unwrap_or(Value::Null);
    chk!(
        wval != Value::Null,
        "the winner must return its own value, got {w}"
    );

    for r in out.iter().filter(|r| !applied(r)) {
        chk!(
            reason(r) == "exists",
            "loser reason must be 'exists' (closed taxonomy §5.3), got '{}' in {r}",
            reason(r)
        );
        chk!(
            r.get("value") == Some(&wval),
            "loser must carry the WINNER's value (§5.3, no second round trip); \
             winner={wval}, loser={r}"
        );
        chk!(
            version(r) == Some(wv),
            "loser must carry the winner's CURRENT version {wv} (§5.3: discriminated \
             in the same statement, never a later snapshot), got {r}"
        );
    }

    // And exactly one physical row, holding the winner's value.
    let c = connect(host, port).await;
    chk!(row_count(&c, &ns, key).await? == 1, "expected exactly 1 row");
    Ok(())
}

// ===========================================================================
// §5.3 — THE MOST SERIOUS BUG REPAIRED: `expect: N > 0` on an absent key must
// take the pure-UPDATE branch, touch zero rows, and CREATE NOTHING. The naive
// ON CONFLICT form falls into the INSERT branch (the conflict arm never runs)
// and creates the row — in a saga that fires the compensation `expect` existed
// to prevent.
// ===========================================================================
async fn case_expect_positive_never_creates(c: &Client) -> Case {
    let ns = unique("kvexp");

    let r = one(
        c,
        json!({ "op": "put", "ns": ns, "key": "ghost", "value": {"x": 1},
                "ttlSeconds": 60, "expect": 90101 }),
    )
    .await?;
    chk!(!applied(&r), "expect:N>0 on an absent key must NOT apply: {r}");
    chk!(
        reason(&r) == "absent",
        "expected reason 'absent', got '{}': {r}",
        reason(&r)
    );
    chk!(
        row_count(c, &ns, "ghost").await? == 0,
        "expect:N>0 on an absent key CREATED THE ROW — §5.3, the pure-UPDATE branch"
    );
    chk!(
        version(&r).unwrap_or(0) == 0,
        "an absent key's effective version is 0 (kv_ver_v1 is total, §6.1), got {r}"
    );

    // Same rule for delete: a lost precondition creates nothing and is a verdict.
    let d = one(
        c,
        json!({ "op": "delete", "ns": ns, "key": "ghost2", "expect": 90101 }),
    )
    .await?;
    chk!(!applied(&d), "delete expect:N>0 on an absent key must not apply: {d}");
    chk!(
        reason(&d) == "absent",
        "expected reason 'absent' on delete, got '{}': {d}",
        reason(&d)
    );
    chk!(
        row_count(c, &ns, "ghost2").await? == 0,
        "delete with expect created a row"
    );
    Ok(())
}

// ===========================================================================
// §5.7 — an expired row is never returned and never counts as existing, EVEN
// BEFORE THE SWEEPER PRUNES IT. Written at now(), read at now()+10s, and the
// physical row asserted still present, so what is under test is the predicate.
// Includes the resurrection arm of §5.3 (`expect:0` wins against an expired row
// and resets `created_at`).
// ===========================================================================
async fn case_expired_reads_as_absent(c: &Client) -> Case {
    let ns = unique("kvexpiry");
    let future = "now() + interval '10 seconds'";

    let w = one(
        c,
        json!({ "op": "put", "ns": ns, "key": "k1", "value": {"v": 1}, "ttlSeconds": 1 }),
    )
    .await?;
    chk!(applied(&w), "seed put must apply: {w}");
    let v0 = version(&w).ok_or("seed put carries no version")?;
    let created0 = col_text(c, &ns, "k1", "created_at")
        .await?
        .ok_or("seed row missing")?;

    let g = one_at(c, future, json!({ "op": "get", "ns": ns, "key": "k1" })).await?;
    chk!(
        g.get("found") == Some(&Value::Bool(false)),
        "an expired key must read as NOT FOUND (§5.7): {g}"
    );
    chk!(
        row_count(c, &ns, "k1").await? == 1,
        "the physical row must still be there — this case is about the predicate, \
         not the sweeper"
    );

    let m = one_at(
        c,
        future,
        json!({ "op": "getMany", "ns": ns, "keys": ["k1", "never-written"] }),
    )
    .await?;
    chk!(
        keys_of(&m["rows"]).is_empty(),
        "getMany must not return an expired key: {m}"
    );
    let missing: Vec<String> = m["missing"]
        .as_array()
        .map(|a| a.iter().filter_map(|k| k.as_str().map(String::from)).collect())
        .unwrap_or_default();
    chk!(
        missing.contains(&"k1".to_string()) && missing.contains(&"never-written".to_string()),
        "absence must be a DATUM (§5.5): expected both keys in `missing`, got {m}"
    );

    let p = one_at(
        c,
        future,
        json!({ "op": "getPrefix", "ns": ns, "prefix": "k" }),
    )
    .await?;
    chk!(
        keys_of(&p["rows"]).is_empty(),
        "getPrefix must not return an expired key: {p}"
    );

    // Resurrection: expect:0 means "must not exist", and an expired row does not
    // exist, so the write wins and starts a NEW lineage (created_at reset).
    let r = one_at(
        c,
        future,
        json!({ "op": "putIfAbsent", "ns": ns, "key": "k1",
                "value": {"v": 2}, "ttlSeconds": 60 }),
    )
    .await?;
    chk!(
        applied(&r),
        "expect:0 must WIN against an expired-but-unpruned row (§5.3): {r}"
    );
    chk!(
        version(&r) != Some(v0),
        "resurrection must mint a new version (the sequence, never version+1): {r}"
    );
    let created1 = col_text(c, &ns, "k1", "created_at")
        .await?
        .ok_or("resurrected row missing")?;
    chk!(
        created1 != created0,
        "a resurrected lineage resets created_at (§5.7); it stayed at {created0}"
    );
    Ok(())
}

// ===========================================================================
// §5.4 — `max` does not saturate and does not truncate. The call that would
// overflow does not apply AND DOES NOT CONSUME BUDGET: `applied` IS the
// admission decision, so a rejected request must leave the counter untouched.
// ===========================================================================
async fn case_incr_max_rejects_without_consuming(c: &Client) -> Case {
    let ns = unique("kvquota");
    let key = "acme:2026081712";

    for want in 1..=3i64 {
        let r = one(
            c,
            json!({ "op": "incr", "ns": ns, "key": key, "delta": 1, "max": 3,
                    "ttlSeconds": 60 }),
        )
        .await?;
        chk!(applied(&r), "incr #{want} must apply under max=3: {r}");
        chk!(
            r.get("value").and_then(|v| v.as_i64()) == Some(want),
            "incr #{want} must return {want}: {r}"
        );
    }

    let over = one(
        c,
        json!({ "op": "incr", "ns": ns, "key": key, "delta": 1, "max": 3,
                "ttlSeconds": 60 }),
    )
    .await?;
    chk!(!applied(&over), "the 4th incr must be REFUSED under max=3: {over}");
    chk!(
        reason(&over) == "limit",
        "expected reason 'limit', got '{}': {over}",
        reason(&over)
    );
    chk!(
        over.get("value").and_then(|v| v.as_i64()) == Some(3),
        "a refused incr must return the CURRENT value (3), not the would-be one: {over}"
    );

    let g = one(c, json!({ "op": "get", "ns": ns, "key": key })).await?;
    chk!(
        g.get("value").and_then(|v| v.as_i64()) == Some(3),
        "a refused incr must NOT consume budget — counter should still be 3: {g}"
    );
    Ok(())
}

// ===========================================================================
// §5.4 repair 2 — the INSERT branch applies no guard in the naive form, so the
// FIRST incr of a window with delta > max returns applied:true and blows the
// quota on the first call, again at every window rotation. `delta` is a pure
// comparison and is validated BEFORE any write.
// ===========================================================================
async fn case_incr_first_call_over_max(c: &Client) -> Case {
    let ns = unique("kvfirst");
    let key = "burst";

    let r = one(
        c,
        json!({ "op": "incr", "ns": ns, "key": key, "delta": 10, "max": 5,
                "ttlSeconds": 60 }),
    )
    .await?;
    chk!(
        !applied(&r),
        "the FIRST incr with delta(10) > max(5) must be refused (§5.4 repair 2): {r}"
    );
    chk!(
        reason(&r) == "limit",
        "expected reason 'limit', got '{}': {r}",
        reason(&r)
    );
    chk!(
        row_count(c, &ns, key).await? == 0,
        "a refused first incr must write NOTHING — the INSERT branch needs the guard"
    );
    chk!(
        r.get("value").and_then(|v| v.as_i64()) == Some(0),
        "the effective value of an absent counter is 0 (kv_num_v1, §6.1): {r}"
    );

    // Symmetric arm: `min` is validated in the same pure pass.
    let n = one(
        c,
        json!({ "op": "incr", "ns": ns, "key": key, "delta": -10, "min": 0,
                "ttlSeconds": 60 }),
    )
    .await?;
    chk!(
        !applied(&n) && reason(&n) == "limit",
        "the first incr with delta below `min` must be refused with reason 'limit': {n}"
    );
    chk!(
        row_count(c, &ns, key).await? == 0,
        "a refused first incr must write NOTHING (min arm)"
    );
    Ok(())
}

// ===========================================================================
// §5.4 repair 3 — the type guard must not contradict §5.7. An EXPIRED
// non-numeric row counts as zero and the counter restarts; only a LIVE
// non-numeric row is reason:'type'. Without this, someone who "initialises" a
// key with an object gets every request refused until the sweeper runs, with a
// reason no client treats as retryable.
// ===========================================================================
async fn case_incr_expired_non_numeric_restarts(c: &Client) -> Case {
    let ns = unique("kvtype");
    let future = "now() + interval '10 seconds'";

    let s = one(
        c,
        json!({ "op": "put", "ns": ns, "key": "c1", "value": {"count": 0}, "ttlSeconds": 1 }),
    )
    .await?;
    chk!(applied(&s), "seed put must apply: {s}");

    let r = one_at(
        c,
        future,
        json!({ "op": "incr", "ns": ns, "key": "c1", "delta": 1, "ttlSeconds": 60 }),
    )
    .await?;
    chk!(
        applied(&r),
        "incr over an EXPIRED non-numeric row must restart from zero, not be \
         refused as 'type' (§5.4 repair 3): {r}"
    );
    chk!(
        reason(&r) == "<none>",
        "a successful incr carries no reason, got '{}': {r}",
        reason(&r)
    );
    chk!(
        r.get("value").and_then(|v| v.as_i64()) == Some(1),
        "the restarted counter must be 1: {r}"
    );

    // Control: a LIVE non-numeric row is still reason:'type'. The repair widens
    // the guard, it does not delete it.
    let s2 = one(
        c,
        json!({ "op": "put", "ns": ns, "key": "c2", "value": {"count": 0}, "ttlSeconds": 600 }),
    )
    .await?;
    chk!(applied(&s2), "seed put c2 must apply: {s2}");
    let t = one(
        c,
        json!({ "op": "incr", "ns": ns, "key": "c2", "delta": 1, "ttlSeconds": 60 }),
    )
    .await?;
    chk!(
        !applied(&t) && reason(&t) == "type",
        "incr over a LIVE non-numeric row must still be reason 'type': {t}"
    );
    Ok(())
}

// ===========================================================================
// §5.4 — the TTL of `incr` is CREATE-ONLY. A live row keeps its expiry: if incr
// extended it, a fixed-window limiter on an always-active client would never
// close its window, i.e. it would stop limiting exactly under load. (It also
// keeps the UPDATE HOT, since expires_at is the only indexed column.)
// ===========================================================================
async fn case_incr_ttl_is_create_only(c: &Client) -> Case {
    let ns = unique("kvwindow");
    let key = "w";

    let a = one(
        c,
        json!({ "op": "incr", "ns": ns, "key": key, "delta": 1, "ttlSeconds": 60 }),
    )
    .await?;
    chk!(applied(&a), "creating incr must apply: {a}");
    let exp0 = col_text(c, &ns, key, "expires_at")
        .await?
        .ok_or("created row has no expires_at — the TTL is mandatory (§5.1)")?;

    let b = one(
        c,
        json!({ "op": "incr", "ns": ns, "key": key, "delta": 1, "ttlSeconds": 86400 }),
    )
    .await?;
    chk!(applied(&b), "second incr must apply: {b}");
    chk!(
        b.get("value").and_then(|v| v.as_i64()) == Some(2),
        "second incr must return 2: {b}"
    );
    let exp1 = col_text(c, &ns, key, "expires_at")
        .await?
        .ok_or("row lost its expires_at")?;
    chk!(
        exp0 == exp1,
        "incr TTL is CREATE-ONLY: a live row keeps its expiry (§5.4). \
         It moved {exp0} -> {exp1}"
    );

    // `forever` on an existing row is equally inert.
    let f = one(
        c,
        json!({ "op": "incr", "ns": ns, "key": key, "delta": 1, "forever": true }),
    )
    .await?;
    chk!(applied(&f), "third incr must apply: {f}");
    let exp2 = col_text(c, &ns, key, "expires_at").await?;
    chk!(
        exp2.as_deref() == Some(exp0.as_str()),
        "incr with forever:true must not make a live row immortal either: {exp2:?}"
    );
    Ok(())
}

// ===========================================================================
// §5.5 — the prefix predicate is `starts_with`, never LIKE. `%` and `_` are
// LITERAL. This is the hole `009_streams_state_get_v1.sql` has today
// (`LIKE prefix || '%'` with no escaping) and it must not be inherited.
// ===========================================================================
async fn case_prefix_metacharacters_are_literal(c: &Client) -> Case {
    let ns = unique("kvprefix");
    let keys = ["a%b", "a%bc", "axb", "aXb", "a_b", "aQb", "ab", "a%"];
    let ops: Vec<Value> = keys
        .iter()
        .map(|k| json!({ "op": "put", "ns": ns, "key": k, "value": {"k": k}, "ttlSeconds": 600 }))
        .collect();
    let seeded = apply(c, json!(ops)).await?;
    chk!(
        seeded.iter().all(applied),
        "seeding the prefix namespace must apply: {seeded:?}"
    );

    let p = one(c, json!({ "op": "getPrefix", "ns": ns, "prefix": "a%b" })).await?;
    let mut got = keys_of(&p["rows"]);
    got.sort();
    chk!(
        got == vec!["a%b".to_string(), "a%bc".to_string()],
        "'%' must be a LITERAL, not a LIKE wildcard: prefix 'a%b' returned {got:?}"
    );

    let u = one(c, json!({ "op": "getPrefix", "ns": ns, "prefix": "a_b" })).await?;
    let mut gotu = keys_of(&u["rows"]);
    gotu.sort();
    chk!(
        gotu == vec!["a_b".to_string()],
        "'_' must be a LITERAL, not a LIKE single-char wildcard: prefix 'a_b' \
         returned {gotu:?}"
    );

    // Byte order, C collation: the page comes back in index order.
    let all = one(c, json!({ "op": "getPrefix", "ns": ns, "prefix": "a" })).await?;
    let ordered = keys_of(&all["rows"]);
    let mut sorted = ordered.clone();
    sorted.sort();
    chk!(
        ordered == sorted,
        "getPrefix must return byte order under COLLATE \"C\" (§3.2), got {ordered:?}"
    );
    Ok(())
}

// ===========================================================================
// §5.1 — every write carries EXACTLY ONE of `ttlSeconds` and `forever`. Zero or
// two declarations are `kv_expiry_not_specified`, SQLSTATE 22023.
// The MESSAGE stays opaque (§6.1 point 5): namespace and key names travel in
// the DETAIL, never in the message that reaches shared logs.
// ===========================================================================
async fn case_expiry_is_mandatory(c: &Client) -> Case {
    let ns = unique("kvttl");
    // Distinctive so the opacity assertion below cannot pass by accident.
    let key = "customer-7f3e-billing-secret";

    // `want_token` marks the cases §5.1 names explicitly: ZERO or TWO
    // declarations are `kv_expiry_not_specified`. A ttlSeconds that is present
    // but not an integer > 0 is a different sentence in the same rule, so only
    // its SQLSTATE is pinned here — the implementation may name it its own way.
    let bad = vec![
        (
            "put with no expiry",
            true,
            json!([{ "op": "put", "ns": ns, "key": key, "value": {"x": 1} }]),
        ),
        (
            "put with BOTH ttlSeconds and forever",
            true,
            json!([{ "op": "put", "ns": ns, "key": key, "value": {"x": 1},
                     "ttlSeconds": 60, "forever": true }]),
        ),
        (
            "putIfAbsent with no expiry",
            true,
            json!([{ "op": "putIfAbsent", "ns": ns, "key": key, "value": {"x": 1} }]),
        ),
        (
            "incr with no expiry",
            true,
            json!([{ "op": "incr", "ns": ns, "key": key, "delta": 1 }]),
        ),
        (
            "put with ttlSeconds = 0 (must be an integer > 0)",
            false,
            json!([{ "op": "put", "ns": ns, "key": key, "value": {"x": 1}, "ttlSeconds": 0 }]),
        ),
        (
            "put with a negative ttlSeconds",
            false,
            json!([{ "op": "put", "ns": ns, "key": key, "value": {"x": 1}, "ttlSeconds": -5 }]),
        ),
    ];

    for (what, want_token, ops) in bad {
        let (code, msg, detail) = expect_raise(c, "now()", false, ops).await?;
        chk!(
            code == "22023",
            "{what}: expected SQLSTATE 22023, got {code} ({msg})"
        );
        chk!(
            !want_token || format!("{msg} {detail}").contains("kv_expiry_not_specified"),
            "{what}: the error must name kv_expiry_not_specified (§5.1) so the \
             broker maps it without parsing prose; message={msg:?} detail={detail:?}"
        );
        chk!(
            !msg.contains(key) && !msg.contains(&ns),
            "{what}: the MESSAGE must stay opaque — names belong in the DETAIL \
             (§6.1 point 5); message={msg:?}"
        );
    }

    // VALIDATE-THEN-APPLY (§6.1 point 1): a rejected batch writes nothing, and a
    // valid op sitting BEFORE the invalid one must not have landed.
    let ops = json!([
        { "op": "put", "ns": ns, "key": "valid-first", "value": {"x": 1}, "ttlSeconds": 60 },
        { "op": "put", "ns": ns, "key": "no-ttl", "value": {"x": 2} }
    ]);
    let (code, _, _) = expect_raise(c, "now()", false, ops).await?;
    chk!(code == "22023", "batch validation must raise 22023, got {code}");
    chk!(
        row_count(c, &ns, "valid-first").await? == 0,
        "validate-then-apply: nothing may be written when any op is rejected (§6.1)"
    );
    Ok(())
}

// ===========================================================================
// §5.5 — `getPrefix` is FORBIDDEN in the transaction wire (RAISE 22023): its
// cost is not bounded a priori by the caller, and the wire holds the outermost
// lock space plus, downstream, the partition ones. `get` and `getMany` are
// allowed because the caller fixes their cost. The boundary is COST, not the
// kind of operation.
// ===========================================================================
async fn case_getprefix_forbidden_in_wire(c: &Client) -> Case {
    let ns = unique("kvwire");
    let seeded = one(
        c,
        json!({ "op": "put", "ns": ns, "key": "k1", "value": {"x": 1}, "ttlSeconds": 600 }),
    )
    .await?;
    chk!(applied(&seeded), "seed must apply: {seeded}");

    let (code, msg, _) = expect_raise(
        c,
        "now()",
        true,
        json!([{ "op": "getPrefix", "ns": ns, "prefix": "k" }]),
    )
    .await?;
    chk!(
        code == "22023",
        "getPrefix in the wire must raise 22023, got {code} ({msg})"
    );

    // Controls, all three of them, or the case above could pass for the wrong reason.
    let ok = apply_raw(
        c,
        "now()",
        true,
        json!([
            { "op": "get", "ns": ns, "key": "k1" },
            { "op": "getMany", "ns": ns, "keys": ["k1"] }
        ]),
    )
    .await
    .map_err(|e| format!("get/getMany must be ALLOWED in the wire (§5.5): {e}"))?;
    chk!(
        results(&ok)?.len() == 2,
        "wire-allowed reads must still be index-aligned: {ok}"
    );

    let out = apply(c, json!([{ "op": "getPrefix", "ns": ns, "prefix": "k" }])).await?;
    chk!(
        keys_of(&out[0]["rows"]).len() == 1,
        "getPrefix must work OUTSIDE the wire (p_in_wire = false): {out:?}"
    );
    Ok(())
}

// ===========================================================================
// §5.5 — the ceilings. `limit` is CLAMPED, never rejected (a 400 on a too-high
// limit is an error the user cannot fix without reading the server's config);
// `truncated` tells the truth; and there is a ceiling in BYTES, because a
// ceiling on the number of keys is not a ceiling on bytes — 1000 keys of 64 KiB
// are 64 MB. The real resource is the byte.
// ===========================================================================
async fn case_read_ceilings(c: &Client) -> Case {
    let ns = unique("kvcap");
    // 1200 small keys, seeded in chunks well under any per-call op budget.
    for chunk in (0..1200).collect::<Vec<i32>>().chunks(50) {
        let ops: Vec<Value> = chunk
            .iter()
            .map(|i| {
                json!({ "op": "put", "ns": ns, "key": format!("k{i:05}"),
                        "value": {"i": i}, "ttlSeconds": 600 })
            })
            .collect();
        let r = apply(c, json!(ops)).await?;
        chk!(r.iter().all(applied), "seeding must apply: {r:?}");
    }

    // limit far above the cap: clamped, NOT rejected.
    let big = one(
        c,
        json!({ "op": "getPrefix", "ns": ns, "prefix": "k", "limit": 5000 }),
    )
    .await?;
    let rows = keys_of(&big["rows"]);
    chk!(
        rows.len() == PREFIX_LIMIT_CAP,
        "limit must be CLAMPED to {PREFIX_LIMIT_CAP} and never rejected (§5.5), \
         got {} rows",
        rows.len()
    );
    chk!(
        big.get("truncated") == Some(&Value::Bool(true)),
        "truncated must tell the truth when rows were left behind: {big}"
    );
    chk!(
        big.get("nextAfter").and_then(|v| v.as_str()) == rows.last().map(|s| s.as_str()),
        "nextAfter must be the last returned key (exclusive keyset cursor): {big}"
    );

    // No limit: the default.
    let def = one(c, json!({ "op": "getPrefix", "ns": ns, "prefix": "k" })).await?;
    chk!(
        keys_of(&def["rows"]).len() == PREFIX_LIMIT_DEFAULT,
        "the default limit is {PREFIX_LIMIT_DEFAULT} (§5.5), got {}",
        keys_of(&def["rows"]).len()
    );

    // Keyset paging, and `truncated` honest on the LAST page too.
    let p1 = one(
        c,
        json!({ "op": "getPrefix", "ns": ns, "prefix": "k01", "limit": 3 }),
    )
    .await?;
    let k1 = keys_of(&p1["rows"]);
    chk!(k1.len() == 3, "page 1 must hold 3 rows: {p1}");
    chk!(
        p1.get("truncated") == Some(&Value::Bool(true)),
        "page 1 must be truncated: {p1}"
    );
    let after = p1["nextAfter"].as_str().unwrap_or("").to_string();
    let p2 = one(
        c,
        json!({ "op": "getPrefix", "ns": ns, "prefix": "k01", "limit": 3, "after": after }),
    )
    .await?;
    let k2 = keys_of(&p2["rows"]);
    chk!(
        k2.first() > k1.last(),
        "`after` is an EXCLUSIVE keyset cursor, not an offset: page1={k1:?} page2={k2:?}"
    );

    // The byte ceiling. 80 values of ~60 KiB = ~4.8 MiB > QUEEN_KV_MAX_READ_BYTES.
    let nsb = unique("kvbytes");
    let blob = "x".repeat(60 * 1024);
    for chunk in (0..80).collect::<Vec<i32>>().chunks(4) {
        let ops: Vec<Value> = chunk
            .iter()
            .map(|i| {
                json!({ "op": "put", "ns": nsb, "key": format!("b{i:03}"),
                        "value": blob, "ttlSeconds": 600 })
            })
            .collect();
        let r = apply(c, json!(ops)).await?;
        chk!(r.iter().all(applied), "seeding big values must apply: {r:?}");
    }
    let fat = one(
        c,
        json!({ "op": "getPrefix", "ns": nsb, "prefix": "b", "limit": 1000 }),
    )
    .await?;
    let fat_rows = fat["rows"].as_array().cloned().unwrap_or_default();
    chk!(
        fat_rows.len() < 80,
        "the ceiling is in BYTES, not rows (§5.5): all 80 rows (~4.8 MiB) came \
         back under QUEEN_KV_MAX_READ_BYTES = {MAX_READ_BYTES}"
    );
    chk!(
        fat.get("truncated") == Some(&Value::Bool(true)),
        "a byte-capped page must report truncated:true: {}",
        fat_rows.len()
    );
    let bytes: usize = fat_rows
        .iter()
        .map(|r| r.get("value").map(|v| v.to_string().len()).unwrap_or(0))
        .sum();
    chk!(
        bytes <= MAX_READ_BYTES + MAX_VALUE_BYTES,
        "aggregate read bytes {bytes} exceed QUEEN_KV_MAX_READ_BYTES \
         ({MAX_READ_BYTES}) by more than one straddling row"
    );
    Ok(())
}

// ===========================================================================
// §5.3 — the loser of a CAS gets the CURRENT value and version back, in the
// SAME statement (a separate discriminating SELECT would run on a later
// snapshot and could hand back an already-stale version, producing a CAS loop
// that never converges on a contended key; there is no server-side backoff).
// ===========================================================================
async fn case_loser_gets_current_value_and_version(c: &Client) -> Case {
    let ns = unique("kvcas");
    let key = "cas";

    let a = one(
        c,
        json!({ "op": "put", "ns": ns, "key": key, "value": {"n": 1}, "ttlSeconds": 600 }),
    )
    .await?;
    chk!(applied(&a), "seed put must apply: {a}");
    let v1 = version(&a).ok_or("seed put carries no version")?;

    let lost = one(
        c,
        json!({ "op": "put", "ns": ns, "key": key, "value": {"n": 2},
                "ttlSeconds": 600, "expect": v1 + 777 }),
    )
    .await?;
    chk!(!applied(&lost), "a stale expect must not apply: {lost}");
    chk!(
        reason(&lost) == "version",
        "expected reason 'version', got '{}': {lost}",
        reason(&lost)
    );
    chk!(
        lost.get("value") == Some(&json!({"n": 1})),
        "the loser must carry the CURRENT value: {lost}"
    );
    chk!(
        version(&lost) == Some(v1),
        "the loser must carry the CURRENT version {v1}, discriminated in the same \
         statement (§5.3): {lost}"
    );

    let g = one(c, json!({ "op": "get", "ns": ns, "key": key })).await?;
    chk!(
        g.get("value") == Some(&json!({"n": 1})) && version(&g) == Some(v1),
        "a lost CAS must have written nothing: {g}"
    );

    // Same shape for delete.
    let dl = one(
        c,
        json!({ "op": "delete", "ns": ns, "key": key, "expect": v1 + 777 }),
    )
    .await?;
    chk!(
        !applied(&dl) && reason(&dl) == "version",
        "a delete with a stale expect must be reason 'version': {dl}"
    );
    chk!(
        dl.get("value") == Some(&json!({"n": 1})) && version(&dl) == Some(v1),
        "the losing delete must carry the current value and version: {dl}"
    );

    let ok = one(
        c,
        json!({ "op": "delete", "ns": ns, "key": key, "expect": v1 }),
    )
    .await?;
    chk!(applied(&ok), "delete with the matching version must apply: {ok}");
    chk!(
        row_count(c, &ns, key).await? == 0,
        "the row must be gone after a matching delete"
    );

    // §5.3: putIfAbsent AND a non-zero expect is a contradiction, not a downgrade.
    let (code, _, _) = expect_raise(
        c,
        "now()",
        false,
        json!([{ "op": "putIfAbsent", "ns": ns, "key": key, "value": {"n": 9},
                 "ttlSeconds": 60, "expect": 42 }]),
    )
    .await?;
    chk!(
        code == "22023",
        "putIfAbsent with expect != 0 must raise 22023 (§5.3), got {code}"
    );
    Ok(())
}

// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn kv_semantics() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // Boot the real broker purely to apply the real schema — the SQL under test
    // is include_str!-embedded, so this is also what proves 024_kv.sql compiled
    // in AND that it is registered in the PROCEDURES list of schema.rs. A file
    // that exists on disk but is missing from that list is never applied, and
    // every case below would fail with 42883 instead of passing silently.
    let _broker = Broker::start(
        BrokerConfig::new()
            .pg(host.clone(), port, "postgres", "postgres", "postgres")
            .pool_size(4),
    )
    .await
    .expect("broker start");

    let c = connect(&host, port).await;

    let mut report: Vec<(&str, Case)> = Vec::new();
    report.push(("one_winner_among_concurrent_putIfAbsent", case_one_winner(&host, port).await));
    report.push(("expect_positive_never_creates", case_expect_positive_never_creates(&c).await));
    report.push(("expired_reads_as_absent", case_expired_reads_as_absent(&c).await));
    report.push(("incr_max_rejects_without_consuming", case_incr_max_rejects_without_consuming(&c).await));
    report.push(("incr_first_call_over_max", case_incr_first_call_over_max(&c).await));
    report.push(("incr_expired_non_numeric_restarts", case_incr_expired_non_numeric_restarts(&c).await));
    report.push(("incr_ttl_is_create_only", case_incr_ttl_is_create_only(&c).await));
    report.push(("prefix_metacharacters_are_literal", case_prefix_metacharacters_are_literal(&c).await));
    report.push(("expiry_is_mandatory", case_expiry_is_mandatory(&c).await));
    report.push(("getPrefix_forbidden_in_wire", case_getprefix_forbidden_in_wire(&c).await));
    report.push(("read_ceilings", case_read_ceilings(&c).await));
    report.push(("loser_gets_current_value_and_version", case_loser_gets_current_value_and_version(&c).await));

    println!("\n===================== KV semantics (PLAN_KV_TIMERS §5) =====================");
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
    assert_eq!(failed, 0, "{failed} KV semantics case(s) failed — see the table above");
}
