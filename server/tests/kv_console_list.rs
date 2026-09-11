//! THE CONSOLE'S KV READ — `queen.kv_list_v1` and `queen.kv_namespaces_v1`,
//! tested against a real Postgres. PLAN_DASHBOARD_ACTIONS.md §2.5.
//!
//! WHY THIS LEVEL, and it is the same answer `kv_semantics.rs` gives: every rule
//! under test lives in SQL, deliberately (§9.2), so that the HTTP route, a
//! direct SQL caller and the embedded broker inherit one implementation instead
//! of three. A test that went through the HTTP handler would prove the rule for
//! one of those surfaces and nothing about the other two.
//!
//! WHAT IS ACTUALLY AT RISK HERE. `kv_list_v1` is the `getPrefix` arm of
//! `kv_apply_v1` with two changes — the prefix may be empty, and the liveness
//! predicate is a parameter — and both changes are of the kind that look right
//! and are silently wrong:
//!
//!   * an empty prefix that accidentally excluded a key would make the dashboard
//!     show a namespace as smaller than it is, with nothing on screen to say so;
//!   * a cursor that were inclusive rather than exclusive would repeat one row
//!     per page forever, and an operator paging a namespace would never reach
//!     the end;
//!   * `include_expired` inverted would hide exactly the rows the header counts
//!     (§2.5 D5), so the page and its own title would disagree.
//!
//! Each of those is one boolean away, and none of them raises.
//!
//! EXPIRY WITHOUT SLEEPS, AND WITHOUT `p_now`. `kv_apply_v1` takes the instant as
//! an argument, so `kv_semantics.rs` reads "in the future" by passing
//! `now() + interval '10 seconds'`. This function does NOT take one — its caller
//! is a console and the honest instant is the transaction's — so the expiry
//! cases move the ROW instead: a normal put, then `expires_at` pushed into the
//! past with one UPDATE. The physical row stays exactly where it was, which is
//! the point: what is under test is the predicate, not the sweeper.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`,
//! the convention `kv_semantics.rs` and `timers_count.rs` already follow:
//!
//! ```bash
//! docker run --rm -d --name queen-w4-pg -e POSTGRES_PASSWORD=postgres -p 5473:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5473 cargo test --test kv_console_list -- --ignored --nocapture
//! ```
//!
//! ONE test function on purpose (a Broker is booted to apply the real,
//! `include_str!`-embedded schema, and the admission arbiter is process-global).
//! The cases are still reported one by one: each returns `Result<(), String>` and
//! the runner prints a PASS/FAIL line per case before failing, so a red run names
//! every broken rule instead of only the first.

use queen::{Broker, BrokerConfig};
use serde_json::{json, Value};
use tokio_postgres::Client;

/// `config::DEFAULT_TENANT`, the tenant every other test in this repo uses.
const DEFAULT_TENANT: &str = "00000000-0000-0000-0000-000000000001";
/// A second tenant, invented here, used by exactly one case: isolation is a
/// WHERE clause inside these procedures (§13.1) and a WHERE clause is a thing a
/// test can actually check.
const OTHER_TENANT: &str = "00000000-0000-0000-0000-0000000009f1";

/// §2.5: `limit` defaults to 100 and is clamped to 1000, the same two numbers
/// `getPrefix` uses — they are duplicated constants in the two DECLARE blocks
/// and this file is where the duplication gets noticed if it ever drifts.
const LIMIT_DEFAULT: usize = 100;
const LIMIT_CAP: usize = 1000;
/// `QUEEN_KV_MAX_READ_BYTES`, applied to the page INSIDE the stored procedure.
const MAX_READ_BYTES: usize = 4 * 1024 * 1024;
/// `QUEEN_KV_MAX_VALUE_BYTES`. Only used as the tolerance for the one row that
/// is allowed to straddle the byte budget.
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

/// `tokio_postgres::Error`'s Display is "db error" and nothing else, which turns
/// a red suite into identical lines. Unwrap the SQLSTATE, message and detail.
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

/// One page. Every argument is bound and cast the way `db::kv_list` binds and
/// casts it, so what runs here is the statement the broker runs.
#[allow(clippy::too_many_arguments)]
async fn list_raw(
    c: &Client,
    tenant: &str,
    ns: &str,
    prefix: &str,
    after: Option<&str>,
    limit: Option<i32>,
    keys_only: bool,
    include_expired: bool,
) -> Result<Value, tokio_postgres::Error> {
    let row = c
        .query_one(
            "SELECT (queen.kv_list_v1($1::text::uuid, $2, $3::text, $4::text, $5::int, \
             $6::bool, $7::bool))::text",
            &[&tenant, &ns, &prefix, &after, &limit, &keys_only, &include_expired],
        )
        .await?;
    let txt: String = row.get(0);
    Ok(serde_json::from_str(&txt).unwrap_or(Value::Null))
}

/// The console's own call: the default tenant, values included, expired rows
/// SHOWN — which is the handler's default and the one asymmetric default of the
/// whole surface (§2.5 D5).
async fn list(
    c: &Client,
    ns: &str,
    prefix: &str,
    after: Option<&str>,
    limit: Option<i32>,
) -> Result<Value, String> {
    list_raw(c, DEFAULT_TENANT, ns, prefix, after, limit, false, true)
        .await
        .map_err(pg_err)
}

async fn namespaces(c: &Client, tenant: &str) -> Result<Value, String> {
    let row = c
        .query_one(
            "SELECT (queen.kv_namespaces_v1($1::text::uuid))::text",
            &[&tenant],
        )
        .await
        .map_err(pg_err)?;
    let txt: String = row.get(0);
    Ok(serde_json::from_str(&txt).unwrap_or(Value::Null))
}

/// Seed through the real write path — `kv_apply_v1` — rather than an INSERT, so
/// the rows under test are rows the product itself could have written (version
/// from the sequence, expiry through the mandatory declaration, shard generated).
async fn put_many(c: &Client, tenant: &str, ns: &str, ops: Vec<Value>) -> Result<(), String> {
    for chunk in ops.chunks(50) {
        let arr = Value::Array(chunk.to_vec());
        let row = c
            .query_one(
                "SELECT (queen.kv_apply_v1($1::text::jsonb, $2::text::uuid, now(), false))::text",
                &[&arr.to_string(), &tenant],
            )
            .await
            .map_err(pg_err)?;
        let txt: String = row.get(0);
        let res: Value = serde_json::from_str(&txt).unwrap_or(Value::Null);
        let all = res
            .as_array()
            .map(|a| a.iter().all(|r| r.get("applied") == Some(&Value::Bool(true))))
            .unwrap_or(false);
        if !all {
            return Err(format!("seeding {ns} did not apply: {res}"));
        }
    }
    Ok(())
}

fn puts(ns: &str, keys: &[&str], value: Value) -> Vec<Value> {
    keys.iter()
        .map(|k| json!({ "op": "put", "ns": ns, "key": k, "value": value, "ttlSeconds": 600 }))
        .collect()
}

fn rows_of(page: &Value) -> Vec<Value> {
    page.get("rows")
        .and_then(|r| r.as_array())
        .cloned()
        .unwrap_or_default()
}

fn keys_of(page: &Value) -> Vec<String> {
    rows_of(page)
        .iter()
        .map(|r| r.get("key").and_then(|k| k.as_str()).unwrap_or("<nokey>").to_string())
        .collect()
}

fn truncated(page: &Value) -> Option<bool> {
    page.get("truncated").and_then(|t| t.as_bool())
}

fn next_after(page: &Value) -> Option<String> {
    page.get("nextAfter").and_then(|n| n.as_str()).map(String::from)
}

/// Push a row's expiry into the past, in place. The row keeps its version, its
/// value and its position in the index — only the predicate's answer changes.
async fn expire_now(c: &Client, tenant: &str, ns: &str, key: &str) -> Result<(), String> {
    c.execute(
        "UPDATE queen.kv SET expires_at = now() - interval '1 minute' \
         WHERE tenant_id = $1::text::uuid AND namespace = $2 AND key = $3",
        &[&tenant, &ns, &key],
    )
    .await
    .map(|_| ())
    .map_err(|e| format!("expire_now({key}): {e}"))
}

// ===========================================================================
// An EMPTY prefix lists the whole namespace, in BYTE order.
//
// This is the difference between this function and `getPrefix`, which refuses an
// empty prefix on purpose (§5.5: a namespace is not a table to enumerate). The
// console needs exactly that, because an operator who has just opened a
// namespace has nothing to type yet.
//
// The order is BYTE order and not locale order, because both name columns carry
// COLLATE "C" (§2.3): "B" sorts before "a", every machine agrees, and a
// libc/ICU upgrade cannot move a cursor. The keys below are chosen so that a
// locale-ordered answer would be a DIFFERENT answer.
// ===========================================================================
async fn case_empty_prefix_lists_the_whole_namespace(c: &Client) -> Case {
    let ns = unique("kvall");
    put_many(c, DEFAULT_TENANT, &ns, puts(&ns, &["B", "a", "Z", "b", "A"], json!(1))).await?;

    let page = list(c, &ns, "", None, None).await?;
    chk!(
        keys_of(&page) == vec!["A", "B", "Z", "a", "b"],
        "an empty prefix must list the whole namespace in BYTE order (COLLATE \"C\"), got {:?}",
        keys_of(&page)
    );
    chk!(
        truncated(&page) == Some(false) && next_after(&page).is_none(),
        "a page that fit must be truncated:false with a null cursor: {page}"
    );
    chk!(
        page.get("bytes").and_then(|b| b.as_i64()).unwrap_or(-1) > 0,
        "the page must report the bytes it actually serialized: {page}"
    );

    // NULL and '' mean the same thing: the caller who omitted the field and the
    // caller who zeroed it are asking the same question.
    let null_prefix = list_raw(c, DEFAULT_TENANT, &ns, "", None, None, false, true)
        .await
        .map_err(pg_err)?;
    chk!(
        keys_of(&null_prefix) == keys_of(&page),
        "'' and an omitted prefix must be the same question"
    );

    // Every row of a value-bearing page carries the five fields the console
    // renders. A missing one is a blank column, never an error.
    let first = rows_of(&page).first().cloned().unwrap_or(Value::Null);
    for f in ["key", "value", "version", "expiresAt", "updatedAt", "expired"] {
        chk!(first.get(f).is_some(), "a row must carry `{f}`: {first}");
    }
    chk!(
        first.get("expired") == Some(&Value::Bool(false)),
        "a live row must read expired:false: {first}"
    );
    Ok(())
}

// ===========================================================================
// A prefix is a TIGHT RANGE, and `%` / `_` are ordinary bytes.
//
// The predicate is the same triple `getPrefix` uses — range bounds as the index
// driver, `starts_with()` as the semantics — so a key that merely CONTAINS the
// prefix is not a match, and a LIKE metacharacter is a byte like any other.
// ===========================================================================
async fn case_prefix_is_a_tight_range(c: &Client) -> Case {
    let ns = unique("kvpfx");
    put_many(
        c,
        DEFAULT_TENANT,
        &ns,
        puts(
            &ns,
            &["wh.deliver:a", "wh.deliver:b", "wh.retry:a", "xx.deliver:a", "a%b", "azb"],
            json!({ "v": 1 }),
        ),
    )
    .await?;

    let page = list(c, &ns, "wh.deliver:", None, None).await?;
    chk!(
        keys_of(&page) == vec!["wh.deliver:a", "wh.deliver:b"],
        "a prefix must select its own range and nothing else, got {:?}",
        keys_of(&page)
    );

    // `%` is a byte. If the predicate ever became a LIKE, `a%b` would also match
    // `azb` and the page would silently grow.
    let meta = list(c, &ns, "a%", None, None).await?;
    chk!(
        keys_of(&meta) == vec!["a%b"],
        "`%` must be a literal byte, not a LIKE wildcard, got {:?}",
        keys_of(&meta)
    );

    // A prefix nobody wrote is an empty page, never an error: an unknown
    // namespace and an unmatched prefix are both "nothing here" (§3.2).
    let empty = list(c, &ns, "zzz-nothing", None, None).await?;
    chk!(
        rows_of(&empty).is_empty() && truncated(&empty) == Some(false),
        "an unmatched prefix is an empty, untruncated page: {empty}"
    );
    let absent_ns = list(c, &unique("kvnone"), "", None, None).await?;
    chk!(
        rows_of(&absent_ns).is_empty(),
        "an unknown namespace is an empty page, never an error: {absent_ns}"
    );
    Ok(())
}

// ===========================================================================
// THE CURSOR: exclusive, and honest at the end.
//
// Three pages of two over five keys. The cursor is a KEY, not an offset, so
// page 3 costs what page 1 costs — and it must not repeat its predecessor's
// last row, which is the failure an inclusive `>=` would produce: an operator
// paging a namespace would see one duplicate per page and never reach the end.
//
// `truncated` is decided by the LIMIT + 1 probe row, so the last page — exactly
// full or not — reports false and hands back a null cursor. A client that paged
// on a non-null `nextAfter` alone must therefore terminate.
// ===========================================================================
async fn case_the_cursor_is_exclusive_and_ends(c: &Client) -> Case {
    let ns = unique("kvpage");
    put_many(c, DEFAULT_TENANT, &ns, puts(&ns, &["k1", "k2", "k3", "k4", "k5"], json!(0))).await?;

    let p1 = list(c, &ns, "", None, Some(2)).await?;
    chk!(keys_of(&p1) == vec!["k1", "k2"], "page 1: {:?}", keys_of(&p1));
    chk!(truncated(&p1) == Some(true), "page 1 must be truncated: {p1}");
    chk!(
        next_after(&p1).as_deref() == Some("k2"),
        "nextAfter must be the LAST RETURNED key: {p1}"
    );

    let p2 = list(c, &ns, "", next_after(&p1).as_deref(), Some(2)).await?;
    chk!(
        keys_of(&p2) == vec!["k3", "k4"],
        "the cursor is EXCLUSIVE: page 2 must not repeat k2, got {:?}",
        keys_of(&p2)
    );
    chk!(truncated(&p2) == Some(true), "page 2 must be truncated: {p2}");

    let p3 = list(c, &ns, "", next_after(&p2).as_deref(), Some(2)).await?;
    chk!(keys_of(&p3) == vec!["k5"], "page 3: {:?}", keys_of(&p3));
    chk!(
        truncated(&p3) == Some(false),
        "the last page must report truncated:false: {p3}"
    );
    chk!(
        next_after(&p3).is_none(),
        "the last page must hand back a NULL cursor, or a pager never stops: {p3}"
    );

    // The exactly-full last page is the case that decides whether `truncated`
    // comes from a probe row or from arithmetic. Four keys, limit 2, two pages:
    // the second is full AND final.
    let ns4 = unique("kvpage4");
    put_many(c, DEFAULT_TENANT, &ns4, puts(&ns4, &["a", "b", "c", "d"], json!(0))).await?;
    let f1 = list(c, &ns4, "", None, Some(2)).await?;
    let f2 = list(c, &ns4, "", next_after(&f1).as_deref(), Some(2)).await?;
    chk!(keys_of(&f2) == vec!["c", "d"], "page 2 of 2: {:?}", keys_of(&f2));
    chk!(
        truncated(&f2) == Some(false) && next_after(&f2).is_none(),
        "a last page that is exactly full is still the last page: {f2}"
    );

    // The cursor may name a key that does not exist — an operator pasting one
    // out of a ticket, or a key deleted between two pages. It is a POSITION.
    let ghost = list(c, &ns, "", Some("k2a"), Some(10)).await?;
    chk!(
        keys_of(&ghost) == vec!["k3", "k4", "k5"],
        "the cursor is a position, not a row that must exist: {:?}",
        keys_of(&ghost)
    );
    Ok(())
}

// ===========================================================================
// THE LIMIT is clamped, never rejected — and the default is 100.
//
// A 400 on a too-high limit is an error the caller cannot fix without reading
// the server's configuration (§5.5), so the ceiling is applied silently and
// `truncated` tells the truth about what was left behind.
// ===========================================================================
async fn case_the_limit_is_clamped_and_defaults(c: &Client) -> Case {
    let ns = unique("kvlim");
    let keys: Vec<String> = (0..1200).map(|i| format!("k{i:05}")).collect();
    let ops: Vec<Value> = keys
        .iter()
        .map(|k| json!({ "op": "put", "ns": ns, "key": k, "value": {"i": 1}, "ttlSeconds": 600 }))
        .collect();
    put_many(c, DEFAULT_TENANT, &ns, ops).await?;

    let def = list(c, &ns, "", None, None).await?;
    chk!(
        rows_of(&def).len() == LIMIT_DEFAULT,
        "the default limit is {LIMIT_DEFAULT}, got {}",
        rows_of(&def).len()
    );
    chk!(truncated(&def) == Some(true), "1200 keys, 100 shown: {}", rows_of(&def).len());

    let big = list(c, &ns, "", None, Some(5000)).await?;
    chk!(
        rows_of(&big).len() == LIMIT_CAP,
        "a limit above the cap must be CLAMPED to {LIMIT_CAP} and never rejected, got {}",
        rows_of(&big).len()
    );
    chk!(
        next_after(&big).as_deref() == keys_of(&big).last().map(|s| s.as_str()),
        "nextAfter must be the last returned key: {:?}",
        next_after(&big)
    );

    // Zero and negative clamp UP to one, for the same reason: they are a
    // caller's arithmetic mistake, not a request to hang the page.
    for bad in [0, -7] {
        let p = list(c, &ns, "", None, Some(bad)).await?;
        chk!(
            rows_of(&p).len() == 1,
            "limit {bad} must clamp to 1, got {} rows",
            rows_of(&p).len()
        );
    }
    Ok(())
}

// ===========================================================================
// THE BYTE BUDGET ends a page early.
//
// A ceiling on the number of keys is not a ceiling on bytes: 1000 keys of 64 KiB
// are 64 MB, and the real resource is the byte (§5.5). The row that straddles
// the budget is included and then the page stops, so a namespace of fat values
// still pages — one row at a time if it must — instead of answering 413 forever.
//
// `keysOnly` is the console's way out of this: with the values dropped the same
// namespace pages at the row limit, because the byte cost of a page becomes
// zero. That is why the flag exists at all.
// ===========================================================================
async fn case_the_byte_budget_ends_the_page(c: &Client) -> Case {
    let ns = unique("kvfat");
    let blob = "x".repeat(60 * 1024);
    let ops: Vec<Value> = (0..80)
        .map(|i| {
            json!({ "op": "put", "ns": ns, "key": format!("b{i:03}"),
                    "value": blob, "ttlSeconds": 600 })
        })
        .collect();
    put_many(c, DEFAULT_TENANT, &ns, ops).await?;

    let fat = list(c, &ns, "", None, Some(1000)).await?;
    let n = rows_of(&fat).len();
    chk!(
        n < 80,
        "the ceiling is in BYTES, not rows (§5.5): all 80 rows (~4.8 MiB) came back \
         under QUEEN_KV_MAX_READ_BYTES = {MAX_READ_BYTES}"
    );
    chk!(
        truncated(&fat) == Some(true) && next_after(&fat).is_some(),
        "a byte-capped page must be truncated AND carry a cursor, or the rows it \
         cut are unreachable: {n} rows, {fat:?}",
    );
    let bytes: usize = rows_of(&fat)
        .iter()
        .map(|r| r.get("value").map(|v| v.to_string().len()).unwrap_or(0))
        .sum();
    chk!(
        bytes <= MAX_READ_BYTES + MAX_VALUE_BYTES,
        "aggregate read bytes {bytes} exceed QUEEN_KV_MAX_READ_BYTES ({MAX_READ_BYTES}) \
         by more than the one row allowed to straddle it"
    );
    chk!(
        fat.get("bytes").and_then(|b| b.as_i64()).unwrap_or(0) as usize <= bytes,
        "the reported `bytes` is the canonical JSONB length, which cannot exceed the \
         serialized one: {fat:?}"
    );

    // The cut rows are reachable: the cursor is honest, so the next page starts
    // where this one stopped.
    let p2 = list(c, &ns, "", next_after(&fat).as_deref(), Some(1000)).await?;
    chk!(
        keys_of(&p2).first() > keys_of(&fat).last(),
        "the page after a byte-capped one must continue past it: {:?} then {:?}",
        keys_of(&fat).last(),
        keys_of(&p2).first()
    );

    // keysOnly: no values, and therefore no byte cost.
    let ko = list_raw(c, DEFAULT_TENANT, &ns, "", None, Some(1000), true, true)
        .await
        .map_err(pg_err)?;
    chk!(
        rows_of(&ko).len() == 80,
        "keysOnly drops the values, so the same namespace pages at the ROW limit: \
         got {} of 80",
        rows_of(&ko).len()
    );
    let row = rows_of(&ko).first().cloned().unwrap_or(Value::Null);
    chk!(
        row.get("value").is_none(),
        "keysOnly must OMIT the value, not null it: {row}"
    );
    for f in ["key", "version", "expiresAt", "updatedAt", "expired"] {
        chk!(row.get(f).is_some(), "keysOnly must keep `{f}`: {row}");
    }
    chk!(
        ko.get("bytes").and_then(|b| b.as_i64()) == Some(0),
        "a keysOnly page costs no value bytes: {ko:?}"
    );
    Ok(())
}

// ===========================================================================
// EXPIRED ROWS: shown and labelled, or excluded — the caller decides.
//
// §2.5 D5. An expired row still occupies the tenant's allowance and still counts
// in the sweeper's `kvRows`, so a console that hid it would print a header its
// own list contradicts. `getPrefix` hides it (§5.7) because an application must
// never see a dead marker; the console shows it greyed and says "awaiting
// sweep". Both are right, which is exactly why the predicate is a parameter.
// ===========================================================================
async fn case_expired_rows_are_visible_and_hidable(c: &Client) -> Case {
    let ns = unique("kvexp");
    put_many(c, DEFAULT_TENANT, &ns, puts(&ns, &["alive", "dead"], json!({"v": 1}))).await?;
    expire_now(c, DEFAULT_TENANT, &ns, "dead").await?;

    let shown = list_raw(c, DEFAULT_TENANT, &ns, "", None, None, false, true)
        .await
        .map_err(pg_err)?;
    chk!(
        keys_of(&shown) == vec!["alive", "dead"],
        "with include_expired the dead row must be IN the page: {:?}",
        keys_of(&shown)
    );
    let dead = rows_of(&shown)
        .into_iter()
        .find(|r| r.get("key") == Some(&json!("dead")))
        .ok_or("the expired row vanished")?;
    chk!(
        dead.get("expired") == Some(&Value::Bool(true)),
        "an expired row must be LABELLED, not silently mixed in: {dead}"
    );
    chk!(
        dead.get("value").is_some() && dead.get("version").is_some(),
        "an expired row keeps its value and version — it is still a row: {dead}"
    );
    let alive = rows_of(&shown)
        .into_iter()
        .find(|r| r.get("key") == Some(&json!("alive")))
        .ok_or("the live row vanished")?;
    chk!(
        alive.get("expired") == Some(&Value::Bool(false)),
        "a live row must read expired:false: {alive}"
    );

    let hidden = list_raw(c, DEFAULT_TENANT, &ns, "", None, None, false, false)
        .await
        .map_err(pg_err)?;
    chk!(
        keys_of(&hidden) == vec!["alive"],
        "without include_expired the dead row must be gone, exactly as getPrefix \
         would answer (§5.7): {:?}",
        keys_of(&hidden)
    );

    // A forever key (expires_at NULL) is never expired, and NULL must not make
    // the predicate vanish — that is what kv_live_v1 being non-STRICT buys.
    let nsf = unique("kvfor");
    put_many(
        c,
        DEFAULT_TENANT,
        &nsf,
        vec![json!({ "op": "put", "ns": nsf, "key": "f", "value": 1, "forever": true })],
    )
    .await?;
    let f = list(c, &nsf, "", None, None).await?;
    let frow = rows_of(&f).first().cloned().unwrap_or(Value::Null);
    chk!(
        frow.get("expired") == Some(&Value::Bool(false))
            && frow.get("expiresAt") == Some(&Value::Null),
        "a forever key is expiresAt:null and expired:false: {frow}"
    );
    let f_hidden = list_raw(c, DEFAULT_TENANT, &nsf, "", None, None, false, false)
        .await
        .map_err(pg_err)?;
    chk!(
        rows_of(&f_hidden).len() == 1,
        "a forever key must survive the liveness filter: {f_hidden}"
    );

    // The expired row still counts in the namespace listing, because it still
    // occupies the tenant's key space. NOT the population queen.kv_usage
    // measures: the sweeper counts LIVE rows only, on a five-minute cadence, and
    // samples above its threshold (026_kv_sweeper.sql kv_usage_step_v1), which
    // is why the dashboard renders that figure with `≈` and this one exact, and
    // why the two are allowed to disagree on screen.
    let all = namespaces(c, DEFAULT_TENANT).await?;
    let mine = all
        .as_array()
        .and_then(|a| a.iter().find(|e| e.get("namespace") == Some(&json!(ns))))
        .cloned()
        .ok_or_else(|| format!("namespace {ns} missing from the listing"))?;
    chk!(
        mine.get("keys") == Some(&json!(2)),
        "an expired row is not a live key, but it IS an occupied one — the selector \
         and the console's own page must count the same population: {mine}"
    );
    Ok(())
}

// ===========================================================================
// THE STAMPS ARE UTC, WHATEVER THE SESSION'S TIME ZONE IS.
//
// Handing a timestamptz to jsonb renders it through the session's TimeZone and
// DateStyle GUCs, and NOTHING in the broker pins either (tests/procedures_
// timezone.rs exists for this bug class, and its source half only scans
// `to_char` sites — a raw column slips past it). A cell whose Postgres was
// installed in Rome would then answer `…+02:00` here and `…Z` on every other
// listing in the product.
//
// It is not a wrong INSTANT either way — the offset is carried, and the
// dashboard parses both forms — but one surface rendering its stamps in the
// server's local time is exactly how a future reader concludes the broker
// stores wall clocks. So the session is deliberately moved to a zone that is
// neither UTC nor the machine's likely default, and the render must not move.
// ===========================================================================
async fn case_stamps_are_utc_under_any_session_timezone(c: &Client) -> Case {
    let ns = unique("kvtz");
    put_many(c, DEFAULT_TENANT, &ns, puts(&ns, &["k"], json!(1))).await?;

    for tz in ["UTC", "Europe/Rome", "America/Sao_Paulo"] {
        c.execute(&format!("SET TimeZone = '{tz}'"), &[])
            .await
            .map_err(|e| format!("set TimeZone {tz}: {e}"))?;
        let page = list(c, &ns, "", None, None).await?;
        let row = rows_of(&page).first().cloned().unwrap_or(Value::Null);
        for f in ["expiresAt", "updatedAt"] {
            let s = row
                .get(f)
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("{f} is missing or not a string under {tz}: {row}"))?;
            chk!(
                s.ends_with('Z') && !s.contains('+'),
                "under TimeZone={tz} the stamp {f} must be rendered in UTC with a \
                 literal Z, the way every other listing in this repo renders one \
                 (025_log_timers.sql), got {s}"
            );
        }
    }
    c.execute("SET TimeZone = 'UTC'", &[])
        .await
        .map_err(|e| format!("reset TimeZone: {e}"))?;

    // A forever key has no expiry, and a NULL must stay a JSON null rather than
    // becoming the string to_char would make of nothing.
    let nsf = unique("kvtzf");
    put_many(
        c,
        DEFAULT_TENANT,
        &nsf,
        vec![json!({ "op": "put", "ns": nsf, "key": "f", "value": 1, "forever": true })],
    )
    .await?;
    let f = list(c, &nsf, "", None, None).await?;
    let frow = rows_of(&f).first().cloned().unwrap_or(Value::Null);
    chk!(
        frow.get("expiresAt") == Some(&Value::Null),
        "a forever key must stay expiresAt:null through the UTC render: {frow}"
    );
    Ok(())
}

// ===========================================================================
// THE NAMESPACE LISTING: counts, order, and nothing from another tenant.
// ===========================================================================
async fn case_namespaces_count_and_order(c: &Client) -> Case {
    let stem = unique("kvns");
    let a = format!("{stem}.a");
    let b = format!("{stem}.b");
    put_many(c, DEFAULT_TENANT, &a, puts(&a, &["k1", "k2", "k3"], json!(1))).await?;
    put_many(c, DEFAULT_TENANT, &b, puts(&b, &["k1"], json!(1))).await?;

    let all = namespaces(c, DEFAULT_TENANT).await?;
    let list = all.as_array().cloned().unwrap_or_default();
    let find = |name: &str| {
        list.iter()
            .find(|e| e.get("namespace").and_then(|n| n.as_str()) == Some(name))
            .cloned()
    };
    chk!(
        find(&a).and_then(|e| e.get("keys").cloned()) == Some(json!(3)),
        "{a} must count 3 keys: {:?}",
        find(&a)
    );
    chk!(
        find(&b).and_then(|e| e.get("keys").cloned()) == Some(json!(1)),
        "{b} must count 1 key: {:?}",
        find(&b)
    );

    // Ordered by namespace, byte order, across the WHOLE answer — a selector
    // that is sorted only by accident is a selector that reorders itself the day
    // a row is written.
    let names: Vec<&str> = list
        .iter()
        .filter_map(|e| e.get("namespace").and_then(|n| n.as_str()))
        .collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    chk!(names == sorted, "the namespace list must be ordered: {names:?}");

    // A namespace exists if and only if a row exists: delete the last key and it
    // leaves the selector, with no registry to clean up.
    c.execute(
        "DELETE FROM queen.kv WHERE tenant_id = $1::text::uuid AND namespace = $2",
        &[&DEFAULT_TENANT, &b],
    )
    .await
    .map_err(|e| format!("delete: {e}"))?;
    let after = namespaces(c, DEFAULT_TENANT).await?;
    chk!(
        !after
            .as_array()
            .map(|l| l.iter().any(|e| e.get("namespace") == Some(&json!(b))))
            .unwrap_or(false),
        "an emptied namespace must leave the selector — nothing registers one"
    );
    Ok(())
}

// ===========================================================================
// ISOLATION — another tenant's keys are not visible, in either read.
//
// §13.1: the tenant is not a filter applied to an identifier the caller
// presented, it is PART OF THE PRIMARY KEY, and there is no RLS on this table —
// so per-tenant isolation is EXACTLY the WHERE clause inside these two
// procedures. That makes it a thing a test can check, and this is the test.
// ===========================================================================
async fn case_another_tenants_keys_are_invisible(c: &Client) -> Case {
    let shared = unique("kvshared");
    let theirs = unique("kvtheirs");
    put_many(c, DEFAULT_TENANT, &shared, puts(&shared, &["mine"], json!("mine"))).await?;
    put_many(c, OTHER_TENANT, &shared, puts(&shared, &["theirs"], json!("theirs"))).await?;
    put_many(c, OTHER_TENANT, &theirs, puts(&theirs, &["k"], json!(1))).await?;

    let page = list(c, &shared, "", None, None).await?;
    chk!(
        keys_of(&page) == vec!["mine"],
        "two tenants in the same namespace NAME are two different namespaces: {:?}",
        keys_of(&page)
    );

    let mine = namespaces(c, DEFAULT_TENANT).await?;
    let has = |v: &Value, name: &str| {
        v.as_array()
            .map(|l| l.iter().any(|e| e.get("namespace") == Some(&json!(name))))
            .unwrap_or(false)
    };
    chk!(
        has(&mine, &shared),
        "the selector must show a namespace this tenant does have"
    );
    chk!(
        !has(&mine, &theirs),
        "the selector must not leak a namespace that exists only for another tenant"
    );

    let ours = namespaces(c, OTHER_TENANT).await?;
    let their_shared = ours
        .as_array()
        .and_then(|l| l.iter().find(|e| e.get("namespace") == Some(&json!(shared))))
        .cloned()
        .ok_or("the other tenant cannot see its own namespace")?;
    chk!(
        their_shared.get("keys") == Some(&json!(1)),
        "each tenant counts only its own rows: {their_shared}"
    );
    Ok(())
}

// ===========================================================================
// THE NAMESPACE IS VALIDATED, THE PREFIX IS NOT.
//
// A namespace is registered nowhere, so an unvalidated typo does not fail — it
// mints a phantom namespace that reads empty forever and the operator concludes
// their data is gone. The charset check is therefore an error (22023 →
// HTTP 400). The PREFIX is free text and must NOT go through the key rules:
// empty is the console's ordinary case, and a prefix longer than the 512-byte
// key ceiling is simply a range that matches nothing.
// ===========================================================================
async fn case_the_namespace_is_validated_and_the_prefix_is_free(c: &Client) -> Case {
    match list_raw(c, DEFAULT_TENANT, "NOT A NAMESPACE", "", None, None, false, true).await {
        Ok(v) => return Err(format!("a malformed namespace must RAISE, got {v}")),
        Err(e) => {
            let db = e
                .as_db_error()
                .ok_or_else(|| format!("expected a database error, got {e}"))?;
            chk!(
                db.code().code() == "22023",
                "a malformed namespace must be 22023 (the handler's 400), got {}",
                db.code().code()
            );
            chk!(
                db.message() == "kv_bad_namespace",
                "the message is the stable identifier clients branch on, got {}",
                db.message()
            );
        }
    }

    // Free text: far over the key ceiling, and still just an empty page.
    let ns = unique("kvfree");
    put_many(c, DEFAULT_TENANT, &ns, puts(&ns, &["k"], json!(1))).await?;
    let long = "z".repeat(4096);
    let page = list(c, &ns, &long, None, None).await?;
    chk!(
        rows_of(&page).is_empty() && truncated(&page) == Some(false),
        "an over-long prefix is a range that matches nothing, not an error: {page}"
    );

    // A NULL tenant must fail loudly rather than read as "this tenant has no
    // keys" — direct SQL callers are part of the supported deployment model.
    let null_tenant: Option<&str> = None;
    let r = c
        .query_one(
            "SELECT (queen.kv_list_v1($1::text::uuid, $2, '', NULL, NULL, false, true))::text",
            &[&null_tenant, &ns],
        )
        .await;
    chk!(r.is_err(), "a NULL tenant must raise, not answer an empty page");
    Ok(())
}

// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn kv_console_list() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // Boot the real broker purely to apply the real schema: the SQL under test is
    // include_str!-embedded, so this is also what proves the file on disk is the
    // one the binary carries and that 024_kv.sql is in schema.rs's PROCEDURES
    // list. Without it every case below would fail with 42883.
    let _broker = Broker::start(
        BrokerConfig::new()
            .pg(host.clone(), port, "postgres", "postgres", "postgres")
            .pool_size(4),
    )
    .await
    .expect("broker start");

    let c = connect(&host, port).await;

    let mut report: Vec<(&str, Case)> = Vec::new();
    report.push((
        "empty_prefix_lists_the_whole_namespace",
        case_empty_prefix_lists_the_whole_namespace(&c).await,
    ));
    report.push(("prefix_is_a_tight_range", case_prefix_is_a_tight_range(&c).await));
    report.push((
        "the_cursor_is_exclusive_and_ends",
        case_the_cursor_is_exclusive_and_ends(&c).await,
    ));
    report.push((
        "the_limit_is_clamped_and_defaults",
        case_the_limit_is_clamped_and_defaults(&c).await,
    ));
    report.push((
        "the_byte_budget_ends_the_page",
        case_the_byte_budget_ends_the_page(&c).await,
    ));
    report.push((
        "expired_rows_are_visible_and_hidable",
        case_expired_rows_are_visible_and_hidable(&c).await,
    ));
    report.push((
        "namespaces_count_and_order",
        case_namespaces_count_and_order(&c).await,
    ));
    report.push((
        "another_tenants_keys_are_invisible",
        case_another_tenants_keys_are_invisible(&c).await,
    ));
    report.push((
        "the_namespace_is_validated_and_the_prefix_is_free",
        case_the_namespace_is_validated_and_the_prefix_is_free(&c).await,
    ));
    report.push((
        "stamps_are_utc_under_any_session_timezone",
        case_stamps_are_utc_under_any_session_timezone(&c).await,
    ));

    println!("\n============ KV console list (PLAN_DASHBOARD_ACTIONS §2.5) ============");
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
    println!(
        "======================== {}/{} passed ========================\n",
        report.len() - failed,
        report.len()
    );
    assert_eq!(failed, 0, "{failed} KV console case(s) failed — see the table above");
}
