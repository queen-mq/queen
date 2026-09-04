//! `queen.log_partitions_changed_v1` — the contract of PLAN_S3_SINK.md §5.1/§5.2,
//! tested against a real Postgres.
//!
//! WHY THIS LEVEL. Every rule under test lives in
//! `033_log_partitions_changed.sql`: the two orderings and their indexes, the
//! cursor, the tenancy arm, the clamps and the whole of `safeTime`. The handler
//! above it composes nothing — it transposes a body into four arrays and
//! returns the function's `jsonb` verbatim — so a test that went through HTTP
//! would prove the transposition (already pinned without a database in
//! `src/tests_unit/partitions_changed.rs`) and prove nothing about the rules.
//! These call the function directly, on the same connection kind the broker
//! uses.
//!
//! THE ONE THAT MATTERS MOST is `a_bump_during_a_sweep_is_seen_twice_never_missed`.
//! The entire discovery design rests on `last_write_at` moving only forward, so
//! that a partition written to mid-sweep re-appears on a later page instead of
//! slipping behind the cursor. Seen twice costs a re-read; missed is a
//! partition a sink never mirrors, silently, for ever. The case reproduces the
//! race deterministically by bumping a row that has already been paged past.
//!
//! `safeTime` gets three: it is pinned to an open transaction's `xact_start`
//! (with the guard set to zero, so the pin is exact rather than approximate),
//! it is released when that transaction commits, and it degrades to the floor
//! for a role that cannot read `pg_stat_activity` across roles. That last case
//! creates a second, non-superuser role and calls the function AS it — the only
//! way to observe the masking of F14, and the fallback it triggers is the one
//! arm of this function where being wrong loses data.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test` —
//! the convention `kv_semantics.rs`, `hotlist_repairs.rs` and
//! `log_fetch_timezone.rs` already follow, including booting the real broker
//! rather than running psql: the SQL is `include_str!`-embedded AND has to be
//! registered in `schema.rs`'s PROCEDURES list, so a broker boot is the only
//! thing that proves the file on disk is the one the binary carries and that it
//! is applied at all (a file missing from that list fails every case with
//! 42883 rather than passing silently).
//!
//! ```bash
//! docker run --rm -d --name queen-s3-p0-pg -e POSTGRES_PASSWORD=postgres -p 5470:5432 postgres:16
//! QUEEN_EMBEDDED_TEST_PG=localhost:5470 cargo test --test partitions_changed -- --ignored --nocapture
//! ```
//!
//! ONE test function on purpose, same reason `kv_semantics.rs` states: a Broker
//! is booted to apply the real schema and the admission arbiter is
//! process-global. The cases are still reported one by one — each returns
//! `Result<(), String>` and the runner prints a PASS/FAIL line per case before
//! failing — so a red run names every broken rule instead of only the first.

use queen::{Broker, BrokerConfig};
use serde_json::Value;
use tokio_postgres::Client;

/// The default tenant every other test in this repo uses.
const TENANT: &str = "00000000-0000-0000-0000-000000000001";
/// A second tenant, which must see the first one's queue as absent.
const OTHER_TENANT: &str = "00000000-0000-0000-0000-0000000000ff";

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

async fn connect_as(host: &str, port: u16, user: &str, password: &str) -> Client {
    let (c, conn) = tokio_postgres::connect(
        &format!("host={host} port={port} user={user} password={password} dbname=postgres"),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

/// `tokio_postgres::Error`'s own Display is just "db error"; the SQLSTATE and
/// the server's message live in its source, and that sentence is the diagnosis.
fn detail(e: &tokio_postgres::Error) -> String {
    use std::error::Error;
    match e.source() {
        Some(s) => format!("{e}: {s}"),
        None => e.to_string(),
    }
}

/// One call, exactly as `db::log_partitions_changed` makes it: `since` bound as
/// TEXT and cast, the tenant bound as TEXT and cast, everything else native.
///
/// Eight arguments because the function under test takes seven and every one of
/// them is exercised by some case — collapsing them into a struct would put a
/// layer between the test and the signature it exists to pin.
#[allow(clippy::too_many_arguments)]
async fn call(
    c: &Client,
    queues: &[&str],
    since: &[Option<&str>],
    after: &[Option<&str>],
    limits: &[i32],
    guard_s: i32,
    floor_s: i32,
    tenant: &str,
) -> Result<Value, String> {
    let queues: Vec<String> = queues.iter().map(|s| s.to_string()).collect();
    let since: Vec<Option<String>> = since.iter().map(|s| s.map(str::to_string)).collect();
    let after: Vec<Option<String>> = after.iter().map(|s| s.map(str::to_string)).collect();
    let limits = limits.to_vec();
    let row = c
        .query_one(
            "SELECT queen.log_partitions_changed_v1(\
                 $1,$2::text[]::timestamptz[],$3,$4,$5::int,$6::int,$7::text::uuid)::text",
            &[
                &queues, &since, &after, &limits, &guard_s, &floor_s, &tenant,
            ],
        )
        .await
        .map_err(|e| format!("log_partitions_changed_v1: {}", detail(&e)))?;
    let txt: String = row.get(0);
    serde_json::from_str(&txt).map_err(|e| format!("answer is not JSON ({e}): {txt}"))
}

/// The one-queue shorthand every paging case uses.
async fn page(
    c: &Client,
    queue: &str,
    since: Option<&str>,
    after: Option<&str>,
    limit: i32,
) -> Result<Value, String> {
    let v = call(c, &[queue], &[since], &[after], &[limit], 5, 30, TENANT).await?;
    Ok(v["entries"][0].clone())
}

fn names(entry: &Value) -> Vec<String> {
    entry["partitions"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|p| p["name"].as_str().unwrap_or("<?>").to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn next_of(entry: &Value) -> Option<String> {
    entry["next"].as_str().map(str::to_string)
}

/// Seed one queue with `n` partitions, one second apart, oldest first.
/// `p-000`.. so name order and time order agree, which is what lets a case that
/// mixes the two modes assert on one list.
async fn seed(c: &Client, queue: &str, n: usize, tenant: &str) -> Result<(), String> {
    c.batch_execute(&format!(
        "DELETE FROM queen.queues WHERE name = '{queue}';
         INSERT INTO queen.queues (name, tenant_id) VALUES ('{queue}', '{tenant}');
         INSERT INTO queen.log_partitions
             (queue_id, name, last_offset, log_start, last_write_at)
         SELECT q.id, 'p-' || lpad(i::text, 3, '0'), i * 10, i,
                TIMESTAMPTZ '2026-09-04 10:00:00+00' + make_interval(secs => i)
         FROM queen.queues q, generate_series(0, {}) i
         WHERE q.name = '{queue}' AND q.tenant_id = '{tenant}';",
        n - 1
    ))
    .await
    .map_err(|e| format!("seed {queue}: {}", detail(&e)))?;
    Ok(())
}

// ===========================================================================
// Enumeration and paging

async fn case_full_enumeration_pages_by_name(c: &Client) -> Case {
    let q = unique("s3disc-enum-");
    seed(c, &q, 7, TENANT).await?;

    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let e = page(c, &q, None, cursor.as_deref(), 3).await?;
        pages += 1;
        chk!(
            pages <= 10,
            "enumeration did not terminate after {pages} pages"
        );
        let got = names(&e);
        chk!(
            got.len() <= 3,
            "page {pages} returned {} partitions for limit 3",
            got.len()
        );
        seen.extend(got);
        match next_of(&e) {
            Some(n) => cursor = Some(n),
            None => break,
        }
    }

    let want: Vec<String> = (0..7).map(|i| format!("p-{i:03}")).collect();
    chk!(
        seen == want,
        "enumeration must return every partition once, in name order: got {seen:?}"
    );
    // 7 partitions at 3 a page is 3 + 3 + 1: the third page is short, so it
    // carries a null `next` and ends the sweep without a fourth round trip.
    chk!(
        pages == 3,
        "7 partitions at limit 3 should take 3 pages, took {pages}"
    );
    Ok(())
}

async fn case_a_filled_page_always_offers_a_cursor(c: &Client) -> Case {
    let q = unique("s3disc-exact-");
    seed(c, &q, 4, TENANT).await?;

    // Exactly `limit` rows left: the page FILLED, so `next` is non-null even
    // though there is nothing after it. The caller pays one empty round trip
    // and learns the sweep is over; the alternative (a look-ahead row) buys
    // nothing it can act on.
    let e = page(c, &q, None, None, 4).await?;
    chk!(names(&e).len() == 4, "expected the whole queue in one page");
    let cursor = next_of(&e).ok_or("a filled page must carry a cursor")?;

    let e = page(c, &q, None, Some(&cursor), 4).await?;
    chk!(
        names(&e).is_empty(),
        "the page after the last one must be empty, got {:?}",
        names(&e)
    );
    chk!(
        next_of(&e).is_none(),
        "an empty page ends the sweep with a null next"
    );
    Ok(())
}

async fn case_since_returns_only_what_moved(c: &Client) -> Case {
    let q = unique("s3disc-since-");
    seed(c, &q, 7, TENANT).await?;

    // p-000 is stamped 10:00:00 and each later one a second on, so a `since` of
    // 10:00:04 must return exactly the last three — and INCLUSIVELY, because a
    // partition whose quantized second equals the caller's watermark may have
    // been written after the caller read it.
    let e = page(c, &q, Some("2026-09-04T10:00:04.000000Z"), None, 1000).await?;
    chk!(
        names(&e) == vec!["p-004", "p-005", "p-006"],
        "since must be inclusive and ordered by (last_write_at, name): got {:?}",
        names(&e)
    );
    chk!(next_of(&e).is_none(), "a short page ends the sweep");

    // A `since` past everything is the caught-up sweep: empty, no cursor, no
    // error. This is the answer a sink gets on most polls.
    let e = page(c, &q, Some("2026-09-04T11:00:00.000000Z"), None, 1000).await?;
    chk!(names(&e).is_empty(), "nothing moved: {:?}", names(&e));
    chk!(next_of(&e).is_none(), "and nothing to page");
    chk!(e["error"].is_null(), "caught up is not an error: {e}");
    Ok(())
}

/// THE ONE THAT MATTERS. A partition written to while a sweep is paging must
/// re-appear on a later page, never vanish between two of them.
async fn case_a_bump_during_a_sweep_is_seen_twice_never_missed(c: &Client) -> Case {
    let q = unique("s3disc-bump-");
    seed(c, &q, 6, TENANT).await?;
    let since = "2026-09-04T10:00:00.000000Z";

    // Page one: the two oldest.
    let e = page(c, &q, Some(since), None, 2).await?;
    chk!(
        names(&e) == vec!["p-000", "p-001"],
        "page 1: {:?}",
        names(&e)
    );
    let mut cursor = next_of(&e).ok_or("page 1 must carry a cursor")?;
    let mut seen = names(&e);

    // Now p-000 — already paged past — is written to, exactly as a producer
    // would: its last_write_at jumps to the newest instant in the queue.
    c.execute(
        "UPDATE queen.log_partitions p
            SET last_write_at = TIMESTAMPTZ '2026-09-04 10:00:59+00',
                last_offset = last_offset + 1
          FROM queen.queues q
         WHERE q.id = p.queue_id AND q.name = $1 AND p.name = 'p-000'",
        &[&q],
    )
    .await
    .map_err(|e| format!("bump p-000: {}", detail(&e)))?;

    // Finish the sweep.
    for guard in 0..10 {
        chk!(guard < 9, "the sweep did not terminate");
        let e = page(c, &q, Some(since), Some(&cursor), 2).await?;
        seen.extend(names(&e));
        match next_of(&e) {
            Some(n) => cursor = n,
            None => break,
        }
    }

    // NOT MISSED: every partition of the queue appears at least once...
    for i in 0..6 {
        let want = format!("p-{i:03}");
        chk!(
            seen.contains(&want),
            "{want} was MISSED by the sweep (this is the failure that silently \
             loses a partition for ever): saw {seen:?}"
        );
    }
    // ...and SEEN TWICE is the price, stated rather than hidden: the bumped row
    // is returned again at its new position.
    chk!(
        seen.iter().filter(|n| *n == "p-000").count() == 2,
        "the bumped partition must re-appear at its new position: {seen:?}"
    );
    Ok(())
}

async fn case_the_cursor_is_mode_tagged(c: &Client) -> Case {
    let q = unique("s3disc-cursor-");
    seed(c, &q, 4, TENANT).await?;

    // An enumeration cursor replayed against a `since` sweep is refused, not
    // silently restarted: the quiet version loops a paging caller for ever on
    // its own first page.
    let e = page(c, &q, None, None, 2).await?;
    let enum_cursor = next_of(&e).ok_or("an enumeration cursor")?;
    chk!(
        enum_cursor.starts_with("n|"),
        "enumeration cursor shape: {enum_cursor}"
    );
    let e = page(
        c,
        &q,
        Some("2026-09-04T10:00:00.000000Z"),
        Some(&enum_cursor),
        2,
    )
    .await?;
    chk!(
        e["error"] == "BAD_CURSOR",
        "an enumeration cursor in a since sweep must be BAD_CURSOR: {e}"
    );

    // ...and the reverse.
    let e = page(c, &q, Some("2026-09-04T10:00:00.000000Z"), None, 2).await?;
    let since_cursor = next_of(&e).ok_or("a since cursor")?;
    chk!(
        since_cursor.starts_with("t|"),
        "since cursor shape: {since_cursor}"
    );
    let e = page(c, &q, None, Some(&since_cursor), 2).await?;
    chk!(
        e["error"] == "BAD_CURSOR",
        "a since cursor in an enumeration must be BAD_CURSOR: {e}"
    );

    // Junk in either mode is the same verdict, and an EMPTY cursor is "start
    // from the beginning" rather than junk (a client that stores "" for absent
    // must not be punished for it).
    for bad in ["", "nonsense", "t|notanumber|p", "t|", "n"] {
        let e = page(c, &q, None, Some(bad), 2).await?;
        if bad.is_empty() {
            chk!(
                names(&e) == vec!["p-000", "p-001"],
                "an empty cursor starts from the beginning: {e}"
            );
        } else {
            chk!(
                e["error"] == "BAD_CURSOR",
                "cursor {bad:?} must be refused: {e}"
            );
        }
    }
    Ok(())
}

async fn case_a_name_with_a_separator_round_trips(c: &Client) -> Case {
    // The cursor's name is the whole remainder after the tag, so a partition
    // name containing the separator must page correctly rather than truncating
    // to its first segment — which would restart the sweep on every page.
    let q = unique("s3disc-pipe-");
    c.batch_execute(&format!(
        "DELETE FROM queen.queues WHERE name = '{q}';
         INSERT INTO queen.queues (name, tenant_id) VALUES ('{q}', '{TENANT}');
         INSERT INTO queen.log_partitions (queue_id, name, last_offset, log_start, last_write_at)
         SELECT q.id, n, 1, 0, TIMESTAMPTZ '2026-09-04 10:00:00+00'
         FROM queen.queues q, unnest(ARRAY['a|b', 'a|c', 'z']) n
         WHERE q.name = '{q}' AND q.tenant_id = '{TENANT}';"
    ))
    .await
    .map_err(|e| format!("seed {q}: {}", detail(&e)))?;

    let e = page(c, &q, None, None, 1).await?;
    chk!(names(&e) == vec!["a|b"], "page 1: {:?}", names(&e));
    let cursor = next_of(&e).ok_or("a cursor")?;
    let e = page(c, &q, None, Some(&cursor), 1).await?;
    chk!(
        names(&e) == vec!["a|c"],
        "a name containing the separator must not truncate the cursor: {:?}",
        names(&e)
    );

    // ...and in since mode, where the name sits after TWO separators.
    let e = page(c, &q, Some("2026-09-04T10:00:00.000000Z"), None, 1).await?;
    let cursor = next_of(&e).ok_or("a since cursor")?;
    let e = page(c, &q, Some("2026-09-04T10:00:00.000000Z"), Some(&cursor), 1).await?;
    chk!(names(&e) == vec!["a|c"], "since mode: {:?}", names(&e));
    Ok(())
}

// ===========================================================================
// Ceilings, shape and tenancy

async fn case_the_limit_is_clamped_not_rejected(c: &Client) -> Case {
    let q = unique("s3disc-clamp-");
    seed(c, &q, 5, TENANT).await?;

    // Zero and negative clamp UP to one: a limit of zero would be an endpoint
    // that answers nothing for ever, and a caller cannot tell the difference
    // between that and a caught-up queue.
    for lim in [0, -1, i32::MIN] {
        let e = page(c, &q, None, None, lim).await?;
        chk!(
            names(&e).len() == 1,
            "limit {lim} must clamp to 1, got {}",
            names(&e).len()
        );
    }
    // Over the ceiling clamps DOWN, and does not error.
    let e = page(c, &q, None, None, i32::MAX).await?;
    chk!(names(&e).len() == 5, "limit MAX must serve the whole queue");
    Ok(())
}

async fn case_unknown_and_foreign_queues_are_byte_identical(c: &Client) -> Case {
    let q = unique("s3disc-tenancy-");
    seed(c, &q, 3, TENANT).await?;

    // The queue exists — for somebody else.
    let foreign = call(c, &[&q], &[None], &[None], &[10], 5, 30, OTHER_TENANT).await?;
    // The queue exists nowhere.
    let ghost = call(
        c,
        &["s3disc-no-such-queue"],
        &[None],
        &[None],
        &[10],
        5,
        30,
        OTHER_TENANT,
    )
    .await?;

    let mut a = foreign["entries"][0].clone();
    let mut b = ghost["entries"][0].clone();
    // The queue NAME is the caller's own echo and is necessarily different; it
    // is the rest of the entry that must be indistinguishable.
    chk!(
        a["queue"] == q.as_str(),
        "the entry echoes the queue asked for"
    );
    a["queue"] = Value::Null;
    b["queue"] = Value::Null;
    chk!(
        a == b,
        "a queue owned by another tenant must be byte-identical to one that does \
         not exist, or this endpoint probes another tenant's namespace.\n  \
         foreign: {a}\n  ghost:   {b}"
    );
    chk!(
        a["error"] == "UNKNOWN_TOPIC_OR_PARTITION",
        "and the marker is the fetch path's: {a}"
    );
    chk!(
        a["partitions"].is_null() && a["next"].is_null(),
        "an error entry carries nothing else: {a}"
    );

    // ...and the owning tenant still sees it, so the case is about tenancy and
    // not about a queue that failed to seed.
    let mine = call(c, &[&q], &[None], &[None], &[10], 5, 30, TENANT).await?;
    chk!(
        mine["entries"][0]["partitions"].as_array().map(|a| a.len()) == Some(3),
        "the owning tenant sees its own queue: {}",
        mine["entries"][0]
    );
    Ok(())
}

async fn case_entries_answer_in_request_order(c: &Client) -> Case {
    let a = unique("s3disc-order-a-");
    let b = unique("s3disc-order-b-");
    seed(c, &a, 2, TENANT).await?;
    seed(c, &b, 3, TENANT).await?;

    // Index alignment is what a caller demuxes on: a misordered answer would
    // hand one queue's partitions under another queue's name.
    let v = call(
        c,
        &[&b, "s3disc-nope", &a],
        &[None, None, None],
        &[None, None, None],
        &[10, 10, 10],
        5,
        30,
        TENANT,
    )
    .await?;
    let e = v["entries"].as_array().ok_or("entries must be an array")?;
    chk!(e.len() == 3, "one entry per request entry, got {}", e.len());
    chk!(e[0]["queue"] == b.as_str(), "entry 0: {}", e[0]);
    chk!(
        e[1]["error"] == "UNKNOWN_TOPIC_OR_PARTITION",
        "entry 1: {}",
        e[1]
    );
    chk!(e[2]["queue"] == a.as_str(), "entry 2: {}", e[2]);
    chk!(
        e[0]["partitions"].as_array().map(|x| x.len()) == Some(3),
        "b has 3"
    );
    chk!(
        e[2]["partitions"].as_array().map(|x| x.len()) == Some(2),
        "a has 2"
    );

    // An empty batch is legal and answers the watermark alone — how a reader
    // whose queues are all idle closes the window it is holding.
    let v = call(c, &[], &[], &[], &[], 5, 30, TENANT).await?;
    chk!(
        v["entries"].as_array().map(|a| a.is_empty()) == Some(true),
        "an empty batch answers no entries: {v}"
    );
    chk!(v["safeTime"].is_string(), "...but still a safeTime: {v}");
    Ok(())
}

async fn case_misaligned_arrays_raise(c: &Client) -> Case {
    // A short companion array would let multi-array unnest pad with NULLs and
    // answer one queue's partitions under another's name. Loud is the only
    // safe direction, and the broker never sends this shape — `bind` builds all
    // four together.
    let queues = vec!["a".to_string(), "b".to_string()];
    let since: Vec<Option<String>> = vec![None];
    let after: Vec<Option<String>> = vec![None, None];
    let limits = vec![10i32, 10];
    let err = c
        .query_one(
            "SELECT queen.log_partitions_changed_v1(\
                 $1,$2::text[]::timestamptz[],$3,$4,$5::int,$6::int,$7::text::uuid)::text",
            &[&queues, &since, &after, &limits, &0i32, &30i32, &TENANT],
        )
        .await
        .err()
        .ok_or("a misaligned call must raise")?;
    let msg = detail(&err);
    chk!(msg.contains("QPARTS"), "the raise must name QPARTS: {msg}");
    Ok(())
}

async fn case_every_timestamp_is_six_digits_and_a_z(c: &Client) -> Case {
    let q = unique("s3disc-stamp-");
    seed(c, &q, 2, TENANT).await?;
    let v = call(c, &[&q], &[None], &[None], &[10], 5, 30, TENANT).await?;

    let mut stamps: Vec<String> = vec![v["safeTime"]
        .as_str()
        .ok_or_else(|| format!("safeTime must be a string: {v}"))?
        .to_string()];
    for p in v["entries"][0]["partitions"].as_array().unwrap_or(&vec![]) {
        stamps.push(
            p["lastWriteAt"]
                .as_str()
                .ok_or_else(|| format!("lastWriteAt must be a string: {p}"))?
                .to_string(),
        );
    }
    chk!(stamps.len() == 3, "one safeTime and two lastWriteAt");
    for s in &stamps {
        // The same format string 032_log_fetch renders a record's `ts` with. A
        // reader compares the two directly, so a precision or suffix that
        // drifted on one side makes every comparison between them wrong.
        chk!(
            s.len() == 27 && s.ends_with('Z'),
            "{s} is not YYYY-MM-DDTHH:MM:SS.ffffffZ"
        );
        let frac = s.rsplit_once('.').map(|x| x.1).unwrap_or("");
        chk!(
            frac.len() == 7,
            "{s} must carry exactly six fractional digits"
        );
    }
    // ...and the seeded instants render as the UTC they were written in,
    // whatever the session timezone is (the repo-wide pin of
    // server/tests/procedures_timezone.rs, exercised here on this file's own
    // two sites).
    for tz in ["UTC", "Asia/Tokyo", "America/Los_Angeles"] {
        c.batch_execute(&format!("SET TIME ZONE '{tz}'"))
            .await
            .map_err(|e| format!("SET TIME ZONE {tz}: {}", detail(&e)))?;
        let v = call(c, &[&q], &[None], &[None], &[10], 5, 30, TENANT).await?;
        let got = v["entries"][0]["partitions"][0]["lastWriteAt"]
            .as_str()
            .unwrap_or("<absent>")
            .to_string();
        chk!(
            got == "2026-09-04T10:00:00.000000Z",
            "under TimeZone={tz} lastWriteAt was {got:?} — AT TIME ZONE 'UTC' is missing"
        );
    }
    c.batch_execute("SET TIME ZONE 'UTC'")
        .await
        .map_err(|e| detail(&e))?;
    Ok(())
}

// ===========================================================================
// safeTime

async fn case_an_open_transaction_pins_safe_time(host: &str, port: u16, c: &Client) -> Case {
    let other = connect_as(host, port, "postgres", "postgres").await;
    other
        .batch_execute("BEGIN")
        .await
        .map_err(|e| format!("BEGIN: {}", detail(&e)))?;
    // A statement inside it, so the transaction really is in progress rather
    // than a BEGIN the server has not processed yet.
    let xact_start: String = other
        .query_one(
            "SELECT to_char(xact_start AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')
               FROM pg_stat_activity WHERE pid = pg_backend_pid()",
            &[],
        )
        .await
        .map_err(|e| format!("xact_start: {}", detail(&e)))?
        .get(0);

    // GUARD ZERO, so the pin is EXACT: with a guard the answer would be
    // `now() - guard` for a transaction younger than it, and the case would be
    // asserting the guard instead of the pin.
    let v = call(c, &[], &[], &[], &[], 0, 30, TENANT).await?;
    let safe = v["safeTime"].as_str().unwrap_or_default().to_string();
    chk!(
        !v["safeTimeDegraded"].as_bool().unwrap_or(true),
        "the superuser this test connects as reads pg_stat_activity unmasked: {v}"
    );
    chk!(
        safe == xact_start,
        "safeTime must be pinned to the oldest open transaction's xact_start.\n  \
         safeTime:   {safe}\n  xact_start: {xact_start}"
    );

    // The guard is real, and it only ever moves the answer BACKWARDS.
    let guarded = call(c, &[], &[], &[], &[], 5, 30, TENANT).await?;
    let guarded = guarded["safeTime"].as_str().unwrap_or_default().to_string();
    chk!(
        guarded <= safe,
        "the guard may only move safeTime earlier: guarded {guarded} vs {safe}"
    );

    // Release it: safeTime must move forward past the transaction that pinned it.
    other
        .batch_execute("COMMIT")
        .await
        .map_err(|e| format!("COMMIT: {}", detail(&e)))?;
    let v = call(c, &[], &[], &[], &[], 0, 30, TENANT).await?;
    let after = v["safeTime"].as_str().unwrap_or_default().to_string();
    chk!(
        after > safe,
        "a committed transaction must stop pinning safeTime: still {after} (was {safe})"
    );
    Ok(())
}

async fn case_an_idle_session_does_not_pin(host: &str, port: u16, c: &Client) -> Case {
    // An idle session has xact_start NULL and must not hold the watermark back
    // — a pool of them is the normal state of a broker, and a design that let
    // them pin would never advance at all.
    let idle = connect_as(host, port, "postgres", "postgres").await;
    idle.batch_execute("SELECT 1")
        .await
        .map_err(|e| detail(&e))?;
    let before: String = c
        .query_one(
            "SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')",
            &[],
        )
        .await
        .map_err(|e| detail(&e))?
        .get(0);
    let v = call(c, &[], &[], &[], &[], 0, 30, TENANT).await?;
    let safe = v["safeTime"].as_str().unwrap_or_default().to_string();
    chk!(
        safe >= before,
        "an idle session must not pin safeTime: {safe} < {before}"
    );
    drop(idle);
    Ok(())
}

async fn case_a_masked_role_degrades_to_the_floor(host: &str, port: u16, c: &Client) -> Case {
    // F14, and the arm where being wrong loses data. PostgreSQL masks
    // pg_stat_activity across roles: for a session owned by a role the reader
    // is not a member of, `state`, `xact_start` AND — measured on PG 16 — even
    // `backend_type` come back NULL. min(xact_start) over the visible rows is
    // then an OVERESTIMATE of the safe instant, so the function must refuse to
    // answer it and fall back to the floor.
    //
    // MEASURED CONSEQUENCE, worth stating where the test is: PostgreSQL's own
    // background workers are owned by the bootstrap superuser and are masked
    // too, so for ANY non-superuser role without pg_read_all_stats the degrade
    // is permanent, not occasional. "One role per cell" is not sufficient on
    // its own — the grant is.
    let role = unique("s3disc_reader_");
    c.batch_execute(&format!(
        "DROP ROLE IF EXISTS {role};
         CREATE ROLE {role} LOGIN PASSWORD 'probe';
         GRANT USAGE ON SCHEMA queen TO {role};"
    ))
    .await
    .map_err(|e| format!("create probe role: {}", detail(&e)))?;

    let probe = connect_as(host, port, &role, "probe").await;
    // A distinctive floor, so the assertion cannot pass on a coincidence.
    let v = call(&probe, &[], &[], &[], &[], 0, 17, TENANT).await?;
    let degraded = v["safeTimeDegraded"].as_bool().unwrap_or(false);
    chk!(
        degraded,
        "a role that cannot read pg_stat_activity across roles MUST degrade: {v}"
    );
    // ...to now() - floor, which the same connection can compute.
    let ok: bool = probe
        .query_one(
            // `::text::timestamptz` and not `::timestamptz`: the bare cast makes
            // PostgreSQL infer $1 as timestamptz, and this crate binds the
            // rendered watermark as the &str it is.
            "SELECT $1::text::timestamptz BETWEEN now() - interval '19 seconds'
                                             AND now() - interval '15 seconds'",
            &[&v["safeTime"].as_str().unwrap_or_default()],
        )
        .await
        .map_err(|e| format!("floor check: {}", detail(&e)))?
        .get(0);
    chk!(
        ok,
        "the degraded answer must be now() - 17s, got {}",
        v["safeTime"]
    );

    // And the un-degraded arm is reachable again with the grant — the operator
    // instruction the deploy page carries.
    c.batch_execute(&format!("GRANT pg_read_all_stats TO {role}"))
        .await
        .map_err(|e| format!("grant: {}", detail(&e)))?;
    let granted = connect_as(host, port, &role, "probe").await;
    let v = call(&granted, &[], &[], &[], &[], 0, 17, TENANT).await?;
    chk!(
        !v["safeTimeDegraded"].as_bool().unwrap_or(true),
        "pg_read_all_stats must lift the degrade: {v}"
    );
    drop(probe);
    drop(granted);
    // Best effort: the role is per-run and unique, so a leftover is harmless.
    let _ = c
        .batch_execute(&format!("DROP ROLE IF EXISTS {role}"))
        .await;
    Ok(())
}

// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn partitions_changed() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // Background loops off: nothing here reads what they write, and a retention
    // or stats transaction would pin safeTime under the cases that assert on it.
    let _broker = Broker::start(
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

    let c = connect_as(&host, port, "postgres", "postgres").await;
    c.batch_execute("SET TIME ZONE 'UTC'")
        .await
        .expect("session tz");

    let mut report: Vec<(&str, Case)> = Vec::new();
    report.push((
        "full_enumeration_pages_by_name",
        case_full_enumeration_pages_by_name(&c).await,
    ));
    report.push((
        "a_filled_page_always_offers_a_cursor",
        case_a_filled_page_always_offers_a_cursor(&c).await,
    ));
    report.push((
        "since_returns_only_what_moved",
        case_since_returns_only_what_moved(&c).await,
    ));
    report.push((
        "a_bump_during_a_sweep_is_seen_twice_never_missed",
        case_a_bump_during_a_sweep_is_seen_twice_never_missed(&c).await,
    ));
    report.push((
        "the_cursor_is_mode_tagged",
        case_the_cursor_is_mode_tagged(&c).await,
    ));
    report.push((
        "a_name_with_a_separator_round_trips",
        case_a_name_with_a_separator_round_trips(&c).await,
    ));
    report.push((
        "the_limit_is_clamped_not_rejected",
        case_the_limit_is_clamped_not_rejected(&c).await,
    ));
    report.push((
        "unknown_and_foreign_queues_are_byte_identical",
        case_unknown_and_foreign_queues_are_byte_identical(&c).await,
    ));
    report.push((
        "entries_answer_in_request_order",
        case_entries_answer_in_request_order(&c).await,
    ));
    report.push((
        "misaligned_arrays_raise",
        case_misaligned_arrays_raise(&c).await,
    ));
    report.push((
        "every_timestamp_is_six_digits_and_a_z",
        case_every_timestamp_is_six_digits_and_a_z(&c).await,
    ));
    report.push((
        "an_open_transaction_pins_safe_time",
        case_an_open_transaction_pins_safe_time(&host, port, &c).await,
    ));
    report.push((
        "an_idle_session_does_not_pin",
        case_an_idle_session_does_not_pin(&host, port, &c).await,
    ));
    report.push((
        "a_masked_role_degrades_to_the_floor",
        case_a_masked_role_degrades_to_the_floor(&host, port, &c).await,
    ));

    println!("\n============= partition discovery (PLAN_S3_SINK §5.1/§5.2) =============");
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
        "========================== {}/{} passed ==========================\n",
        report.len() - failed,
        report.len()
    );
    assert_eq!(
        failed, 0,
        "{failed} discovery case(s) failed — see the table above"
    );
}
