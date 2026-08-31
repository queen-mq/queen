//! THE SMART MIRROR — Kafka consumer groups in Queen's own consumer-group
//! views, tested against a real Postgres.
//!
//! WHY THIS EXISTS. A Kafka consumer group's committed offsets do not live in
//! `queen.log_consumers`. The facade writes them to `queen.kv` under
//! `qk:group:<esc group>:<esc topic>:<partition>` (protocols/queen-kafka/src/offsets.rs —
//! "offsets and existence are Queen's, liveness is this process's"), so before
//! `queen.get_consumer_groups_v4` grew its `kafka_base` CTE a group that a real
//! Kafka client was actively draining was INVISIBLE in `/api/v1/consumer-groups`
//! and the operator's only lag number was the one Kafka's own admin API gave.
//! The mirror is READ ONLY: KV stays the single source of truth for a Kafka
//! offset, and nothing in this file or in the SQL it tests ever writes one.
//!
//! WHY THIS LEVEL. Every rule under test is written in SQL, once, inside
//! `010_log_admin.sql`, and three HTTP routes plus the embedded broker inherit
//! it without a per-surface copy. So the tests call the functions directly, on
//! the connection kind the broker uses — the same argument `kv_semantics.rs`
//! makes for calling `kv_apply_v1` rather than going through a route.
//!
//! ISOLATION IS BY TENANT, NOT BY NAME. `get_consumer_groups_v4` answers for a
//! whole tenant, so two cases sharing one tenant would see each other's groups.
//! Each case therefore allocates its own tenant uuid, which is exactly the
//! parameter the function already takes.
//!
//! THE ONE THING NOT PROVEN HERE is that the facade writes the key shape these
//! cases seed. That is not provable from SQL and is not faked: it is proven by a
//! real Kafka client against a running broker + facade, and the transcript is in
//! this track's REPORT.md.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`,
//! same convention as `kv_semantics` and `embedded_smoke`:
//!
//! ```bash
//! docker run --rm -d --name qkc-t1-pg -e POSTGRES_PASSWORD=postgres -p 33000:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:33000 cargo test --test kafka_group_mirror -- --ignored --nocapture
//! ```
//!
//! ONE test function on purpose (a Broker is booted to apply the real,
//! `include_str!`-embedded schema — which is also what proves a `.sql` edit was
//! actually rebuilt). The cases are still reported one by one: each returns
//! `Result<(), String>` and the runner prints a PASS/FAIL line per case before
//! failing, so a red run names every broken rule instead of only the first.

use queen::{Broker, BrokerConfig};
use serde_json::{json, Value};
use tokio_postgres::Client;

type Case = Result<(), String>;

macro_rules! chk {
    ($cond:expr, $($arg:tt)*) => {
        if !($cond) { return Err(format!($($arg)*)); }
    };
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
/// a red suite into identical lines. Unwrap the SQLSTATE and message — a
/// collation mismatch (42P21/42P22) in particular is invisible without it.
fn pg_err(e: tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!(
            "{} {}{}",
            db.code().code(),
            db.message(),
            db.detail()
                .map(|d| format!(" | DETAIL: {d}"))
                .unwrap_or_default()
        ),
        None => format!("{e}"),
    }
}

// ------------------------------------------------------------------ fixtures

/// A fresh tenant per case. `gen_random_uuid()` rather than a Rust uuid so the
/// value is the database's own type from the start.
async fn new_tenant(c: &Client) -> Result<String, String> {
    c.query_one("SELECT gen_random_uuid()::text", &[])
        .await
        .map(|r| r.get::<_, String>(0))
        .map_err(pg_err)
}

async fn seed_queue(c: &Client, tenant: &str, name: &str) -> Result<String, String> {
    c.query_one(
        "INSERT INTO queen.queues (name, tenant_id) VALUES ($1, $2::text::uuid) RETURNING id::text",
        &[&name, &tenant],
    )
    .await
    .map(|r| r.get::<_, String>(0))
    .map_err(pg_err)
}

/// `last_offset` is the HIGHEST ALLOCATED offset (001_log_schema.sql: "allocator;
/// next base = last_offset+1"), so N pushed records means `last_offset = N - 1`.
async fn seed_partition(
    c: &Client,
    queue_id: &str,
    name: &str,
    last_offset: i64,
    log_start: i64,
) -> Result<String, String> {
    c.query_one(
        "INSERT INTO queen.log_partitions (queue_id, name, last_offset, log_start) \
         VALUES ($1::text::uuid, $2, $3, $4) RETURNING id::text",
        &[&queue_id, &name, &last_offset, &log_start],
    )
    .await
    .map(|r| r.get::<_, String>(0))
    .map_err(pg_err)
}

/// One segment covering `[base, end]`, aged by `age_expr` (a SQL interval
/// expression). The covering probe reads `created_at`, and nothing here reads
/// the blob.
async fn seed_segment(
    c: &Client,
    partition_id: &str,
    base: i64,
    end: i64,
    age_expr: &str,
) -> Result<(), String> {
    let sql = format!(
        "INSERT INTO queen.log_segments (partition_id, base_offset, end_offset, created_at, blob) \
         VALUES ($1::text::uuid, $2, $3, now() - interval '{age_expr}', '\\x00'::bytea)"
    );
    c.execute(sql.as_str(), &[&partition_id, &base, &end])
        .await
        .map(|_| ())
        .map_err(pg_err)
}

async fn seed_native_cursor(
    c: &Client,
    partition_id: &str,
    group: &str,
    committed: i64,
    total_consumed: i64,
) -> Result<(), String> {
    c.execute(
        "INSERT INTO queen.log_consumers (partition_id, consumer_group, committed, total_consumed) \
         VALUES ($1::text::uuid, $2, $3, $4)",
        &[&partition_id, &group, &committed, &total_consumed],
    )
    .await
    .map(|_| ())
    .map_err(pg_err)
}

/// A raw row in `queen.kv`. Direct INSERT and not `kv_apply_v1` on purpose: what
/// is under test is the READ, and a raw row lets a case seed shapes the write
/// path would refuse (an already-expired row, a value with no offset) without
/// mocking a clock. `expires_expr` is interpolated because it is a SQL
/// expression; test-only input, no injection surface.
async fn seed_kv(
    c: &Client,
    tenant: &str,
    key: &str,
    value: Value,
    expires_expr: &str,
) -> Result<(), String> {
    let sql = format!(
        "INSERT INTO queen.kv (tenant_id, namespace, key, value, version, expires_at) \
         VALUES ($1::text::uuid, 'queen-kafka', $2, $3::text::jsonb, \
                 nextval('queen.kv_version_seq'), {expires_expr})"
    );
    c.execute(sql.as_str(), &[&tenant, &key, &value.to_string()])
        .await
        .map(|_| ())
        .map_err(pg_err)
}

/// A committed Kafka offset. NOTE THE VALUE: Kafka's committed offset is the
/// NEXT RECORD TO READ, so this is what the facade writes after consuming
/// through `offset - 1`.
async fn commit_kafka(
    c: &Client,
    tenant: &str,
    group: &str,
    topic: &str,
    partition: &str,
    offset: i64,
) -> Result<(), String> {
    let key = format!("qk:group:{}:{}:{}", esc(group), esc(topic), partition);
    seed_kv(
        c,
        tenant,
        &key,
        json!({"offset": offset, "metadata": "", "ts": 1787824800123i64}),
        "NULL",
    )
    .await
}

/// `protocols/queen-kafka/src/offsets.rs::escape`, transcribed. The tests compose keys the
/// way the facade does, so a case that spells a group id with a separator in it
/// exercises the same bytes the facade would write.
fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// -------------------------------------------------------------------- reading

async fn groups(c: &Client, tenant: &str) -> Result<Vec<Value>, String> {
    let row = c
        .query_one(
            "SELECT (queen.get_consumer_groups_v4($1::text::uuid))::text",
            &[&tenant],
        )
        .await
        .map_err(pg_err)?;
    let txt: String = row.get(0);
    serde_json::from_str::<Value>(&txt)
        .map_err(|e| format!("get_consumer_groups_v4 returned unparseable JSON: {e}"))?
        .as_array()
        .cloned()
        .ok_or_else(|| format!("expected a JSON array, got {txt}"))
}

async fn lagging(c: &Client, tenant: &str, min_lag: i32) -> Result<Vec<Value>, String> {
    let row = c
        .query_one(
            "SELECT (queen.get_lagging_partitions_v1($1::int, $2::text::uuid))::text",
            &[&min_lag, &tenant],
        )
        .await
        .map_err(pg_err)?;
    let txt: String = row.get(0);
    serde_json::from_str::<Value>(&txt)
        .map_err(|e| format!("get_lagging_partitions_v1 returned unparseable JSON: {e}"))?
        .as_array()
        .cloned()
        .ok_or_else(|| format!("expected a JSON array, got {txt}"))
}

async fn details(c: &Client, group: &str, tenant: &str) -> Result<Value, String> {
    let row = c
        .query_one(
            "SELECT (queen.get_consumer_group_details_v1($1, $2::text::uuid))::text",
            &[&group, &tenant],
        )
        .await
        .map_err(pg_err)?;
    let txt: String = row.get(0);
    serde_json::from_str::<Value>(&txt)
        .map_err(|e| format!("get_consumer_group_details_v1 returned unparseable JSON: {e}"))
}

fn find<'a>(rows: &'a [Value], name: &str, queue: &str) -> Option<&'a Value> {
    rows.iter()
        .find(|r| r["name"] == json!(name) && r["queueName"] == json!(queue))
}

fn names(rows: &[Value]) -> Vec<String> {
    rows.iter()
        .map(|r| {
            format!(
                "{}@{}/{}",
                r["name"].as_str().unwrap_or("?"),
                r["queueName"].as_str().unwrap_or("?"),
                r["kind"].as_str().unwrap_or("?")
            )
        })
        .collect()
}

// ========================================================================
// cases
// ========================================================================

/// The mirror exists at all: a group with nothing but a KV commit is a row.
async fn case_appears_with_kind_kafka(c: &Client) -> Case {
    let t = new_tenant(c).await?;
    let q = seed_queue(c, &t, "orders").await?;
    seed_partition(c, &q, "0", 9, 0).await?;
    commit_kafka(c, &t, "billing", "orders", "0", 10).await?;

    let rows = groups(c, &t).await?;
    chk!(
        rows.len() == 1,
        "expected exactly one group, got {:?}",
        names(&rows)
    );
    let g = &rows[0];
    chk!(
        g["kind"] == json!("kafka"),
        "kind must be \"kafka\", got {}",
        g["kind"]
    );
    chk!(
        g["name"] == json!("billing"),
        "name must be the group id, got {}",
        g["name"]
    );
    chk!(
        g["queueName"] == json!("orders"),
        "queueName must be the topic, got {}",
        g["queueName"]
    );
    chk!(
        g["topics"] == json!(["orders"]),
        "topics must be [queue], got {}",
        g["topics"]
    );
    // "partition cursors, not processes" — the same meaning the native field
    // already carries; one committed partition is one cursor.
    chk!(
        g["members"] == json!(1),
        "members must be 1 cursor, got {}",
        g["members"]
    );
    chk!(
        g["partitionCursors"] == json!(1),
        "partitionCursors must be 1, got {}",
        g["partitionCursors"]
    );
    chk!(
        g["storage"] == json!("segments"),
        "storage describes the QUEUE, got {}",
        g["storage"]
    );
    // Queen queue-group policy; the facade declares none.
    chk!(
        g["subscriptionMode"] == json!(null),
        "subscriptionMode must be null, got {}",
        g["subscriptionMode"]
    );
    chk!(
        g["subscriptionTimestamp"] == json!(null),
        "subscriptionTimestamp must be null"
    );
    chk!(
        g["subscriptionCreatedAt"] == json!(null),
        "subscriptionCreatedAt must be null"
    );
    chk!(
        g["conflation"] == json!(false),
        "conflation must be false, got {}",
        g["conflation"]
    );
    Ok(())
}

/// THE OFF-BY-ONE. `log_consumers.committed` is the LAST CONSUMED offset; a
/// Kafka committed offset is the NEXT RECORD TO READ. Get this wrong and every
/// Kafka group in the console reads one message behind forever — silently.
async fn case_lag_is_head_minus_committed_offset(c: &Client) -> Case {
    let t = new_tenant(c).await?;
    let q = seed_queue(c, &t, "orders").await?;
    // Ten records pushed: offsets 0..=9, so last_offset = 9.
    seed_partition(c, &q, "0", 9, 0).await?;

    // Caught up: consumed through 9, next to read is 10.
    commit_kafka(c, &t, "g", "orders", "0", 10).await?;
    let rows = groups(c, &t).await?;
    let g = find(&rows, "g", "orders").ok_or("group g missing after a caught-up commit")?;
    chk!(
        g["totalLag"] == json!(0),
        "a fully caught-up Kafka group must read lag 0, got {} (the classic +1)",
        g["totalLag"]
    );
    chk!(
        g["partitionsWithLag"] == json!(0),
        "no partition lags, got {}",
        g["partitionsWithLag"]
    );

    // Rewind: next to read is 7, so 7, 8 and 9 are outstanding.
    c.execute(
        "UPDATE queen.kv SET value = '{\"offset\":7}'::jsonb \
         WHERE tenant_id = $1::text::uuid AND namespace = 'queen-kafka' AND key = 'qk:group:g:orders:0'",
        &[&t],
    )
    .await
    .map_err(pg_err)?;
    let rows = groups(c, &t).await?;
    let g = find(&rows, "g", "orders").ok_or("group g missing after the rewind")?;
    chk!(
        g["totalLag"] == json!(3),
        "committed offset 7 over head 9 is 3 outstanding, got {}",
        g["totalLag"]
    );
    chk!(
        g["partitionsWithLag"] == json!(1),
        "one partition lags, got {}",
        g["partitionsWithLag"]
    );
    Ok(())
}

/// `qk:groups:` (the durable group INDEX) must sort OUTSIDE
/// `[qk:group:, qk:group;)`. The key below is chosen so that a leak would be
/// loud rather than subtle: read as an offsets key it names group "s" on the
/// topic "orders", which exists — so a wrong range invents a group.
async fn case_group_index_is_not_read_as_offsets(c: &Client) -> Case {
    let t = new_tenant(c).await?;
    let q = seed_queue(c, &t, "orders").await?;
    seed_partition(c, &q, "0", 9, 0).await?;
    commit_kafka(c, &t, "real", "orders", "0", 5).await?;
    // Deliberately shaped to decode into a valid group if the range leaked.
    seed_kv(c, &t, "qk:groups:orders:0", json!({"offset": 1}), "NULL").await?;
    // And the shape the facade actually writes there.
    seed_kv(
        c,
        &t,
        "qk:groups:real",
        json!({"pt": "consumer", "ts": 1}),
        "NULL",
    )
    .await?;

    let rows = groups(c, &t).await?;
    chk!(
        rows.len() == 1,
        "the group index must contribute nothing, got {:?}",
        names(&rows)
    );
    chk!(
        rows[0]["name"] == json!("real"),
        "only the real group survives, got {}",
        rows[0]["name"]
    );
    Ok(())
}

/// The `offsets.rs` ambiguity, at the SQL end. Splitting the DECODED key would
/// make these two the same row; splitting the ESCAPED key keeps them apart,
/// because `escape` percent-encodes the separator.
async fn case_a_colon_in_a_group_id_does_not_split(c: &Client) -> Case {
    let t = new_tenant(c).await?;
    let q_zero = seed_queue(c, &t, "0").await?;
    seed_partition(c, &q_zero, "0", 9, 0).await?;
    let q_b = seed_queue(c, &t, "b").await?;
    seed_partition(c, &q_b, "0", 9, 0).await?;

    // group "a:b" on topic "0"  ->  qk:group:a%3Ab:0:0
    commit_kafka(c, &t, "a:b", "0", "0", 3).await?;
    // group "a"   on topic "b"  ->  qk:group:a:b:0
    commit_kafka(c, &t, "a", "b", "0", 5).await?;

    let rows = groups(c, &t).await?;
    chk!(
        rows.len() == 2,
        "expected two distinct groups, got {:?}",
        names(&rows)
    );
    let ab = find(&rows, "a:b", "0").ok_or_else(|| {
        format!(
            "group \"a:b\" on queue \"0\" missing (unescape or split is wrong): {:?}",
            names(&rows)
        )
    })?;
    let a = find(&rows, "a", "b")
        .ok_or_else(|| format!("group \"a\" on queue \"b\" missing: {:?}", names(&rows)))?;
    chk!(
        ab["totalLag"] == json!(7),
        "a:b committed 3 over head 9 lags 7, got {}",
        ab["totalLag"]
    );
    chk!(
        a["totalLag"] == json!(5),
        "a committed 5 over head 9 lags 5, got {}",
        a["totalLag"]
    );
    Ok(())
}

/// Everything in the namespace that is NOT a live committed offset: expired,
/// non-numeric, and the other four `qk:` spaces.
async fn case_expired_or_foreign_rows_are_not_groups(c: &Client) -> Case {
    let t = new_tenant(c).await?;
    let q = seed_queue(c, &t, "orders").await?;
    seed_partition(c, &q, "0", 9, 0).await?;

    // The control: one live, well-formed commit.
    commit_kafka(c, &t, "live", "orders", "0", 4).await?;

    // §5.7 — an expired row is never returned and never counts as existing,
    // whether or not the sweeper has been past.
    seed_kv(
        c,
        &t,
        "qk:group:gone:orders:0",
        json!({"offset": 1}),
        "now() - interval '1 minute'",
    )
    .await?;
    // Not ours: no numeric offset.
    seed_kv(
        c,
        &t,
        "qk:group:nometa:orders:0",
        json!({"metadata": "x"}),
        "NULL",
    )
    .await?;
    seed_kv(
        c,
        &t,
        "qk:group:strnum:orders:0",
        json!({"offset": "7"}),
        "NULL",
    )
    .await?;
    seed_kv(
        c,
        &t,
        "qk:group:nullv:orders:0",
        json!({"offset": null}),
        "NULL",
    )
    .await?;
    // The other reserved spaces, all outside the range (they differ at byte 4).
    seed_kv(c, &t, "qk:txn:orders:0", json!({"offset": 1}), "NULL").await?;
    seed_kv(c, &t, "qk:fence:orders:0", json!({"offset": 1}), "NULL").await?;
    seed_kv(c, &t, "qk:node:orders:0", json!({"offset": 1}), "NULL").await?;
    // A foreign namespace in the same table.
    c.execute(
        "INSERT INTO queen.kv (tenant_id, namespace, key, value, version) \
         VALUES ($1::text::uuid, 'app', 'qk:group:foreignns:orders:0', '{\"offset\":1}'::jsonb, \
                 nextval('queen.kv_version_seq'))",
        &[&t],
    )
    .await
    .map_err(pg_err)?;

    let rows = groups(c, &t).await?;
    chk!(
        rows.len() == 1,
        "only the live commit is a group, got {:?}",
        names(&rows)
    );
    chk!(
        rows[0]["name"] == json!("live"),
        "wrong survivor: {}",
        rows[0]["name"]
    );
    // The expired row is still physically there — what is under test is the
    // predicate, not the sweeper.
    let still: i64 = c
        .query_one(
            "SELECT count(*) FROM queen.kv WHERE tenant_id = $1::text::uuid AND key = 'qk:group:gone:orders:0'",
            &[&t],
        )
        .await
        .map_err(pg_err)?
        .get(0);
    chk!(
        still == 1,
        "the expired row must still exist physically, count = {still}"
    );
    Ok(())
}

/// Two tenants, same group name, same topic name. The queues join carries
/// tenant_id — the same guard `server/src/handlers/fetch.rs` documents.
async fn case_scoped_to_its_tenant(c: &Client) -> Case {
    let a = new_tenant(c).await?;
    let b = new_tenant(c).await?;
    let qa = seed_queue(c, &a, "shared").await?;
    seed_partition(c, &qa, "0", 9, 0).await?;
    let qb = seed_queue(c, &b, "shared").await?;
    seed_partition(c, &qb, "0", 99, 0).await?;
    commit_kafka(c, &a, "same", "shared", "0", 10).await?; // caught up on A
    commit_kafka(c, &b, "same", "shared", "0", 50).await?; // 50 behind on B

    let ra = groups(c, &a).await?;
    chk!(
        ra.len() == 1,
        "tenant A must see exactly its own group, got {:?}",
        names(&ra)
    );
    chk!(
        ra[0]["totalLag"] == json!(0),
        "A's cursor is caught up, got {}",
        ra[0]["totalLag"]
    );
    let rb = groups(c, &b).await?;
    chk!(
        rb.len() == 1,
        "tenant B must see exactly its own group, got {:?}",
        names(&rb)
    );
    chk!(
        rb[0]["totalLag"] == json!(50),
        "B committed 50 over head 99 lags 50, got {}",
        rb[0]["totalLag"]
    );
    Ok(())
}

/// A KV row naming a topic this tenant does not own contributes nothing — and in
/// particular does not reach across to the tenant that DOES own that name.
async fn case_a_topic_with_no_queue_contributes_nothing(c: &Client) -> Case {
    let a = new_tenant(c).await?;
    let b = new_tenant(c).await?;
    // Only B owns "ghost".
    let qb = seed_queue(c, &b, "ghost").await?;
    seed_partition(c, &qb, "0", 99, 0).await?;
    // A commits against it anyway.
    commit_kafka(c, &a, "g", "ghost", "0", 1).await?;
    // A owns a queue, but the commit does not name a partition of it.
    let qa = seed_queue(c, &a, "real").await?;
    seed_partition(c, &qa, "0", 9, 0).await?;
    commit_kafka(c, &a, "g", "real", "7", 1).await?; // partition "7" was never pushed to

    let ra = groups(c, &a).await?;
    chk!(
        ra.is_empty(),
        "no queue and no partition means no row, got {:?}",
        names(&ra)
    );
    let rb = groups(c, &b).await?;
    chk!(
        rb.is_empty(),
        "A's commit must not surface under B, got {:?}",
        names(&rb)
    );
    Ok(())
}

/// The regression guard on the native leg: every key it emitted before the
/// Kafka mirror existed, with the values the old arithmetic produced, plus
/// exactly one new key — `kind`.
async fn case_native_rows_are_unchanged_but_for_kind(c: &Client) -> Case {
    let t = new_tenant(c).await?;
    let q = seed_queue(c, &t, "work").await?;
    let p = seed_partition(c, &q, "0", 9, 0).await?;
    // Native: `committed` is the LAST CONSUMED offset, so 6 leaves 7, 8, 9.
    seed_native_cursor(c, &p, "native", 6, 7).await?;

    let rows = groups(c, &t).await?;
    chk!(
        rows.len() == 1,
        "expected the one native group, got {:?}",
        names(&rows)
    );
    let g = &rows[0];

    let mut got: Vec<&str> = g
        .as_object()
        .ok_or("row is not an object")?
        .keys()
        .map(|s| s.as_str())
        .collect();
    got.sort_unstable();
    let mut want = vec![
        "name",
        "topics",
        "queueName",
        "members",
        "partitionCursors",
        "partitionsWithLag",
        "totalLag",
        "maxTimeLag",
        "state",
        "storage",
        "subscriptionMode",
        "subscriptionTimestamp",
        "subscriptionCreatedAt",
        "conflation",
        "kind",
    ];
    want.sort_unstable();
    chk!(
        got == want,
        "the key set changed.\n  got  {got:?}\n  want {want:?}"
    );

    chk!(
        g["kind"] == json!("queen"),
        "a native row is kind \"queen\", got {}",
        g["kind"]
    );
    chk!(
        g["totalLag"] == json!(3),
        "native lag arithmetic changed: {}",
        g["totalLag"]
    );
    chk!(
        g["members"] == json!(1),
        "members changed: {}",
        g["members"]
    );
    chk!(
        g["partitionsWithLag"] == json!(1),
        "partitionsWithLag changed: {}",
        g["partitionsWithLag"]
    );
    chk!(
        g["maxTimeLag"] == json!(0),
        "no segments means no time lag, got {}",
        g["maxTimeLag"]
    );
    // total_consumed > 0 and no time lag -> Stable, exactly as before.
    chk!(
        g["state"] == json!("Stable"),
        "state changed: {}",
        g["state"]
    );
    chk!(
        g["storage"] == json!("segments"),
        "storage changed: {}",
        g["storage"]
    );
    Ok(())
}

/// A never-consumed native cursor is still 'Dead', and a Kafka cursor never is:
/// a committed offset IS the evidence of consumption that `total_consumed > 0`
/// stands in for.
async fn case_state_dead_stays_native_only(c: &Client) -> Case {
    let t = new_tenant(c).await?;
    let q = seed_queue(c, &t, "orders").await?;
    let p = seed_partition(c, &q, "0", 9, 0).await?;
    seed_native_cursor(c, &p, "abandoned", -1, 0).await?;
    // A Kafka group that has consumed nothing yet still committed offset 0.
    commit_kafka(c, &t, "fresh", "orders", "0", 0).await?;

    let rows = groups(c, &t).await?;
    let n = find(&rows, "abandoned", "orders").ok_or("native group missing")?;
    chk!(
        n["state"] == json!("Dead"),
        "an unconsumed native cursor is Dead, got {}",
        n["state"]
    );
    let k = find(&rows, "fresh", "orders").ok_or("kafka group missing")?;
    chk!(
        k["state"] == json!("Stable"),
        "a Kafka group with a commit is never Dead, got {}",
        k["state"]
    );
    chk!(
        k["totalLag"] == json!(10),
        "committed 0 over head 9 lags all 10, got {}",
        k["totalLag"]
    );
    Ok(())
}

/// The collision, stated as behaviour rather than left to be discovered: one
/// name, one queue, two stores, TWO rows — because `kind` is in the GROUP BY.
async fn case_a_name_in_both_stores_is_two_rows(c: &Client) -> Case {
    let t = new_tenant(c).await?;
    let q = seed_queue(c, &t, "orders").await?;
    let p = seed_partition(c, &q, "0", 9, 0).await?;
    seed_native_cursor(c, &p, "twin", 4, 5).await?;
    commit_kafka(c, &t, "twin", "orders", "0", 8).await?;

    let rows = groups(c, &t).await?;
    chk!(
        rows.len() == 2,
        "two cursors in two stores are two rows, got {:?}",
        names(&rows)
    );
    let kinds: Vec<&str> = rows.iter().filter_map(|r| r["kind"].as_str()).collect();
    chk!(
        kinds.contains(&"queen") && kinds.contains(&"kafka"),
        "both kinds must be present, got {kinds:?}"
    );
    let native = rows.iter().find(|r| r["kind"] == json!("queen")).unwrap();
    let kafka = rows.iter().find(|r| r["kind"] == json!("kafka")).unwrap();
    chk!(
        native["totalLag"] == json!(5),
        "native committed 4 over head 9 lags 5, got {}",
        native["totalLag"]
    );
    chk!(
        kafka["totalLag"] == json!(2),
        "kafka committed 8 over head 9 lags 2, got {}",
        kafka["totalLag"]
    );
    Ok(())
}

/// `queen.kv_qk_unescape` against `offsets.rs::unescape`, including both of its
/// deliberate leniencies.
async fn case_unescape_matches_the_rust(c: &Client) -> Case {
    let cases: &[(&str, &str)] = &[
        // The common path: nothing to decode.
        ("plain-group.id_1", "plain-group.id_1"),
        // What escape() actually produces for the separators.
        ("a%3Ab", "a:b"),
        ("with%20space", "with space"),
        ("%2F%25%2B", "/%+"),
        // Lower-case hex is accepted, same as u8::from_str_radix.
        ("a%3ab", "a:b"),
        // "an escape that is not one is left as it stands": not hex.
        ("%zz", "%zz"),
        ("100%", "100%"),
        // ... and a '%' with fewer than two bytes after it.
        ("a%4", "a%4"),
        ("%", "%"),
        // Multi-byte UTF-8 survives the byte-wise round trip.
        ("caff%C3%A8", "caff\u{e8}"),
    ];
    for (input, want) in cases {
        let got: String = c
            .query_one("SELECT queen.kv_qk_unescape($1)", &[input])
            .await
            .map_err(pg_err)?
            .get(0);
        chk!(
            got == *want,
            "kv_qk_unescape({input:?}) = {got:?}, want {want:?}"
        );
        // And the round trip from the Rust side's own escaper.
        let round: String = c
            .query_one("SELECT queen.kv_qk_unescape($1)", &[&esc(want)])
            .await
            .map_err(pg_err)?
            .get(0);
        chk!(
            round == *want,
            "escape/unescape round trip broke for {want:?}: got {round:?}"
        );
    }
    // STRICT: null in, null out, never an error inside a LEFT JOIN.
    let n: Option<String> = c
        .query_one("SELECT queen.kv_qk_unescape(NULL)", &[])
        .await
        .map_err(pg_err)?
        .get(0);
    chk!(n.is_none(), "kv_qk_unescape(NULL) must be NULL, got {n:?}");
    Ok(())
}

/// The lagging view carries the Kafka leg too, with the same time-lag probe.
async fn case_the_lagging_view_sees_kafka(c: &Client) -> Case {
    let t = new_tenant(c).await?;
    let q = seed_queue(c, &t, "orders").await?;
    let p = seed_partition(c, &q, "0", 9, 0).await?;
    // One segment covering the whole log, written an hour ago.
    seed_segment(c, &p, 0, 9, "1 hour").await?;
    commit_kafka(c, &t, "slow", "orders", "0", 4).await?;

    let rows = lagging(c, &t, 60).await?;
    chk!(
        rows.len() == 1,
        "expected one lagging partition, got {rows:?}"
    );
    let r = &rows[0];
    chk!(
        r["kind"] == json!("kafka"),
        "kind must be kafka, got {}",
        r["kind"]
    );
    chk!(
        r["consumer_group"] == json!("slow"),
        "group must be slow, got {}",
        r["consumer_group"]
    );
    chk!(
        r["queue_name"] == json!("orders"),
        "queue must be orders, got {}",
        r["queue_name"]
    );
    chk!(
        r["partition_name"] == json!("0"),
        "partition must be \"0\", got {}",
        r["partition_name"]
    );
    chk!(
        r["offset_lag"] == json!(6),
        "committed 4 over head 9 lags 6, got {}",
        r["offset_lag"]
    );
    // A KV commit names a group and a partition, never the member that wrote it.
    chk!(
        r["worker_id"] == json!(null),
        "a Kafka row has no worker id, got {}",
        r["worker_id"]
    );
    let secs = r["time_lag_seconds"].as_i64().unwrap_or(0);
    chk!(
        secs > 3000,
        "the covering probe must find the hour-old segment, got {secs}s"
    );

    // And the threshold still filters.
    let none = lagging(c, &t, 100_000).await?;
    chk!(
        none.is_empty(),
        "min_lag_seconds must still filter, got {none:?}"
    );
    Ok(())
}

/// The detail view is keyed BY QUEUE NAME, so it is the one place both stores
/// cannot be shown at once. The native entry wins and `kind` says so.
async fn case_details_prefers_native_on_a_collision(c: &Client) -> Case {
    let t = new_tenant(c).await?;
    let q = seed_queue(c, &t, "orders").await?;
    let p = seed_partition(c, &q, "0", 9, 0).await?;
    seed_native_cursor(c, &p, "twin", 4, 5).await?;
    commit_kafka(c, &t, "twin", "orders", "0", 8).await?;

    let d = details(c, "twin", &t).await?;
    let e = &d["orders"];
    chk!(!e.is_null(), "the queue entry is missing: {d}");
    chk!(
        e["kind"] == json!("queen"),
        "the native entry must win a collision, got {}",
        e["kind"]
    );
    let parts = e["partitions"]
        .as_array()
        .ok_or("partitions is not an array")?;
    chk!(
        parts.len() == 1,
        "the two stores must not be mixed into one array, got {parts:?}"
    );
    chk!(
        parts[0]["offsetLag"] == json!(5),
        "the native lag must be shown, got {}",
        parts[0]["offsetLag"]
    );

    // Alone on its queue, a Kafka group renders as itself.
    let t2 = new_tenant(c).await?;
    let q2 = seed_queue(c, &t2, "orders").await?;
    seed_partition(c, &q2, "0", 9, 0).await?;
    commit_kafka(c, &t2, "twin", "orders", "0", 8).await?;
    let d2 = details(c, "twin", &t2).await?;
    let e2 = &d2["orders"];
    chk!(
        e2["kind"] == json!("kafka"),
        "a lone Kafka group renders as kafka, got {}",
        e2["kind"]
    );
    chk!(
        e2["conflation"] == json!(false),
        "conflation is Queen policy: false, got {}",
        e2["conflation"]
    );
    let p2 = e2["partitions"]
        .as_array()
        .ok_or("partitions is not an array")?;
    // Committed offset 8 = "next to read is 8", so 8 and 9 are both outstanding.
    chk!(
        p2[0]["offsetLag"] == json!(2),
        "committed 8 over head 9 lags 2, got {}",
        p2[0]["offsetLag"]
    );
    chk!(
        p2[0]["workerId"] == json!(null),
        "no worker id, got {}",
        p2[0]["workerId"]
    );
    chk!(
        p2[0]["totalConsumed"] == json!(null),
        "no total_consumed in KV, got {}",
        p2[0]["totalConsumed"]
    );
    chk!(
        p2[0]["leaseActive"] == json!(false),
        "a Kafka fetch takes no lease, got {}",
        p2[0]["leaseActive"]
    );
    Ok(())
}

/// THE 60k-PARTITION LESSON, applied to the new leg. `queen.kv` is a
/// PK-range read and must never be a Seq Scan: the whole reason the kv table
/// carries no second index is that the read is an index-prefix range on
/// (tenant_id, namespace, key) COLLATE "C".
///
/// The fixture is the production shape, not a toy: the table holds thousands of
/// rows belonging to somebody else, and this tenant's slice is small. On a table
/// that held ONLY the rows under test a Seq Scan would be the correct plan and
/// the assertion would prove nothing.
async fn case_the_kv_leg_is_a_pk_range_scan(c: &Client) -> Case {
    let t = new_tenant(c).await?;
    let other = new_tenant(c).await?;
    let q = seed_queue(c, &t, "orders").await?;
    seed_partition(c, &q, "0", 999, 0).await?;

    // 5000 foreign rows: another tenant, and other key spaces of this one.
    c.execute(
        "INSERT INTO queen.kv (tenant_id, namespace, key, value, version) \
         SELECT $1::text::uuid, 'queen-kafka', 'qk:group:noise' || g || ':orders:0', \
                jsonb_build_object('offset', g), nextval('queen.kv_version_seq') \
         FROM generate_series(1, 4000) g",
        &[&other],
    )
    .await
    .map_err(pg_err)?;
    c.execute(
        "INSERT INTO queen.kv (tenant_id, namespace, key, value, version) \
         SELECT $1::text::uuid, 'app', 'cache:' || g, '{}'::jsonb, nextval('queen.kv_version_seq') \
         FROM generate_series(1, 1000) g",
        &[&t],
    )
    .await
    .map_err(pg_err)?;
    // Ours: a handful.
    for i in 0..20 {
        commit_kafka(c, &t, &format!("g{i}"), "orders", "0", 100 + i).await?;
    }
    c.execute("ANALYZE queen.kv", &[]).await.map_err(pg_err)?;

    // The kv leg, verbatim from queen.get_consumer_groups_v4's kafka_base.
    let sql = "EXPLAIN (ANALYZE, BUFFERS) \
        SELECT queen.kv_qk_unescape(split_part(substr(kv.key, 10), ':', 1)) AS grp, \
               queen.kv_qk_unescape(split_part(substr(kv.key, 10), ':', 2)) AS topic, \
               split_part(substr(kv.key, 10), ':', 3)                       AS part_name, \
               (kv.value->>'offset')::bigint                                AS off \
        FROM queen.kv \
        WHERE kv.tenant_id = $1::text::uuid \
          AND kv.namespace = 'queen-kafka' \
          AND kv.key >= 'qk:group:' AND kv.key < 'qk:group;' \
          AND (kv.expires_at IS NULL OR kv.expires_at > now()) \
          AND jsonb_typeof(kv.value->'offset') = 'number'";
    let plan: String = c
        .query(sql, &[&t])
        .await
        .map_err(pg_err)?
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n");
    println!("\n--- EXPLAIN (ANALYZE, BUFFERS), kv leg ---\n{plan}\n---");
    // The property is the ACCESS PATH, not the node name: plain, index-only and
    // bitmap index scans all take the range off kv_pkey and read only the rows
    // in it. What must never appear is a scan of the whole heap.
    chk!(
        plan.contains("kv_pkey"),
        "the kv leg must take its range off the (tenant_id, namespace, key) PK. Plan:\n{plan}"
    );
    chk!(
        plan.contains("Index Cond") && plan.contains("key >= 'qk:group:'"),
        "the range must be an INDEX CONDITION, not a post-scan filter. Plan:\n{plan}"
    );
    chk!(
        !plan.contains("Seq Scan on kv"),
        "the kv leg must never seq-scan queen.kv — that is the 2026-08-24 lesson. Plan:\n{plan}"
    );
    // And the whole view still answers over that table.
    let rows = groups(c, &t).await?;
    chk!(
        rows.len() == 20,
        "expected this tenant's 20 groups, got {}",
        rows.len()
    );
    Ok(())
}

// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn kafka_group_mirror() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // Boot the real broker purely to apply the real schema. The SQL under test
    // is include_str!-embedded, so this is also what proves the edit to
    // 010_log_admin.sql was REBUILT: a stale binary carries the old bytes and
    // every `kind` assertion below fails on a missing key.
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
        "a_kafka_group_appears_with_kind_kafka",
        case_appears_with_kind_kafka(&c).await,
    ));
    report.push((
        "kafka_lag_is_head_minus_committed_offset",
        case_lag_is_head_minus_committed_offset(&c).await,
    ));
    report.push((
        "the_group_index_prefix_is_not_read_as_offsets",
        case_group_index_is_not_read_as_offsets(&c).await,
    ));
    report.push((
        "a_group_id_containing_a_colon_does_not_split_into_a_topic",
        case_a_colon_in_a_group_id_does_not_split(&c).await,
    ));
    report.push((
        "an_expired_or_foreign_kv_row_is_not_a_group",
        case_expired_or_foreign_rows_are_not_groups(&c).await,
    ));
    report.push((
        "a_kafka_row_is_scoped_to_its_tenant",
        case_scoped_to_its_tenant(&c).await,
    ));
    report.push((
        "a_topic_with_no_queue_row_contributes_nothing",
        case_a_topic_with_no_queue_contributes_nothing(&c).await,
    ));
    report.push((
        "native_rows_are_byte_identical_to_before",
        case_native_rows_are_unchanged_but_for_kind(&c).await,
    ));
    report.push((
        "a_kafka_group_is_never_dead",
        case_state_dead_stays_native_only(&c).await,
    ));
    report.push((
        "one_name_in_two_stores_is_two_rows",
        case_a_name_in_both_stores_is_two_rows(&c).await,
    ));
    report.push((
        "kv_qk_unescape_matches_offsets_rs",
        case_unescape_matches_the_rust(&c).await,
    ));
    report.push((
        "the_lagging_view_sees_kafka_rows",
        case_the_lagging_view_sees_kafka(&c).await,
    ));
    report.push((
        "the_details_view_prefers_the_native_entry",
        case_details_prefers_native_on_a_collision(&c).await,
    ));
    report.push((
        "the_kv_leg_plans_as_a_pk_range_scan",
        case_the_kv_leg_is_a_pk_range_scan(&c).await,
    ));

    println!("\n================ Kafka group mirror (DESIGN §1) ================");
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
        "======================= {}/{} passed =======================\n",
        report.len() - failed,
        report.len()
    );
    assert_eq!(
        failed, 0,
        "{failed} Kafka group mirror case(s) failed — see the table above"
    );
}
