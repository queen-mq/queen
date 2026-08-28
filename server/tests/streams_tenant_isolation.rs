//! Tenant isolation for the `queen_streams` streaming engine (Track B §5, 2026-08-27):
//! `queen.streams_register_query_v1`, `queen.streams_state_get_v1` and
//! `queen.log_streams_cycle_v1`.
//!
//! The claim under test is the one `timers_tenant_isolation.rs` makes for timers and KV,
//! restated for the third feature that reaches Postgres by NAME and by RAW UUID at the
//! same time: **two tenants sharing every name share nothing else, and the one that owns
//! a thing is never inconvenienced by the one that does not.** Both halves are asserted
//! everywhere below, because only asserting the first passes trivially on an engine that
//! has stopped working, and only asserting the second passes on one with no isolation at
//! all.
//!
//! The setup is adversarial in the same way: **the same query name, the same source queue
//! name, the same sink queue name, the same state keys, under two tenants.** That is the
//! shape in which every bug in this feature is invisible. Nothing raises; one customer's
//! window aggregate simply appears in the other's.
//!
//! WHY EACH SECTION EXISTS — what specifically went wrong before it, or would:
//!
//!   1. **Per-tenant names.** `queries.name` was `TEXT UNIQUE` — GLOBALLY unique, across
//!      the whole database. The second tenant to deploy `orders.per_customer_per_min`
//!      collided with a query it could not see, and the conflict result handed it the
//!      other owner's internal UUID (008's `ELSE` branch). Uniqueness is
//!      `(tenant_id, name)` now, and the conflict can only ever reveal the caller's own
//!      id.
//!   2. **The grant is a DENIAL by absence** for a non-default tenant, the posture
//!      `queen.kv_quota` takes and for the same reason: the tenant header is opaque and
//!      validated against nothing, so fail-open mints an unlimited tenant for any
//!      invented id. Checked ONLY on the fresh-insert path — a revoked grant must stop
//!      NEW queries and never a Runner that is already draining, or a billing decision
//!      turns into a stranded backlog nobody can consume.
//!   3. **The cap is exact and per-tenant.** The count that feeds `max_queries` is taken
//!      under the `FOR UPDATE` on the tenant's own grant row, and it counts that tenant's
//!      rows. A global `count(*)` would deny a tenant its FIRST query on a busy cell.
//!   4. **`state_get` is not an oracle.** A foreign `(query_id, partition_id)` reads
//!      exactly like a key that was never written — `success: true`, `rows: []` — so the
//!      endpoint cannot be probed for the existence of another tenant's query.
//!   5. **The cycle is the dangerous one**, because BOTH of its addresses are raw uuids
//!      off the wire and its sinks are resolved by NAME. Pre-tenancy the source lookup
//!      was decorative (it fed a metric label) and NOT FOUND was silently fine, so a
//!      foreign or bogus `partition_id` ran the whole cycle; and a bare-name sink resolve
//!      does not merely go unscoped, it can MULTI-MATCH, fanning one emit across the
//!      same-named queue of every tenant on the cell.
//!   6. **OSS byte-identity.** Every pre-tenancy caller lands on the default tenant,
//!      which is never gate-checked and needs no grant row to exist. The one-ARGUMENT
//!      call is exercised directly here: 008/009/007 DROP the pre-tenancy single-argument
//!      overload and re-create a two-argument one with a DEFAULT, and that only resolves
//!      if the DROP actually happened (two live overloads make the call ambiguous, not
//!      convenient). "Global-feeling" is also pinned to mean what it says — the default
//!      tenant sees its own rows and no one else's.
//!
//! WHAT THIS FILE CANNOT REACH, and why — see `handler_source_pins_the_status_map` at the
//! bottom. The HTTP behaviours (403 vs 409 on register, 404 from the partition pre-gate)
//! live in `src/handlers/streams.rs`, whose routes are declared in `src/main.rs` — the
//! BINARY's crate root. `src/lib.rs` keeps `mod handlers` private and the embedded facade
//! (`queen::Broker`) exposes no streams surface at all, so an integration test in this
//! directory has no way to issue a request at those handlers. That is the same wall
//! `kv_handler_isolation.rs` meets, and this file answers it the same way: a source-level
//! pin, in a second test function that needs no Postgres and runs in the plain
//! `cargo test` matrix.
//!
//! ONE Postgres test function, for the reason `embedded_smoke.rs` gives: the broker is
//! booted once, so a single flow drives a single instance.
//!
//! ```bash
//! docker run --rm -d --name queen-streams-pg -e POSTGRES_PASSWORD=postgres -p 5490:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5490 cargo test --test streams_tenant_isolation -- --ignored --nocapture
//! ```

use queen::{Broker, BrokerConfig};
use serde_json::json;
use std::path::{Path, PathBuf};
use tokio_postgres::Client;

/// The tenant every pre-tenancy caller lands in. It is never gate-checked and must never
/// need a `queen_streams.quota` row — that is the whole of the OSS story.
const TENANT_DEFAULT: &str = "00000000-0000-0000-0000-000000000001";
/// A and B share EVERY name. Neither is the default one, so a missing tenant leg cannot
/// pass by landing there.
const TENANT_A: &str = "aaaaaaaa-0000-4000-8000-0000000000a5";
const TENANT_B: &str = "bbbbbbbb-0000-4000-8000-0000000000b5";
/// C owns nothing to start with: it is the tenant that proves "not mine" reads as absent,
/// and it is where the grant ladder (no row -> enabled -> revoked) is walked.
const TENANT_C: &str = "cccccccc-0000-4000-8000-0000000000c5";
/// D exists only to be capped. Its own section is the only place `max_queries` is set.
const TENANT_D: &str = "dddddddd-0000-4000-8000-0000000000d5";

/// The query name A, B and the DEFAULT tenant all register. Per-tenant uniqueness means
/// nothing unless something actually collides.
const QNAME: &str = "orders.per_customer_per_min";
/// The sink queue name A emits to and B pre-creates. Queue identity is `(tenant_id,
/// name)`, so this is the collision a bare-name sink resolve would fan across.
const SINK: &str = "orders.totals_per_customer_per_min";

// ------------------------------------------------------------------------ plumbing

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

/// Section banner: this is a single test function (see the header), so the log line is
/// what names the failing scenario.
fn section(title: &str) {
    println!("\n──── {title}");
}

fn text(v: &serde_json::Value, k: &str) -> String {
    v.get(k)
        .and_then(|x| x.as_str())
        .unwrap_or_else(|| panic!("result has no string field `{k}`: {v}"))
        .to_string()
}

fn ok(v: &serde_json::Value) -> bool {
    v.get("success").and_then(|x| x.as_bool()).unwrap_or(false)
}

/// `denied` is the ONLY thing that separates a 403 from a 409 in the handler, so its
/// ABSENCE is as load-bearing as its presence and is asserted as such throughout.
fn denied(v: &serde_json::Value) -> bool {
    v.get("denied").and_then(|x| x.as_bool()).unwrap_or(false)
}

// ------------------------------------------------------------------ the SP wrappers
//
// Every wrapper takes `tenant: Option<&str>`. `None` issues the ONE-ARGUMENT call — the
// pre-tenancy signature — which is not a convenience: 008/009/007 each DROP that overload
// and re-create a defaulted two-argument one, and if the DROP were skipped the call would
// fail as ambiguous ("function is not unique") rather than resolve. Exercising it is the
// only way to notice.
//
// All three SPs take a JSONB ARRAY of requests and return `[{idx, result}]`; the wrappers
// unwrap element 0, exactly as the handlers do.

const REG_T: &str = "SELECT COALESCE(((queen.streams_register_query_v1($1::text::jsonb, \
                     $2::text::uuid))->0->'result')::text, 'null')";
const REG_1: &str = "SELECT COALESCE(((queen.streams_register_query_v1($1::text::jsonb))\
                     ->0->'result')::text, 'null')";

const GET_T: &str = "SELECT COALESCE(((queen.streams_state_get_v1($1::text::jsonb, \
                     $2::text::uuid))->0->'result')::text, 'null')";
const GET_1: &str = "SELECT COALESCE(((queen.streams_state_get_v1($1::text::jsonb))\
                     ->0->'result')::text, 'null')";

const CYC_T: &str = "SELECT COALESCE(((queen.log_streams_cycle_v1($1::text::jsonb, \
                     $2::text::uuid))->0->'result')::text, 'null')";
const CYC_1: &str = "SELECT COALESCE(((queen.log_streams_cycle_v1($1::text::jsonb))\
                     ->0->'result')::text, 'null')";

/// One request element, stamped `idx: 0` and wrapped in the one-element array the handlers
/// build.
fn one(mut element: serde_json::Value) -> String {
    element["idx"] = json!(0);
    serde_json::Value::Array(vec![element]).to_string()
}

fn parse(txt: String, what: &str) -> serde_json::Value {
    serde_json::from_str(&txt).unwrap_or_else(|e| panic!("{what} result is not JSON: {e}"))
}

async fn call(c: &Client, sql_t: &str, sql_1: &str, tenant: Option<&str>, body: String) -> String {
    let row = match tenant {
        Some(t) => c.query_one(sql_t, &[&body, &t]).await,
        None => c.query_one(sql_1, &[&body]).await,
    }
    .unwrap_or_else(|e| panic!("SP call failed: {e}\n  body: {body}"));
    row.get(0)
}

async fn register(c: &Client, tenant: Option<&str>, req: serde_json::Value) -> serde_json::Value {
    parse(call(c, REG_T, REG_1, tenant, one(req)).await, "register")
}

async fn state_get(c: &Client, tenant: Option<&str>, req: serde_json::Value) -> serde_json::Value {
    parse(call(c, GET_T, GET_1, tenant, one(req)).await, "state_get")
}

async fn cycle(c: &Client, tenant: Option<&str>, req: serde_json::Value) -> serde_json::Value {
    parse(call(c, CYC_T, CYC_1, tenant, one(req)).await, "cycle")
}

/// A cycle that also emits ONE broker-prepacked sink segment.
///
/// The segment is built in SQL rather than in Rust for the reason `timers_support::fire`
/// builds its segments there: the blob is stored opaquely (`log_segments.blob` has no
/// reader in SQL) and the hash blob only has to satisfy `log_push_one_v1`'s stride guard —
/// 16 bytes per message, which `decode(md5(txn), 'hex')` is exactly. Anything else would
/// be this test re-implementing the packer instead of exercising the SP.
async fn cycle_with_sink(
    c: &Client,
    tenant: &str,
    req: serde_json::Value,
    sink_queue: &str,
    sink_partition: &str,
    txn: &str,
) -> serde_json::Value {
    let body = one(req);
    let row = c
        .query_one(
            "SELECT COALESCE(((queen.log_streams_cycle_v1(
                 jsonb_build_array(
                     ($1::text::jsonb -> 0) || jsonb_build_object(
                         'sink_segments', jsonb_build_array(jsonb_build_object(
                             'queue',     $2::text,
                             'partition', $3::text,
                             'hashesB64', encode(decode(md5($4::text), 'hex'), 'base64'),
                             'blobB64',   encode(convert_to('queen-streams-test-blob', 'UTF8'),
                                                 'base64'),
                             'count',     1)))),
                 $5::text::uuid))->0->'result')::text, 'null')",
            &[&body, &sink_queue, &sink_partition, &txn, &tenant],
        )
        .await
        .unwrap_or_else(|e| panic!("cycle_with_sink failed: {e}\n  body: {body}"));
    parse(row.get(0), "cycle_with_sink")
}

fn upsert(key: &str, value: serde_json::Value) -> serde_json::Value {
    json!({"type": "upsert", "key": key, "value": value})
}

// ---------------------------------------------------------------- table inspection

/// The grant row. Nothing in the broker writes one — it is the control plane's — so every
/// test that needs a granted tenant writes it here, by hand, exactly as the control plane
/// would.
async fn grant(c: &Client, tenant: &str, enabled: bool, max_queries: Option<i64>) {
    c.execute(
        "INSERT INTO queen_streams.quota(tenant_id, enabled, max_queries)
             VALUES ($1::text::uuid, $2, $3)
         ON CONFLICT (tenant_id) DO UPDATE SET enabled = EXCLUDED.enabled,
                                               max_queries = EXCLUDED.max_queries,
                                               updated_at = now()",
        &[&tenant, &enabled, &max_queries],
    )
    .await
    .expect("grant");
}

async fn has_grant(c: &Client, tenant: &str) -> i64 {
    c.query_one(
        "SELECT count(*) FROM queen_streams.quota WHERE tenant_id = $1::text::uuid",
        &[&tenant],
    )
    .await
    .expect("has_grant")
    .get(0)
}

/// `(id, config_hash)` of one tenant's query by name — `None` when that tenant has none,
/// which is a different fact from "nobody has one".
async fn query_row(c: &Client, tenant: &str, name: &str) -> Option<(String, String)> {
    c.query(
        "SELECT id::text, config_hash FROM queen_streams.queries
          WHERE tenant_id = $1::text::uuid AND name = $2",
        &[&tenant, &name],
    )
    .await
    .expect("query_row")
    .first()
    .map(|r| (r.get(0), r.get(1)))
}

async fn query_count(c: &Client, tenant: &str) -> i64 {
    c.query_one(
        "SELECT count(*) FROM queen_streams.queries WHERE tenant_id = $1::text::uuid",
        &[&tenant],
    )
    .await
    .expect("query_count")
    .get(0)
}

async fn scalar(c: &Client, sql: &str, args: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> i64 {
    c.query_one(sql, args).await.unwrap_or_else(|e| panic!("{sql}: {e}")).get(0)
}

/// State keys under one `(query_id, partition_id)` shard, read straight from the table —
/// deliberately NOT through `streams_state_get_v1`, because several assertions below are
/// about what that SP hides, and a probe built on the thing under test cannot see it.
async fn state_keys(c: &Client, query_id: &str, partition_id: &str) -> Vec<String> {
    c.query(
        "SELECT key FROM queen_streams.state
          WHERE query_id = $1::text::uuid AND partition_id = $2::text::uuid
          ORDER BY key",
        &[&query_id, &partition_id],
    )
    .await
    .expect("state_keys")
    .iter()
    .map(|r| r.get(0))
    .collect()
}

/// `queen.log_partitions.id` for `(tenant, queue, partition)`.
async fn partition_of(c: &Client, tenant: &str, queue: &str, partition: &str) -> String {
    c.query_one(
        "SELECT p.id::text FROM queen.log_partitions p
           JOIN queen.queues q ON q.id = p.queue_id
          WHERE q.tenant_id = $1::text::uuid AND q.name = $2 AND p.name = $3",
        &[&tenant, &queue, &partition],
    )
    .await
    .unwrap_or_else(|e| panic!("partition {tenant}/{queue}/{partition}: {e}"))
    .get(0)
}

/// Segments and messages actually in the log for one partition — the only thing that can
/// tell "the sink emit landed here" from "the sink emit landed next door".
async fn log_state(c: &Client, partition_id: &str) -> (i64, i64) {
    let r = c
        .query_one(
            "SELECT count(*)::bigint,
                    COALESCE(sum(s.end_offset - s.base_offset + 1), 0)::bigint
               FROM queen.log_segments s WHERE s.partition_id = $1::text::uuid",
            &[&partition_id],
        )
        .await
        .expect("log_state");
    (r.get(0), r.get(1))
}

/// Provision a queue and one partition under `tenant` through the real writers, so the
/// rows carry whatever the engine stamps on them.
async fn make_queue(c: &Client, tenant: &str, queue: &str) {
    c.execute(
        "SELECT queen.configure_queue_v1($1, '{}'::jsonb, $2::text::uuid)",
        &[&queue, &tenant],
    )
    .await
    .expect("configure_queue_v1");
    c.execute(
        "SELECT queen.log_push_one_v1($1, 'Default', 1, decode(repeat('ab',16),'hex'), 0,
                                      convert_to('seed', 'UTF8'), NULL, NULL, $2::text::uuid)",
        &[&queue, &tenant],
    )
    .await
    .expect("log_push_one_v1");
}

// ================================================================= the Postgres test

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn two_tenants_sharing_every_streaming_name_share_nothing_else() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG").expect(
        "QUEEN_EMBEDDED_TEST_PG must be set (host:port) for the streams isolation test",
    );
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // These tests need to be the only writer: the sweeper would fire timers, and retention
    // / the stats refresh would rewrite rows the segment counts below compare.
    std::env::set_var("QUEEN_SWEEPER", "false");

    // Booting the real broker is what applies the real schema. The SQL is
    // include_str!-embedded, so this is also the only proof that the 002/007/008/009 files
    // on disk are the ones the binary carries — a psql script would pass against a stale
    // build.
    let broker = Broker::start(
        BrokerConfig::new()
            .pg(host.clone(), port, "postgres", "postgres", "postgres")
            .pool_size(4)
            .retention(false)
            .stats_refresh(false)
            .system_metrics(false),
    )
    .await
    .expect("broker start");

    let c = connect(&host, port).await;

    // A previous run's rows would make every count below order-dependent noise. These
    // tests own the throwaway Postgres they are pointed at, which is what makes a blanket
    // clear acceptable.
    for t in [TENANT_A, TENANT_B, TENANT_C, TENANT_D, TENANT_DEFAULT] {
        c.execute(
            "SELECT queen.delete_tenant_data_v1($1::text::uuid, 1000, 1000000, true)",
            &[&t],
        )
        .await
        .expect("pre-clean");
    }

    // ========================================================================
    section("setup: the same queue names under two tenants, both granted");
    // ========================================================================
    grant(&c, TENANT_A, true, None).await;
    grant(&c, TENANT_B, true, None).await;
    make_queue(&c, TENANT_A, "orders").await;
    make_queue(&c, TENANT_B, "orders").await;
    // B pre-creates the SINK queue A will emit to. A does not: A's is auto-provisioned by
    // the cycle, which is the half of §5 that has to stamp the caller's tenant.
    make_queue(&c, TENANT_B, SINK).await;

    let pid_a = partition_of(&c, TENANT_A, "orders", "Default").await;
    let pid_b = partition_of(&c, TENANT_B, "orders", "Default").await;
    assert_ne!(pid_a, pid_b, "same queue NAME, two queue rows, two partitions");
    let b_sink_pid = partition_of(&c, TENANT_B, SINK, "Default").await;
    let b_sink_before = log_state(&c, &b_sink_pid).await;
    assert_eq!(b_sink_before.0, 1, "B's sink starts with the one segment its seed push made");

    // ========================================================================
    section("1. the same query NAME under two tenants is two queries");
    // Uniqueness moved from (name) to (tenant_id, name) — 002's queries_tenant_name_uk.
    // Before that, the second tenant to register this name did not get a query, it got a
    // 409 pointing at a row it could not see.
    // ========================================================================
    let ra = register(
        &c,
        Some(TENANT_A),
        json!({"name": QNAME, "source_queue": "orders", "config_hash": "hash-A"}),
    )
    .await;
    assert!(ok(&ra), "A registers its own name: {ra}");
    assert_eq!(ra["fresh"], json!(true), "a first registration is fresh: {ra}");
    let ida = text(&ra, "query_id");

    let rb = register(
        &c,
        Some(TENANT_B),
        json!({"name": QNAME, "source_queue": "orders", "config_hash": "hash-B"}),
    )
    .await;
    assert!(
        ok(&rb),
        "B registers the SAME name with a DIFFERENT config_hash and must not collide: {rb}"
    );
    assert_eq!(
        rb["fresh"],
        json!(true),
        "and it is B's OWN fresh row, not an update of A's: {rb}"
    );
    let idb = text(&rb, "query_id");
    assert_ne!(ida, idb, "two names that are equal, two queries that are not");

    assert_eq!(
        scalar(&c, "SELECT count(*) FROM queen_streams.queries WHERE name = $1", &[&QNAME]).await,
        2,
        "two rows for one name: the pre-tenancy global UNIQUE(name) would have collapsed \
         them into one — and 002 drops that constraint AND a same-named hand-made index, \
         because either one still enforces it"
    );
    assert_eq!(
        query_row(&c, TENANT_A, QNAME).await.expect("A's row").1,
        "hash-A",
        "B's registration did not overwrite A's config_hash"
    );
    assert_eq!(query_row(&c, TENANT_B, QNAME).await.expect("B's row").1, "hash-B");

    // The conflict, which is where the pre-tenancy leak was: the result hands back the
    // EXISTING row's query_id, and with a bare-name lookup that was the OTHER owner's.
    let conflict = register(
        &c,
        Some(TENANT_A),
        json!({"name": QNAME, "source_queue": "orders", "config_hash": "hash-CHANGED"}),
    )
    .await;
    assert!(!ok(&conflict), "a changed operator chain must not silently rebind: {conflict}");
    assert!(
        text(&conflict, "error").contains("config_hash mismatch"),
        "the refusal must say what changed: {conflict}"
    );
    assert_eq!(
        text(&conflict, "query_id"),
        ida,
        "the conflict reveals the CALLER'S OWN query_id and never B's — pre-tenancy this \
         same line handed a bare-name collision the other owner's internal UUID: {conflict}"
    );
    assert!(
        conflict.get("denied").is_none(),
        "a config_hash conflict must carry NO `denied` key: that flag is the only thing \
         separating the handler's 403 from its 409, and a client retrying a 409 with \
         reset:true is doing the right thing while doing it against a 403 is forever: \
         {conflict}"
    );
    assert_eq!(
        query_row(&c, TENANT_A, QNAME).await.expect("A").1,
        "hash-A",
        "a refused registration changes nothing"
    );
    assert_eq!(query_row(&c, TENANT_B, QNAME).await.expect("B").1, "hash-B", "B untouched");

    // ========================================================================
    section("2. owner writes state through the real cycle; the foreigner reads nothing");
    // Same partition-shard shape under both tenants, same state key. The cycle is used
    // rather than an INSERT because `queen_streams.state` carries no tenant column: it is
    // attributable ONLY through the query FK the register path stamped, so a hand-written
    // row would be the test asserting its own idea of ownership.
    // ========================================================================
    let wa = cycle(
        &c,
        Some(TENANT_A),
        json!({"query_id": ida, "partition_id": pid_a,
               "state_ops": [upsert("customer-42", json!({"total": 1}))]}),
    )
    .await;
    assert!(ok(&wa), "A's own cycle on A's own partition: {wa}");
    assert_eq!(wa["state_ops_applied"], json!(1), "{wa}");
    assert_eq!(
        wa["queueName"], json!("orders"),
        "the source queue name is resolved under the caller's tenant: {wa}"
    );

    let wb = cycle(
        &c,
        Some(TENANT_B),
        json!({"query_id": idb, "partition_id": pid_b,
               "state_ops": [upsert("customer-42", json!({"total": 2}))]}),
    )
    .await;
    assert!(ok(&wb), "B's own cycle: {wb}");

    assert_eq!(
        scalar(
            &c,
            "SELECT count(*) FROM queen_streams.state WHERE key = 'customer-42'",
            &[],
        )
        .await,
        2,
        "one key, two shards: the state PK is (query_id, partition_id, key) and the two \
         query ids are what keep them apart"
    );

    let ga = state_get(&c, Some(TENANT_A), json!({"query_id": ida, "partition_id": pid_a})).await;
    assert!(ok(&ga), "{ga}");
    assert_eq!(ga["rows"].as_array().map(|r| r.len()), Some(1), "A reads its own: {ga}");
    assert_eq!(ga["rows"][0]["value"]["total"], json!(1), "and gets A's value: {ga}");

    let gb = state_get(&c, Some(TENANT_B), json!({"query_id": idb, "partition_id": pid_b})).await;
    assert_eq!(gb["rows"][0]["value"]["total"], json!(2), "B reads B's: {gb}");

    // THE NO-ORACLE HALF: B asks for A's exact (query_id, partition_id).
    let foreign = state_get(&c, Some(TENANT_B), json!({"query_id": ida, "partition_id": pid_a}))
        .await;
    assert!(
        ok(&foreign),
        "a foreign read is not an ERROR — 009 writes ownership as a PREDICATE, not a \
         pre-check, so that this answer is INDISTINGUISHABLE from a key that was never \
         written: {foreign}"
    );
    assert_eq!(
        foreign["rows"],
        json!([]),
        "...and the predicate must actually filter: {foreign}"
    );
    let stranger =
        state_get(&c, Some(TENANT_C), json!({"query_id": ida, "partition_id": pid_a})).await;
    assert_eq!(
        stranger["rows"],
        json!([]),
        "a tenant that owns nothing at all reads empty, never an error and never a \
         neighbour's window: {stranger}"
    );
    assert_eq!(
        state_keys(&c, &ida, &pid_a).await,
        vec!["customer-42".to_string()],
        "and the row B could not see is still exactly where A left it — an empty answer is \
         a filter, not a fact about existence"
    );

    // ========================================================================
    section("3. the cycle: foreign pid, foreign query, bogus pid — and the owner's sink");
    // Both addresses are raw uuids off the wire. The state ops below are aimed at the
    // OTHER tenant's shard on purpose: if either gate is missing they land, and nothing
    // anywhere raises.
    // ========================================================================
    let foreign_pid = cycle(
        &c,
        Some(TENANT_B),
        json!({"query_id": idb, "partition_id": pid_a,
               "state_ops": [upsert("pwned-by-pid", json!({"total": 666}))]}),
    )
    .await;
    assert!(!ok(&foreign_pid), "B must not cycle on A's partition: {foreign_pid}");
    assert_eq!(
        text(&foreign_pid, "error"),
        "partition not found",
        "generic on purpose: 'not found' is the same answer for a missing partition and \
         for somebody else's, so a cycle is not an existence oracle either: {foreign_pid}"
    );

    let foreign_query = cycle(
        &c,
        Some(TENANT_B),
        json!({"query_id": ida, "partition_id": pid_b,
               "state_ops": [upsert("pwned-by-query", json!({"total": 666}))]}),
    )
    .await;
    assert!(
        !ok(&foreign_query),
        "B owns this partition but not this query — the state ops are keyed by query_id \
         ALONE, so without the query gate they would write into A's shard: {foreign_query}"
    );
    assert_eq!(text(&foreign_query, "error"), "query not found", "{foreign_query}");

    let bogus = cycle(
        &c,
        Some(TENANT_A),
        json!({"query_id": ida, "partition_id": "00000000-0000-4000-8000-00000000dead",
               "state_ops": [upsert("pwned-by-bogus", json!({"total": 666}))]}),
    )
    .await;
    assert!(!ok(&bogus), "{bogus}");
    assert_eq!(
        text(&bogus, "error"),
        "partition not found",
        "pre-tenancy this lookup fed a metric label and NOT FOUND was silently fine, so a \
         bogus uuid ran the whole cycle and left state attributed to no partition at all: \
         {bogus}"
    );

    assert_eq!(
        scalar(
            &c,
            "SELECT count(*) FROM queen_streams.state WHERE key LIKE 'pwned-%'",
            &[],
        )
        .await,
        0,
        "not one of the three refused cycles wrote a state row — the per-element savepoint \
         is what makes a RAISE the right failure mode here"
    );

    // The owner's cycle, with the two things a refused one must never get to do: a state
    // op and a sink emit.
    let emit = cycle_with_sink(
        &c,
        TENANT_A,
        json!({"query_id": ida, "partition_id": pid_a,
               "state_ops": [upsert("customer-43", json!({"total": 7}))]}),
        SINK,
        "Default",
        "txn-a-sink-1",
    )
    .await;
    assert!(ok(&emit), "A's own cycle with a sink must commit: {emit}");
    assert_eq!(emit["state_ops_applied"], json!(1), "{emit}");
    assert_eq!(
        emit["push_results"][0]["status"],
        json!("queued"),
        "the inline sink push is the same allocator path as a plain push: {emit}"
    );

    // The sink queue did not exist under A. The cycle's lazy provisioning INSERT has to
    // STAMP the caller's tenant — it used to take the column default, which is right only
    // for the default tenant.
    let sink_owners: Vec<String> = c
        .query(
            "SELECT tenant_id::text FROM queen.queues WHERE name = $1 ORDER BY tenant_id::text",
            &[&SINK],
        )
        .await
        .expect("sink owners")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    let mut want = vec![TENANT_A.to_string(), TENANT_B.to_string()];
    want.sort();
    assert_eq!(
        sink_owners, want,
        "the auto-created sink must be A's OWN queue row: one name, two rows, one per \
         tenant. A single row here means the emit went into whichever tenant's queue the \
         bare name resolved to"
    );
    let a_sink_pid = partition_of(&c, TENANT_A, SINK, "Default").await;
    assert_eq!(
        log_state(&c, &a_sink_pid).await,
        (1, 1),
        "A's sink holds exactly the one frame this cycle emitted"
    );
    assert_eq!(
        log_state(&c, &b_sink_pid).await,
        b_sink_before,
        "B's identically-named sink did not grow by A's message. A bare-name join here is \
         not merely unscoped — it MULTI-MATCHES, fanning one emit across the same-named \
         queue of every tenant on the cell (and pre-locking their partitions on the way)"
    );

    // ========================================================================
    section("4. reset wipes the caller's state and nobody else's");
    // The reset path is the safety net for an operator-shape change, and its state delete
    // carries no tenant predicate of its own — it is scoped by the query_id the
    // tenant-scoped lookup returned. That is correct only while the lookup is scoped.
    // ========================================================================
    assert_eq!(state_keys(&c, &ida, &pid_a).await.len(), 2, "A has two keys to lose");
    assert_eq!(state_keys(&c, &idb, &pid_b).await.len(), 1, "B has one to keep");

    let reset = register(
        &c,
        Some(TENANT_A),
        json!({"name": QNAME, "source_queue": "orders", "config_hash": "hash-A2",
               "reset": true}),
    )
    .await;
    assert!(ok(&reset), "reset:true is the way through a config_hash conflict: {reset}");
    assert_eq!(reset["reset"], json!(true), "{reset}");
    assert_eq!(text(&reset, "query_id"), ida, "and it is still the same query: {reset}");
    assert!(
        state_keys(&c, &ida, &pid_a).await.is_empty(),
        "A's state is gone, which is what A asked for"
    );
    assert_eq!(
        state_keys(&c, &idb, &pid_b).await,
        vec!["customer-42".to_string()],
        "B's identically-keyed state under its identically-named query is NOT: one DELETE, \
         one query id, one tenant"
    );
    assert_eq!(query_row(&c, TENANT_A, QNAME).await.expect("A").1, "hash-A2");
    assert_eq!(
        query_row(&c, TENANT_B, QNAME).await.expect("B").1,
        "hash-B",
        "and B's operator chain was not re-fingerprinted by A's redeploy"
    );

    // ========================================================================
    section("5. the grant: absence is a denial, and revocation never stops a drain");
    // ========================================================================
    assert_eq!(has_grant(&c, TENANT_C).await, 0, "C starts with no grant row");
    let ungranted = register(
        &c,
        Some(TENANT_C),
        json!({"name": QNAME, "source_queue": "orders", "config_hash": "hash-C"}),
    )
    .await;
    assert!(!ok(&ungranted), "{ungranted}");
    assert!(
        denied(&ungranted),
        "the refusal must carry `denied` — that is the flag the handler turns into a 403, \
         and nothing the caller can change in the request fixes it: {ungranted}"
    );
    assert_eq!(
        text(&ungranted, "error"),
        "streams not granted for this tenant",
        "{ungranted}"
    );
    assert_eq!(
        query_count(&c, TENANT_C).await,
        0,
        "a denied registration inserts nothing — fail-open here would mint an unlimited \
         tenant for any invented header value"
    );

    grant(&c, TENANT_C, true, None).await;
    let qc = format!("{QNAME}.c");
    let rc = register(
        &c,
        Some(TENANT_C),
        json!({"name": qc, "source_queue": "orders", "config_hash": "hash-C"}),
    )
    .await;
    assert!(ok(&rc), "a granted tenant registers: {rc}");
    let idc = text(&rc, "query_id");

    // Revoked. NEW queries stop; the query already draining does not.
    grant(&c, TENANT_C, false, None).await;
    let revoked_new = register(
        &c,
        Some(TENANT_C),
        json!({"name": format!("{qc}.second"), "source_queue": "orders",
               "config_hash": "hash-C2"}),
    )
    .await;
    assert!(denied(&revoked_new), "enabled=false denies a NEW name: {revoked_new}");
    assert_eq!(
        text(&revoked_new, "error"),
        "streams not granted for this tenant",
        "{revoked_new}"
    );

    let redeploy = register(
        &c,
        Some(TENANT_C),
        json!({"name": qc, "source_queue": "orders", "config_hash": "hash-C"}),
    )
    .await;
    assert!(
        ok(&redeploy),
        "a plain redeploy of an EXISTING query is never gate-checked: revoking a grant \
         must stop new queries and never a Runner that is already draining, or the \
         billing decision strands a source backlog nobody can consume — the same drain \
         posture the proxy's GatedOp documents for the cycle route: {redeploy}"
    );
    assert!(!denied(&redeploy), "{redeploy}");
    assert_eq!(text(&redeploy, "query_id"), idc, "and it is the same query: {redeploy}");

    let revoked_reset = register(
        &c,
        Some(TENANT_C),
        json!({"name": qc, "source_queue": "orders", "config_hash": "hash-C-new",
               "reset": true}),
    )
    .await;
    assert!(
        ok(&revoked_reset),
        "and neither is the reset path: it is how an existing query recovers from an \
         operator-shape change, not how a new one is created: {revoked_reset}"
    );
    assert_eq!(revoked_reset["reset"], json!(true), "{revoked_reset}");
    assert_eq!(
        query_count(&c, TENANT_C).await,
        1,
        "C still owns exactly the one query it was granted, and gained none while revoked"
    );

    // ========================================================================
    section("6. max_queries is exact, and counts the tenant's OWN rows");
    // ========================================================================
    grant(&c, TENANT_D, true, Some(2)).await;
    // The global row count already exceeds D's cap. If the count feeding the cap were a
    // global one, D would be denied its FIRST query — so this line is what makes the two
    // registrations below evidence rather than coincidence.
    let global_before = scalar(&c, "SELECT count(*) FROM queen_streams.queries", &[]).await;
    assert!(
        global_before > 2,
        "the fixture needs more queries on the cell than D's cap, got {global_before}"
    );

    for n in 1..=2 {
        let r = register(
            &c,
            Some(TENANT_D),
            json!({"name": format!("d.query.{n}"), "source_queue": "orders",
                   "config_hash": format!("hash-D{n}")}),
        )
        .await;
        assert!(ok(&r), "D's query {n} is within a cap of 2: {r}");
    }
    assert_eq!(query_count(&c, TENANT_D).await, 2, "D is exactly at its cap");

    let capped = register(
        &c,
        Some(TENANT_D),
        json!({"name": "d.query.3", "source_queue": "orders", "config_hash": "hash-D3"}),
    )
    .await;
    assert!(!ok(&capped), "{capped}");
    assert!(denied(&capped), "a cap refusal is a 403, not a 409: {capped}");
    assert_eq!(
        text(&capped, "error"),
        "streams query quota exceeded (max 2)",
        "the refusal must name the cap it hit — an operator raising a limit needs to know \
         which one: {capped}"
    );
    assert_eq!(query_count(&c, TENANT_D).await, 2, "and the third row was not inserted");

    let at_cap_redeploy = register(
        &c,
        Some(TENANT_D),
        json!({"name": "d.query.1", "source_queue": "orders", "config_hash": "hash-D1"}),
    )
    .await;
    assert!(
        ok(&at_cap_redeploy),
        "at the cap, an EXISTING query still redeploys — the gate is on the fresh-insert \
         path only: {at_cap_redeploy}"
    );

    // The other direction of the same claim: D sitting at its cap costs A nothing.
    let a_unaffected = register(
        &c,
        Some(TENANT_A),
        json!({"name": "a.another.query", "source_queue": "orders", "config_hash": "hash-A3"}),
    )
    .await;
    assert!(
        ok(&a_unaffected),
        "A has no cap of its own (max_queries NULL = unlimited) and D's exhausted one is \
         not A's: {a_unaffected}"
    );
    // NB there is no delete surface for a registered query, so "the cap frees up again" is
    // out of scope here by construction — the only assertion available about the count
    // BASIS is that it is per-tenant, which is what the two directions above pin.

    // ========================================================================
    section("7. tenancy off: the one-ARGUMENT call, the default tenant, no grant read");
    // The OSS / self-hosted / embedded path. Behaviour must be byte-identical to the
    // pre-tenancy engine, which means two things and they are different: the old CALL
    // still resolves, and the default tenant is never gate-checked.
    // ========================================================================
    assert_eq!(
        has_grant(&c, TENANT_DEFAULT).await,
        0,
        "the default tenant has no grant row, and a self-hosted broker has no control \
         plane to write one"
    );
    make_queue(&c, TENANT_DEFAULT, "orders").await;
    let pid_def = partition_of(&c, TENANT_DEFAULT, "orders", "Default").await;

    // One argument: the pre-tenancy signature. It resolves only because 008 DROPped that
    // overload — leaving it in the catalog makes this call ambiguous ("function is not
    // unique"), not convenient.
    let rdef = register(
        &c,
        None,
        json!({"name": QNAME, "source_queue": "orders", "config_hash": "hash-DEFAULT"}),
    )
    .await;
    assert!(
        ok(&rdef),
        "a caller that names no tenant registers the same name a third time, and reads no \
         grant row to do it: {rdef}"
    );
    let id_def = text(&rdef, "query_id");
    assert!(id_def != ida && id_def != idb, "three names that are equal, three queries");
    assert_eq!(
        has_grant(&c, TENANT_DEFAULT).await,
        0,
        "and it neither needed a grant row nor created one — the default tenant skips the \
         table entirely, so there is nothing to configure and nothing that can be revoked"
    );

    let cdef = cycle(
        &c,
        None,
        json!({"query_id": id_def, "partition_id": pid_def,
               "state_ops": [upsert("customer-42", json!({"total": 0}))]}),
    )
    .await;
    assert!(ok(&cdef), "the one-argument cycle: {cdef}");
    let gdef = state_get(&c, None, json!({"query_id": id_def, "partition_id": pid_def})).await;
    assert_eq!(gdef["rows"][0]["value"]["total"], json!(0), "the one-argument read: {gdef}");

    // "Global-feeling" is not "global". The default tenant is the tenant every unlabelled
    // caller lands in, not a superuser over the others.
    let def_peeks_at_a =
        state_get(&c, None, json!({"query_id": ida, "partition_id": pid_a})).await;
    assert_eq!(
        def_peeks_at_a["rows"],
        json!([]),
        "the default tenant cannot read A's shard: {def_peeks_at_a}"
    );
    let a_peeks_at_def =
        state_get(&c, Some(TENANT_A), json!({"query_id": id_def, "partition_id": pid_def})).await;
    assert_eq!(
        a_peeks_at_def["rows"],
        json!([]),
        "and A cannot read the default tenant's: {a_peeks_at_def}"
    );
    let def_cycles_at_a = cycle(
        &c,
        None,
        json!({"query_id": id_def, "partition_id": pid_a,
               "state_ops": [upsert("pwned-by-default", json!({"total": 666}))]}),
    )
    .await;
    assert_eq!(
        text(&def_cycles_at_a, "error"),
        "partition not found",
        "nor cycle on it: {def_cycles_at_a}"
    );
    assert_eq!(
        scalar(&c, "SELECT count(*) FROM queen_streams.state WHERE key LIKE 'pwned-%'", &[]).await,
        0,
        "still nothing written by anyone who did not own both ends"
    );

    let _ = broker.shutdown().await;
    println!("\nOK — two tenants sharing every streaming name share nothing else.");
}

// ====================================================== the handler-level source pin

fn manifest(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(rel)
}

/// The HTTP half of §5, pinned the only way this directory can reach it.
///
/// `src/handlers/streams.rs` is dispatched from the router in `src/main.rs` — the BINARY's
/// crate root. `src/lib.rs` compiles the same module tree but keeps `mod handlers`
/// private, and the embedded facade (`queen::Broker`) exposes no streams surface at all,
/// so an integration test here cannot issue a request at those three handlers: there is no
/// router to bind and no public function to call. Everything the Postgres test above
/// asserts is therefore SP-level.
///
/// What is left over is exactly the mapping, and it is worth pinning because each half of
/// it is one word:
///
///   * `Extension<Tenant>` on all three handlers — drop it and the SPs are called with a
///     default that no request can influence, which reads as "isolation off", silently.
///   * `denied` -> 403 and everything else with `success:false` -> 409. Conflating them
///     tells a client to retry with `reset:true` against a grant refusal, forever.
///   * `tenant_owns_partition` -> 404 `{"error":"not found"}` on the two pid-addressed
///     routes. A 403 there would confirm the partition exists, which is the oracle the
///     generic SP messages are written to avoid.
///
/// Same mechanism and same rationale as `kv_handler_isolation.rs`: ten lines of grep are
/// cheaper than a reviewer remembering. Needs no Postgres, so it runs in the plain
/// `cargo test` matrix.
#[test]
fn handler_source_pins_the_status_map() {
    let src = std::fs::read_to_string(manifest("src/handlers/streams.rs"))
        .expect("read src/handlers/streams.rs");

    assert_eq!(
        src.matches("Extension(tenant): Extension<crate::tenant::Tenant>").count(),
        3,
        "all THREE streams handlers (register, state_get, cycle) must take the request \
         tenant as an extractor — a handler that does not cannot scope anything, and the \
         SP it calls would silently fall back to the default tenant"
    );
    for wrapper in ["db::streams_register(", "db::streams_state_get(", "db::streams_cycle("] {
        assert!(
            src.contains(wrapper),
            "src/handlers/streams.rs must reach the SPs through the `{wrapper}` wrapper in \
             src/db.rs — that wrapper is the only place allowed to bind p_tenant"
        );
    }
    assert!(
        src.matches("tenant.as_str()").count() >= 3,
        "each handler must PASS the extracted tenant down; an unused extractor is the same \
         bug with an extra line"
    );

    // register: the two failure classes must stay distinguishable.
    assert!(
        src.contains("StatusCode::FORBIDDEN"),
        "a grant/quota refusal (`denied`: true) must map to 403 — nothing the caller can \
         change in the request fixes it"
    );
    assert!(
        src.contains("StatusCode::CONFLICT"),
        "a config_hash mismatch must map to 409 — the status a client answers with \
         reset:true"
    );
    assert!(
        src.contains("get(\"denied\")"),
        "the 403/409 split must be driven by the SP's `denied` flag, not by string-matching \
         an error message"
    );

    // the two pid-addressed routes: 404, never 403, and never a 200 that skipped the gate.
    assert_eq!(
        src.matches("tenant_owns_partition(").count(),
        2,
        "state_get and cycle are addressed by a raw partition uuid and BOTH need the \
         ownership pre-gate; register is not pid-addressed and must not have one"
    );
    // Counted separately from the body literal so a reformat of the call cannot quietly
    // turn this into a match on nothing.
    assert_eq!(
        src.matches("StatusCode::NOT_FOUND").count(),
        2,
        "a foreign partition must get the SAME 404 a missing one gets, on BOTH gated \
         routes. A 403 confirms it exists, which is precisely the oracle 007's generic \
         'partition not found' is written to deny"
    );
    assert_eq!(
        src.matches("{\\\"error\\\":\\\"not found\\\"}").count(),
        2,
        "and the 404 body must say nothing more than `not found` — a message that named \
         the tenant, the partition or the reason would put the oracle back"
    );

    // The state_get gate is deliberately skipped when partition_id is absent, so the SP's
    // own 400 ("query_id and partition_id are required") survives instead of being
    // replaced by a 404 that tells the caller nothing.
    assert!(
        src.contains("!partition_id.is_empty()"),
        "state_get must skip the ownership gate when partition_id is ABSENT: there is \
         nothing to own, and turning a malformed body into a 404 would swallow the SP's \
         400"
    );
}

/// The other half of the same rule, and the half a grep over the handlers cannot give: the
/// wrappers have to exist, and they have to bind the tenant into the SP call. Without
/// this, "the handler passes tenant.as_str() to db::streams_register" is satisfied by a
/// wrapper that drops it on the floor.
#[test]
fn the_streams_wrappers_bind_the_tenant_in_db_rs() {
    let db = std::fs::read_to_string(manifest("src/db.rs")).expect("read src/db.rs");
    for sp in [
        "queen.streams_register_query_v1",
        "queen.streams_state_get_v1",
        "queen.log_streams_cycle_v1",
    ] {
        // The SP name appears in the route comment above each wrapper too, so match the
        // STATEMENT: the only non-comment line that names it is the SELECT.
        let stmt = db
            .lines()
            .find(|l| l.contains(sp) && !l.trim_start().starts_with("//"))
            .unwrap_or_else(|| panic!("`{sp}` must be called from src/db.rs"));
        assert!(
            stmt.contains("$2::text::uuid"),
            "`{sp}` must be called with the tenant bound as its second argument — the \
             two-argument overload has a DEFAULT, so a one-argument call here compiles, \
             runs, and puts every tenant on the default one. Offending line:\n  {}",
            stmt.trim()
        );
    }
}
