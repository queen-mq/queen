//! Shared harness for the timer contract tests (PLAN_KV_TIMERS.md §15, F3).
//!
//! Not a test target of its own: `tests/timers_support/` is a directory, so cargo does
//! not compile it as a test binary. Each timer test file declares `mod timers_support;`
//! and gets its own copy.
//!
//! ============================================================================
//! CONTRACT ASSUMPTIONS — the ONE place to reconcile with the implementation
//! ============================================================================
//! These tests are written BEFORE the SQL exists, so every name below is the test's
//! reading of the plan, not an observation of the code. Where the plan spells a
//! signature out, it is copied; where it does not, the shape is derived from the
//! nearest in-house precedent and listed here so the implementer changes ONE file
//! instead of three.
//!
//! Fully specified by the plan, copied verbatim:
//!   §6.2  queen.log_timers_apply_v1(p_ops JSONB, p_tenant UUID, p_producer_sub TEXT,
//!                                   p_now TIMESTAMPTZ) RETURNS JSONB
//!   §6.2  queen.log_timers_due_v1(p_shards SMALLINT[], p_now TIMESTAMPTZ, p_cap INT)
//!                                   RETURNS JSONB {nextInMs, due, dueCapped, lateMs, now}
//!   §6.2  queen.log_timers_claim_v1(p_shards, p_now, p_lease_ms, p_max_rows, p_per_tenant)
//!                                   RETURNS TABLE(...)
//!   §3.3  the queen.log_timers columns (seeding writes them directly, the way
//!         hotlist_repairs.rs seeds queen.log_partitions)
//!
//! DERIVED — the plan names the arguments' MEANING but not the signature. If the
//! implementation differs, fix `fire`, `fail` and `dlq` below and nothing else:
//!   log_timers_fire_v1(p_tenants UUID[], p_queues TEXT[], p_partitions TEXT[],
//!                      p_msg_counts INT[], p_hashes BYTEA[], p_blobs BYTEA[],
//!                      p_keys TEXT[], p_seg_of INT[], p_tokens UUID[],
//!                      p_now TIMESTAMPTZ) RETURNS JSONB
//!       Six segment-parallel arrays in the byte-for-byte order of
//!       queen.log_push_multi_v1 (003_log_push.sql:243-251), plus three timer-parallel
//!       arrays carrying §6.2 guard 1's frame→timer map. `p_seg_of` is **1-based**,
//!       because the guard that reads it ("as many keys with seg_of = i as count[i]")
//!       is written in SQL, where arrays are 1-based.
//!   log_timers_fail_v1(p_tenants UUID[], p_queues TEXT[], p_keys TEXT[], p_tokens UUID[],
//!                      p_backoff_ms INT, p_error TEXT, p_count_attempt BOOLEAN,
//!                      p_max_attempts INT, p_now TIMESTAMPTZ) RETURNS JSONB
//!   log_timers_dlq_v1(p_tenants UUID[], p_queues TEXT[], p_keys TEXT[], p_payloads JSONB[],
//!                     p_errors TEXT[], p_min_attempts INT, p_now TIMESTAMPTZ) RETURNS JSONB
//!       No p_partitions: the row already carries it, and §6.2 requires the DLQ to
//!       resolve the partition UNDER THE TIMER'S TENANT from the row itself.
//!   claim column names: `r_timer_key` (§6.2 names `r_late_ms`, so the prefix is `r_`).
//!       Everything else the tests need is read back from queen.log_timers, so the
//!       claim's return type is pinned by exactly one column name.
//!   claim ROW ORDER: the claim must RETURN its rows in `visible_at` order. §4.2 makes
//!       that order the order in which the timers enter the log ("the order is decided at
//!       the fire"), so it is a contract and not an artifact — and note that a plain
//!       `UPDATE ... RETURNING` does NOT preserve it, nor can it order by `visible_at`
//!       AFTER the update, since the claim itself pushes every claimed row's `visible_at`
//!       out to the lease. It takes selecting `FOR UPDATE SKIP LOCKED` into a CTE and
//!       ordering the final SELECT by the value captured there.
//!   op field names on the apply wire: `op`, `queue`, `timerKey`, `partition`,
//!       `delayMs` (§20.6, ratified), `txn` (§20.2, mandatory in schedule),
//!       `payload` (base64 of the BYTEA column), `payloadZstd`, `encrypted`.
//!   `_messageId`: the SERVER-INJECTED message id. §6.2 has the handler mint the id and
//!       put it in the op, while §4.2 makes a client-supplied `messageId` a RAISE 22023 —
//!       both hold only if the injected field has a name a client cannot spell, and the
//!       house already does exactly this with `_tenant` (005_log_ack.sql:925).
//!   kv op field names: `op`, `ns`, `key`, `value`, `ttlSeconds` (§5.1, ratified §20.1).

#![allow(dead_code)]

use queen::{Broker, BrokerConfig};
use tokio_postgres::Client;

/// The default tenant every pre-tenancy caller lands in (§3.3, and the same default as
/// queen.hotlist_repairs).
pub const TENANT_DEFAULT: &str = "00000000-0000-0000-0000-000000000001";
/// Two tenants for the isolation criterion (§15, §13.2). Deliberately NOT the default
/// one, so a missing `tenant_id = p_tenant` leg cannot pass by landing on the default.
pub const TENANT_A: &str = "aaaaaaaa-0000-4000-8000-00000000000a";
pub const TENANT_B: &str = "bbbbbbbb-0000-4000-8000-00000000000b";
/// A third tenant that owns nothing: it exists to prove that "not mine" reads as
/// `absent` and never as someone else's row (§4.4).
pub const TENANT_C: &str = "cccccccc-0000-4000-8000-00000000000c";

/// All 64 shards, the way the sweeper scans them (§1.10: shards spread contention, they
/// do NOT partition ownership, so a claim always names every one of them).
pub const ALL_SHARDS: &str = "(SELECT array_agg(g::smallint) FROM generate_series(0,63) g)";

pub fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

pub struct Rig {
    pub broker: Broker,
    pub c: Client,
}

/// Boot the real broker (which applies the real schema — the SQL is include_str!-embedded,
/// so this is also the proof that 025_log_timers.sql compiled in) and open one plain
/// connection next to it.
///
/// Both feature flags are forced OFF: with them off the sweeper is not spawned at all
/// (§7.1), and these tests need to BE the sweeper. A background fire loop stealing rows
/// out from under a claim would make every assertion here a coin flip.
pub async fn boot() -> Rig {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port) for the timer tests");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    std::env::set_var("QUEEN_TIMERS_ENABLED", "false");
    std::env::set_var("QUEEN_KV_ENABLED", "false");

    let broker = Broker::start(
        BrokerConfig::new()
            .pg(host.clone(), port, "postgres", "postgres", "postgres")
            .pool_size(4),
    )
    .await
    .expect("broker start");

    Rig {
        broker,
        c: connect(&host, port).await,
    }
}

pub async fn connect(host: &str, port: u16) -> Client {
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

/// A second connection to the same database, for the fault that needs a backend of its
/// own to kill.
pub async fn second_connection() -> Client {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG").expect("QUEEN_EMBEDDED_TEST_PG");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));
    connect(&host, port).await
}

/// Empty the timer table before a run.
///
/// Not hygiene theatre: `log_timers_claim_v1` selects by SHARD and has no name filter, so
/// one row left behind by a previous failed run lands in this run's claim batch and every
/// `assert_eq!` on a claim result becomes order-dependent noise. These files own the
/// throwaway Postgres they are pointed at (`QUEEN_EMBEDDED_TEST_PG`), which is the only
/// reason a blanket DELETE is acceptable — and DELETE, not TRUNCATE, because TRUNCATE
/// takes an ACCESS EXCLUSIVE lock, which is the exact class of lock §3.3 turns
/// `vacuum_truncate = off` on to avoid.
pub async fn reset_timers(c: &Client) {
    c.execute("DELETE FROM queen.log_timers", &[])
        .await
        .expect("reset queen.log_timers");
}

// ---------------------------------------------------------------------------- seeding

/// One pending timer, written straight into the table.
///
/// Seeding by INSERT rather than through `log_timers_apply_v1` is deliberate and is what
/// keeps these tests about the FIRE path: §3.3 pins every column, so the seed depends on
/// the schema alone. The tests that are actually about the schedule/cancel contract call
/// `apply` instead.
pub struct Seed<'a> {
    pub tenant: &'a str,
    pub queue: &'a str,
    pub key: &'a str,
    pub partition: &'a str,
    /// Relative to now, in seconds. Negative = a deliver_at in the past, which §4.2
    /// declares legal.
    pub delay_s: f64,
    pub txn: &'a str,
    pub attempts: i32,
}

impl<'a> Seed<'a> {
    pub fn new(tenant: &'a str, queue: &'a str, key: &'a str) -> Self {
        Seed {
            tenant,
            queue,
            key,
            partition: "Default",
            delay_s: 0.0,
            txn: key,
            attempts: 0,
        }
    }
    pub fn partition(mut self, p: &'a str) -> Self {
        self.partition = p;
        self
    }
    pub fn delay_s(mut self, s: f64) -> Self {
        self.delay_s = s;
        self
    }
    pub fn txn(mut self, t: &'a str) -> Self {
        self.txn = t;
        self
    }
    pub fn attempts(mut self, n: i32) -> Self {
        self.attempts = n;
        self
    }
}

pub async fn seed(c: &Client, s: &Seed<'_>) {
    c.execute(
        "INSERT INTO queen.log_timers
             (tenant_id, queue, timer_key, \"partition\", deliver_at, txn, message_id,
              payload, payload_zstd, encrypted, producer_sub, attempts)
         VALUES ($1::text::uuid, $2, $3, $4, now() + make_interval(secs => $5::float8),
                 $6, gen_random_uuid(), convert_to($7, 'UTF8'), false, false, 'seeder', $8::int)",
        &[
            &s.tenant,
            &s.queue,
            &s.key,
            &s.partition,
            &s.delay_s,
            &s.txn,
            &format!("{{\"timer\":\"{}\"}}", s.key),
            &s.attempts,
        ],
    )
    .await
    .expect("seed timer");
}

// -------------------------------------------------------------------- the SP wrappers

/// `log_timers_apply_v1` — schedule / reschedule / cancel. Returns the raw JSONB text so a
/// test can assert on the closed status taxonomy (§4.1) without a serde model that would
/// have to be kept in sync.
pub async fn apply(
    c: &Client,
    ops: &serde_json::Value,
    tenant: &str,
    producer_sub: Option<&str>,
    now_off_s: f64,
) -> Result<serde_json::Value, tokio_postgres::Error> {
    let ops_txt = ops.to_string();
    let row = c
        .query_one(
            "SELECT (queen.log_timers_apply_v1($1::text::jsonb, $2::text::uuid, $3,
                     now() + make_interval(secs => $4::float8)))::text",
            &[&ops_txt, &tenant, &producer_sub, &now_off_s],
        )
        .await?;
    let txt: String = row.get(0);
    Ok(serde_json::from_str(&txt).expect("apply result is JSON"))
}

/// `log_timers_due_v1` — the sweeper's probe. Everything in it is relative to the SERVER
/// clock (§6.2), which is why no test here ever does arithmetic on a timestamp.
pub async fn due(c: &Client, now_off_s: f64, cap: i32) -> serde_json::Value {
    let sql = format!(
        "SELECT (queen.log_timers_due_v1({ALL_SHARDS},
                 now() + make_interval(secs => $1::float8), $2::int))::text"
    );
    let row = c.query_one(&sql, &[&now_off_s, &cap]).await.expect("due");
    let txt: String = row.get(0);
    serde_json::from_str(&txt).expect("due result is JSON")
}

/// `log_timers_claim_v1` — one claim pass, returning the claimed keys IN THE ORDER THE
/// CLAIM RETURNED THEM. That order is the contract of §4.2 ("the order is decided at the
/// fire"), so it must not be sorted here.
pub async fn claim(
    c: &Client,
    now_off_s: f64,
    lease_ms: i32,
    max_rows: i32,
    per_tenant: i32,
) -> Vec<String> {
    let sql = format!(
        "SELECT r_timer_key FROM queen.log_timers_claim_v1({ALL_SHARDS},
             now() + make_interval(secs => $1::float8), $2::int, $3::int, $4::int)"
    );
    c.query(&sql, &[&now_off_s, &lease_ms, &max_rows, &per_tenant])
        .await
        .expect("claim")
        .iter()
        .map(|r| r.get(0))
        .collect()
}

/// One packed segment: exactly the grouping key §6.2 point 8 requires,
/// `(tenant_id, queue, partition)` — never `(queue, partition)`.
pub struct Seg {
    pub tenant: String,
    pub queue: String,
    pub partition: String,
}

impl Seg {
    pub fn new(tenant: &str, queue: &str, partition: &str) -> Self {
        Seg {
            tenant: tenant.into(),
            queue: queue.into(),
            partition: partition.into(),
        }
    }
}

/// One timer inside a packed segment: its key, the 1-based index of its segment, its fixed
/// txn (whose md5 stands in for the frame's 16-byte dedup hash) and the claim token the
/// broker holds for it.
pub struct Framed {
    pub key: String,
    pub seg: i32,
    pub txn: String,
    pub token: String,
}

impl Framed {
    pub fn new(key: &str, seg: i32, txn: &str, token: &str) -> Self {
        Framed {
            key: key.into(),
            seg,
            txn: txn.into(),
            token: token.into(),
        }
    }
}

/// `log_timers_fire_v1` — verify the claim, provision, pre-lock, push, delete, in ONE
/// transaction (§1.3, §6.2).
///
/// The blob is arbitrary bytes on purpose: nothing here ever pops, and the SP stores the
/// blob opaquely (`log_segments.blob` has no reader in SQL). The per-segment hash blob is
/// built in SQL as the concatenation of md5(txn) — 16 bytes each, which is exactly the
/// stride `log_push_one_v1` guards (003_log_push.sql:82-86).
///
/// Returns the Result rather than unwrapping: a poisoned batch failing loudly IS the
/// assertion in §12's poison row.
pub async fn fire(
    c: &Client,
    segs: &[Seg],
    frames: &[Framed],
    now_off_s: f64,
) -> Result<serde_json::Value, tokio_postgres::Error> {
    let seg_tenants: Vec<String> = segs.iter().map(|s| s.tenant.clone()).collect();
    let seg_queues: Vec<String> = segs.iter().map(|s| s.queue.clone()).collect();
    let seg_parts: Vec<String> = segs.iter().map(|s| s.partition.clone()).collect();
    let keys: Vec<String> = frames.iter().map(|f| f.key.clone()).collect();
    let seg_of: Vec<i32> = frames.iter().map(|f| f.seg).collect();
    let txns: Vec<String> = frames.iter().map(|f| f.txn.clone()).collect();
    let tokens: Vec<String> = frames.iter().map(|f| f.token.clone()).collect();

    let row = c
        .query_one(
            "WITH t AS (
                 SELECT k.key, s.seg, x.txn, o.tok, k.ord
                 FROM unnest($4::text[]) WITH ORDINALITY AS k(key, ord)
                 JOIN unnest($5::int[])  WITH ORDINALITY AS s(seg, ord) USING (ord)
                 JOIN unnest($6::text[]) WITH ORDINALITY AS x(txn, ord) USING (ord)
                 JOIN unnest($7::text[]) WITH ORDINALITY AS o(tok, ord) USING (ord)
             ),
             h AS (
                 SELECT seg,
                        string_agg(decode(md5(txn), 'hex'), ''::bytea ORDER BY ord) AS hashes,
                        count(*)::int AS n
                 FROM t GROUP BY seg
             ),
             segs AS (
                 SELECT g.ord AS seg, g.tenant, q.queue, p.part, h.hashes, h.n
                 FROM unnest($1::text[]) WITH ORDINALITY AS g(tenant, ord)
                 JOIN unnest($2::text[]) WITH ORDINALITY AS q(queue, ord) USING (ord)
                 JOIN unnest($3::text[]) WITH ORDINALITY AS p(part, ord) USING (ord)
                 JOIN h ON h.seg = g.ord::int
             )
             SELECT (queen.log_timers_fire_v1(
                 (SELECT array_agg(tenant::uuid ORDER BY seg) FROM segs),
                 (SELECT array_agg(queue        ORDER BY seg) FROM segs),
                 (SELECT array_agg(part         ORDER BY seg) FROM segs),
                 (SELECT array_agg(n            ORDER BY seg) FROM segs),
                 (SELECT array_agg(hashes       ORDER BY seg) FROM segs),
                 (SELECT array_agg(convert_to('queen-test-blob-' || seg, 'UTF8') ORDER BY seg) FROM segs),
                 (SELECT array_agg(key      ORDER BY ord) FROM t),
                 (SELECT array_agg(seg      ORDER BY ord) FROM t),
                 (SELECT array_agg(tok::uuid ORDER BY ord) FROM t),
                 now() + make_interval(secs => $8::float8)))::text",
            &[
                &seg_tenants,
                &seg_queues,
                &seg_parts,
                &keys,
                &seg_of,
                &txns,
                &tokens,
                &now_off_s,
            ],
        )
        .await?;
    let txt: String = row.get(0);
    Ok(serde_json::from_str(&txt).expect("fire result is JSON"))
}

/// `log_timers_fail_v1` — push `claimed_until` out by a backoff and NULL the claim token,
/// so the row is in backoff and in nobody's hands (§4.1, §6.2). `count_attempt = false` is
/// the transient path: infrastructure must never spend the DLQ budget (§4.5).
pub async fn fail(
    c: &Client,
    tenant: &str,
    queue: &str,
    key: &str,
    token: &str,
    backoff_ms: i32,
    error: &str,
    count_attempt: bool,
    max_attempts: i32,
    now_off_s: f64,
) -> Result<serde_json::Value, tokio_postgres::Error> {
    let tenants = vec![tenant.to_string()];
    let queues = vec![queue.to_string()];
    let keys = vec![key.to_string()];
    let tokens = vec![token.to_string()];
    let row = c
        .query_one(
            "SELECT (queen.log_timers_fail_v1($1::text[]::uuid[], $2::text[], $3::text[],
                     $4::text[]::uuid[], $5::int, $6, $7::boolean, $8::int,
                     now() + make_interval(secs => $9::float8)))::text",
            &[
                &tenants,
                &queues,
                &keys,
                &tokens,
                &backoff_ms,
                &error,
                &count_attempt,
                &max_attempts,
                &now_off_s,
            ],
        )
        .await?;
    let txt: String = row.get(0);
    Ok(serde_json::from_str(&txt).expect("fail result is JSON"))
}

/// `log_timers_dlq_v1` — archive an exhausted timer straight into queen.log_dlq, with the
/// partition resolved (or provisioned) UNDER THE TIMER'S TENANT (§6.2, §13.2 point 2).
pub async fn dlq(
    c: &Client,
    tenant: &str,
    queue: &str,
    key: &str,
    payload: &str,
    error: &str,
    min_attempts: i32,
    now_off_s: f64,
) -> Result<serde_json::Value, tokio_postgres::Error> {
    let tenants = vec![tenant.to_string()];
    let queues = vec![queue.to_string()];
    let keys = vec![key.to_string()];
    let payloads = vec![payload.to_string()];
    let errors = vec![error.to_string()];
    let row = c
        .query_one(
            "SELECT (queen.log_timers_dlq_v1($1::text[]::uuid[], $2::text[], $3::text[],
                     $4::text[]::jsonb[], $5::text[], $6::int,
                     now() + make_interval(secs => $7::float8)))::text",
            &[
                &tenants,
                &queues,
                &keys,
                &payloads,
                &errors,
                &min_attempts,
                &now_off_s,
            ],
        )
        .await?;
    let txt: String = row.get(0);
    Ok(serde_json::from_str(&txt).expect("dlq result is JSON"))
}

// ------------------------------------------------------------------- op constructors

/// A schedule op. `_messageId` is the server-injected id (see the CONTRACT block): the
/// client-spellable `messageId` is a RAISE 22023 and is only ever built by the tests that
/// assert that rejection.
pub fn schedule_op(queue: &str, key: &str, partition: &str, delay_ms: i64, txn: &str) -> serde_json::Value {
    serde_json::json!({
        "op": "schedule",
        "queue": queue,
        "timerKey": key,
        "partition": partition,
        "delayMs": delay_ms,
        "txn": txn,
        "_messageId": "0190a0a0-0000-7000-8000-0000000000aa",
        // base64 of {"hello":"timer"} — the payload column is BYTEA, so the JSONB wire
        // carries it encoded.
        "payload": "eyJoZWxsbyI6InRpbWVyIn0=",
        "payloadZstd": false,
        "encrypted": false
    })
}

pub fn cancel_op(queue: &str, key: &str) -> serde_json::Value {
    serde_json::json!({ "op": "cancel", "queue": queue, "timerKey": key })
}

// ------------------------------------------------------------------ table inspection

pub struct TimerRow {
    pub attempts: i32,
    pub claim_token: Option<String>,
    /// NULL when `claimed_until` is NULL — three states, not two.
    pub lease_in_future: Option<bool>,
    pub visible_in_future: bool,
    pub txn: String,
    pub partition: String,
    pub last_error: Option<String>,
}

pub async fn timer_row(c: &Client, tenant: &str, queue: &str, key: &str) -> Option<TimerRow> {
    let rows = c
        .query(
            "SELECT attempts, claim_token::text, (claimed_until > now()), (visible_at > now()),
                    txn, \"partition\", last_error
             FROM queen.log_timers
             WHERE tenant_id = $1::text::uuid AND queue = $2 AND timer_key = $3",
            &[&tenant, &queue, &key],
        )
        .await
        .expect("timer_row");
    rows.first().map(|r| TimerRow {
        attempts: r.get(0),
        claim_token: r.get(1),
        lease_in_future: r.get(2),
        visible_in_future: r.get(3),
        txn: r.get(4),
        partition: r.get(5),
        last_error: r.get(6),
    })
}

/// Every timer of a tenant on a queue, ordered by key — for "nothing of mine vanished".
pub async fn timer_keys(c: &Client, tenant: &str, queue: &str) -> Vec<String> {
    c.query(
        "SELECT timer_key FROM queen.log_timers
         WHERE tenant_id = $1::text::uuid AND queue = $2 ORDER BY timer_key",
        &[&tenant, &queue],
    )
    .await
    .expect("timer_keys")
    .iter()
    .map(|r| r.get(0))
    .collect()
}

pub struct PartitionState {
    pub id: String,
    pub last_offset: i64,
    pub segments: i64,
    pub messages_in_segments: i64,
}

/// What actually landed in the log for `(tenant, queue, partition)`. `None` means the
/// partition was never provisioned, which for a segment that must NOT have fired is the
/// strongest possible assertion.
pub async fn partition_state(
    c: &Client,
    tenant: &str,
    queue: &str,
    partition: &str,
) -> Option<PartitionState> {
    let rows = c
        .query(
            "SELECT p.id::text, p.last_offset,
                    (SELECT count(*) FROM queen.log_segments s WHERE s.partition_id = p.id),
                    -- ::bigint because sum() over bigint returns NUMERIC
                    (SELECT COALESCE(sum(s.end_offset - s.base_offset + 1), 0)::bigint
                       FROM queen.log_segments s WHERE s.partition_id = p.id)
             FROM queen.log_partitions p
             JOIN queen.queues q ON q.id = p.queue_id
             WHERE q.tenant_id = $1::text::uuid AND q.name = $2 AND p.name = $3",
            &[&tenant, &queue, &partition],
        )
        .await
        .expect("partition_state");
    rows.first().map(|r| PartitionState {
        id: r.get(0),
        last_offset: r.get(1),
        segments: r.get(2),
        messages_in_segments: r.get(3),
    })
}

/// Dead letters visible under one tenant's queue, as (consumer_group, offset, txn).
pub async fn dlq_rows(c: &Client, tenant: &str, queue: &str) -> Vec<(String, i64, Option<String>)> {
    c.query(
        "SELECT d.consumer_group, d.\"offset\", d.transaction_id
         FROM queen.log_dlq d
         JOIN queen.log_partitions p ON p.id = d.partition_id
         JOIN queen.queues q ON q.id = p.queue_id
         WHERE q.tenant_id = $1::text::uuid AND q.name = $2
         ORDER BY d.failed_at",
        &[&tenant, &queue],
    )
    .await
    .expect("dlq_rows")
    .iter()
    .map(|r| (r.get(0), r.get(1), r.get(2)))
    .collect()
}

/// Set a queue's dedup window, provisioning the queue under `tenant` if needed — the
/// duplicate verdict only exists when the window is open.
pub async fn ensure_queue(c: &Client, tenant: &str, queue: &str, dedup_window_s: i32) {
    c.execute(
        "INSERT INTO queen.queues (tenant_id, name, namespace, task, dedup_window_seconds)
         VALUES ($1::text::uuid, $2::text, split_part($2::text, '.', 1), '', $3::int)
         ON CONFLICT (tenant_id, name) DO UPDATE SET dedup_window_seconds = EXCLUDED.dedup_window_seconds",
        &[&tenant, &queue, &dedup_window_s],
    )
    .await
    .expect("ensure_queue");
}

/// `log_timers_peek_v1` — one key, with its payload (§6.2). DERIVED signature:
/// `(p_tenant UUID, p_queue TEXT, p_timer_key TEXT) RETURNS JSONB {found, ...}`.
pub async fn peek(c: &Client, tenant: &str, queue: &str, key: &str) -> serde_json::Value {
    let row = c
        .query_one(
            "SELECT (queen.log_timers_peek_v1($1::text::uuid, $2, $3))::text",
            &[&tenant, &queue, &key],
        )
        .await
        .expect("peek");
    let txt: String = row.get(0);
    serde_json::from_str(&txt).expect("peek result is JSON")
}

/// `log_timers_list_v1` — keyset over the PK, QUEUE MANDATORY (§4.1: there is no
/// tenant-wide list, because that would be a scan an end user of the customer could
/// trigger). DERIVED signature:
/// `(p_tenant UUID, p_queue TEXT, p_after TEXT, p_limit INT) RETURNS JSONB {rows: [...]}`.
pub async fn list(c: &Client, tenant: &str, queue: &str, after: Option<&str>, limit: i32) -> serde_json::Value {
    let row = c
        .query_one(
            "SELECT (queen.log_timers_list_v1($1::text::uuid, $2, $3, $4::int))::text",
            &[&tenant, &queue, &after, &limit],
        )
        .await
        .expect("list");
    let txt: String = row.get(0);
    serde_json::from_str(&txt).expect("list result is JSON")
}

/// `kv_apply_v1(p_ops JSONB, p_tenant UUID, p_now TIMESTAMPTZ, p_in_wire BOOLEAN)` — the
/// signature is spelled out in §6.1. `p_in_wire = false` is the standalone surface.
pub async fn kv_apply(
    c: &Client,
    ops: &serde_json::Value,
    tenant: &str,
    in_wire: bool,
) -> Result<serde_json::Value, tokio_postgres::Error> {
    let ops_txt = ops.to_string();
    let row = c
        .query_one(
            "SELECT (queen.kv_apply_v1($1::text::jsonb, $2::text::uuid, now(), $3::boolean))::text",
            &[&ops_txt, &tenant, &in_wire],
        )
        .await?;
    let txt: String = row.get(0);
    Ok(serde_json::from_str(&txt).expect("kv result is JSON"))
}

/// Per-segment verdict of the fire, index-aligned with the segment arrays (§6.4). The
/// closed taxonomy is `fired | stale | duplicate`, which is also what
/// `queen_timers_fired_total{result}` counts (§14.2).
pub fn seg_result(v: &serde_json::Value, i: usize) -> String {
    v.get(i)
        .and_then(|s| s.get("result"))
        .and_then(|r| r.as_str())
        .unwrap_or_else(|| panic!("fire result[{i}] has no result field: {v}"))
        .to_string()
}

/// Per-op verdict of apply, index-aligned with the input ops (§6.4), as (ok, status).
/// The status taxonomy is closed: scheduled | rescheduled | cancelled | absent | too_late
/// (§4.1), because a client that has to tell them apart writes a switch, not a regex.
pub fn op_status(v: &serde_json::Value, i: usize) -> (bool, String) {
    let op = v
        .get(i)
        .unwrap_or_else(|| panic!("apply result has no index {i}: {v}"));
    let ok = op
        .get("ok")
        .and_then(|o| o.as_bool())
        .unwrap_or_else(|| panic!("apply result[{i}] has no ok field: {v}"));
    let status = op
        .get("status")
        .and_then(|s| s.as_str())
        .unwrap_or_else(|| panic!("apply result[{i}] has no status field: {v}"))
        .to_string();
    (ok, status)
}

pub fn sqlstate(e: &tokio_postgres::Error) -> String {
    e.code()
        .map(|c| c.code().to_string())
        .unwrap_or_else(|| "<none>".to_string())
}

/// The claim token the table holds for a row — the fire's only proof of ownership.
pub async fn token_of(c: &Client, tenant: &str, queue: &str, key: &str) -> String {
    timer_row(c, tenant, queue, key)
        .await
        .unwrap_or_else(|| panic!("timer {key} must exist"))
        .claim_token
        .unwrap_or_else(|| panic!("timer {key} must be claimed"))
}

/// Section banner: every one of these files is a single test function (see each file's
/// header for why), so the log line is what names the failing scenario.
pub fn section(name: &str) {
    println!("\n──── {name}");
}
