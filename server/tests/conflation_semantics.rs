//! CONFLATION SEMANTICS — the contract of `PLAN_CONFLATION.md` §1/§3.3/§2.5,
//! the server-side test plan of §7.1, tested against the real broker binary on
//! a real Postgres. Written RED-FIRST: the feature is not implemented, and every
//! case whose name does not say "guard" is expected to FAIL on a behavioral
//! assertion until it is.
//!
//! WHY THIS LEVEL — the HTTP wire, not the SPs, not the embedded facade.
//! Conflation is declared as a QUERY PARAMETER on the pop routes (§3.1), and
//! half of its contract is response-shaped: the `"conflation":true` echo on
//! every conflating pop INCLUDING empty ones (the §8 degrade-loudly contract
//! for old-broker detection hangs off exactly that key), the
//! `"conflationConflict":true` echo (§3.3), the two 400 refusals (§3.3), the
//! `conflated` skipped-count in the ack result (§2.4, pinned by §7.1.2), and
//! the three new `/depth` fields (§2.5/§5.3). None of that is observable from
//! the SQL alone. The embedded facade cannot spell the flag at all today (its
//! params are typed and the field does not exist yet — using it would not
//! compile, which is the wrong kind of red), and calling the new SP signatures
//! directly would die with 42883 instead of failing on behavior. Driving the
//! REAL binary's router lets every case send `conflation=true` TODAY: the
//! current broker deserializes `PopParams` without `deny_unknown_fields`, so
//! the flag is silently ignored, the full backlog is delivered, no new response
//! keys appear, no 400s fire — and the assertions go red for exactly the right
//! reason. Table-level facts (`committed`, `batch_end`, `batch_retry_count`,
//! `attempt_*`, the DLQ row) are asserted straight from Postgres next to the
//! wire traffic, the way `timers_support` does.
//!
//! ============================================================================
//! CONTRACT ASSUMPTIONS — the ONE place to reconcile with the implementation
//! ============================================================================
//! Everything below is the plan's own spelling; where the plan pins a name the
//! section is cited, and nothing else in this file is loose.
//!
//!   * pop wire: `conflation=true|false` query param on all pop routes (§3.1).
//!   * pop response: top-level `"conflation":true` emitted ONLY when the
//!     effective flag is true, on empty responses too (§3.1, §4 degrade-loudly);
//!     `"conflationConflict":true` when the request disagreed with the stored
//!     group policy (§3.3). Flag-off responses stay byte-identical (§3.1).
//!   * refusals (§3.3): `conflation=true` without `consumerGroup`
//!     (`__QUEUE_MODE__`) → 400; `conflation=true` with `autoAck=true` → 400.
//!   * ack result: the per-item object gains `'conflated' = commit_to − previous
//!     cursor − 1` on a clean conflating ack (§2.4 item 2 is the normative
//!     formula, `GREATEST(v_delta - 1, 0)`; §7.1.2's worked example says 98 for
//!     a 100-deep partition, which is an arithmetic slip — it is 99, see
//!     `c01_tail_only_delivery_and_cursor_jump`). The ack REQUEST is unchanged
//!     (§3.1).
//!   * storage: `queen.consumer_groups_metadata.conflation BOOLEAN` (§2.1),
//!     `queen.log_consumers.lease_conflated BOOLEAN` (§2.2). Both are read here
//!     via `to_jsonb(row)` so their ABSENCE today reads as a failed assertion
//!     ("no such key"), never as a 42703 that would masquerade as a fixture bug.
//!   * depth: `GET /api/v1/resources/queues/:q/depth?group=g` gains
//!     `partitionsPending`, `conflation`, `effectivePending` (§2.5, §5.3);
//!     `pending` stays log depth.
//!   * sizing (M5/§3.2): a conflating pop with `batch=B` and no `partitions`
//!     param claims up to B partitions, hard ceiling 64, one tail each. The
//!     sizing cases use ONE message per partition on purpose: the hot-list
//!     budget estimate is allowed to over-count backlogged partitions and claim
//!     narrower (§3.2 note, §10 Q4 keeps that), so only the 1-message shape has
//!     an exact expected width.
//!   * M4 (§2.3 step 2): a group that only ever used the pinned route
//!     `GET /pop/queue/:q/partition/:p` still ends up with a durable
//!     `consumer_groups_metadata` row (partition_name = '') carrying the flag.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`,
//! same convention as `kv_semantics` / `timers_semantics`:
//!
//! ```bash
//! docker run --rm -d --name queen-conflation-pg -e POSTGRES_PASSWORD=postgres -p 5478:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5478 cargo test --test conflation_semantics -- --ignored --nocapture
//! ```
//!
//! ONE test function on purpose, like the sibling suites — here the process
//! global is the spawned broker BINARY (one per run, plus the deliberate
//! restart in the boot-idempotence case). Cases are still reported one by one:
//! each returns `Result<(), String>` and the runner prints a PASS/FAIL line per
//! case before failing, so a red run names every broken rule instead of only
//! the first.

use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::Client;

/// `config::DEFAULT_TENANT` — tenancy is off on the spawned broker, so every
/// row this suite inspects lives under the default tenant.
const TENANT: &str = "00000000-0000-0000-0000-000000000001";

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
    format!("{prefix}-{nanos}")
}

// ======================================================================
// throwaway-PG plumbing (QUEEN_EMBEDDED_TEST_PG convention of the siblings)
// ======================================================================

fn pg_target() -> (String, u16) {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port) for the conflation tests");
    target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432))
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

// ======================================================================
// the broker under test: the REAL binary, spawned against the throwaway PG
// ======================================================================

struct BrokerProc {
    child: tokio::process::Child,
    http: Http,
    pg_host: String,
    pg_port: u16,
    /// Kept for post-mortem: the path is printed by every boot failure.
    #[allow(dead_code)]
    log_path: std::path::PathBuf,
}

fn free_port() -> u16 {
    // Bind :0, read the assigned port, drop the listener. The tiny race window
    // before the broker re-binds is acceptable for a local test run.
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    l.local_addr().expect("local_addr").port()
}

impl BrokerProc {
    async fn spawn(pg_host: &str, pg_port: u16) -> Result<BrokerProc, String> {
        let port = free_port();
        let log_path = std::env::temp_dir().join(format!(
            "queen-conflation-broker-{}-{}.log",
            std::process::id(),
            port
        ));
        let log = std::fs::File::create(&log_path)
            .map_err(|e| format!("create broker log {}: {e}", log_path.display()))?;
        let log2 = log.try_clone().map_err(|e| format!("clone log: {e}"))?;
        let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_queen"))
            .env("PG_HOST", pg_host)
            .env("PG_PORT", pg_port.to_string())
            .env("PG_USER", "postgres")
            .env("PG_PASSWORD", "postgres")
            .env("PG_DATABASE", "postgres")
            .env("PORT", port.to_string())
            .env("DB_POOL_SIZE", "16")
            .env("LOG_LEVEL", "warn")
            .stdout(std::process::Stdio::from(log))
            .stderr(std::process::Stdio::from(log2))
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("spawn queen binary: {e}"))?;
        let http = Http {
            addr: format!("127.0.0.1:{port}"),
        };
        wait_health(&http, 40, &log_path).await?;
        Ok(BrokerProc {
            child,
            http,
            pg_host: pg_host.to_string(),
            pg_port,
            log_path,
        })
    }

    /// Kill and re-spawn: the second boot re-applies the include_str!-embedded
    /// schema on a POPULATED database — the §7.1.11 leg. A fresh port avoids
    /// any EADDRINUSE flake from the old listener.
    async fn restart(&mut self) -> Result<(), String> {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
        let next = BrokerProc::spawn(&self.pg_host.clone(), self.pg_port).await?;
        let old = std::mem::replace(self, next);
        drop(old.child); // already dead
        Ok(())
    }
}

async fn wait_health(http: &Http, secs: u64, log_path: &std::path::Path) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut last = String::from("<no response yet>");
    while Instant::now() < deadline {
        match http.req("GET", "/health", None).await {
            Ok((200, _)) => return Ok(()),
            Ok((code, body)) => last = format!("{code}: {body}"),
            Err(e) => last = e,
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!(
        "broker did not become healthy within {secs}s (last: {last}); log: {}",
        log_path.display()
    ))
}

// ======================================================================
// minimal HTTP/1.1 client — tokio TcpStream, Connection: close, no new deps
// ======================================================================

#[derive(Clone)]
struct Http {
    addr: String,
}

impl Http {
    async fn req(&self, method: &str, path_query: &str, body: Option<&Value>) -> Result<(u16, Value), String> {
        let fut = self.req_inner(method, path_query, body);
        tokio::time::timeout(Duration::from_secs(30), fut)
            .await
            .map_err(|_| format!("HTTP timeout: {method} {path_query}"))?
    }

    async fn req_inner(
        &self,
        method: &str,
        path_query: &str,
        body: Option<&Value>,
    ) -> Result<(u16, Value), String> {
        let mut s = tokio::net::TcpStream::connect(&self.addr)
            .await
            .map_err(|e| format!("connect {}: {e}", self.addr))?;
        let payload = body.map(|b| b.to_string()).unwrap_or_default();
        let mut req = format!(
            "{method} {path_query} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
            self.addr
        );
        if body.is_some() {
            req.push_str("Content-Type: application/json\r\n");
            req.push_str(&format!("Content-Length: {}\r\n", payload.len()));
        }
        req.push_str("\r\n");
        s.write_all(req.as_bytes()).await.map_err(|e| format!("write: {e}"))?;
        if body.is_some() {
            s.write_all(payload.as_bytes()).await.map_err(|e| format!("write body: {e}"))?;
        }
        let mut raw = Vec::with_capacity(16 * 1024);
        s.read_to_end(&mut raw).await.map_err(|e| format!("read: {e}"))?;
        parse_http(&raw)
    }
}

/// Status + JSON body out of a raw HTTP/1.1 response (Connection: close, so the
/// body ends at EOF). Handles chunked transfer-encoding; an empty body (204)
/// parses as Null.
fn parse_http(raw: &[u8]) -> Result<(u16, Value), String> {
    let sep = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| format!("no header/body separator in: {:.200}", String::from_utf8_lossy(raw)))?;
    let head = String::from_utf8_lossy(&raw[..sep]).to_string();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| format!("bad status line: {status_line}"))?;
    let chunked = lines
        .filter_map(|l| l.split_once(':'))
        .any(|(k, v)| k.trim().eq_ignore_ascii_case("transfer-encoding")
            && v.to_ascii_lowercase().contains("chunked"));
    let mut body = raw[sep + 4..].to_vec();
    if chunked {
        body = dechunk(&body)?;
    }
    if body.is_empty() {
        return Ok((code, Value::Null));
    }
    let v: Value = serde_json::from_slice(&body)
        .map_err(|e| format!("non-JSON body ({e}): {:.400}", String::from_utf8_lossy(&body)))?;
    Ok((code, v))
}

fn dechunk(b: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0usize;
    loop {
        let nl = b[i..]
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or("chunked: missing size CRLF")?;
        let size_hex = std::str::from_utf8(&b[i..i + nl]).map_err(|_| "chunked: bad size")?;
        let size = usize::from_str_radix(size_hex.trim().split(';').next().unwrap_or("0"), 16)
            .map_err(|_| format!("chunked: bad size {size_hex:?}"))?;
        i += nl + 2;
        if size == 0 {
            return Ok(out);
        }
        if i + size > b.len() {
            return Err("chunked: truncated body".into());
        }
        out.extend_from_slice(&b[i..i + size]);
        i += size + 2; // skip the chunk's trailing CRLF
    }
}

// ======================================================================
// broker API helpers
// ======================================================================

async fn configure(h: &Http, queue: &str, options: Value) -> Result<(), String> {
    let (code, v) = h
        .req("POST", "/api/v1/configure", Some(&json!({"queue": queue, "options": options})))
        .await?;
    if !(200..300).contains(&code) {
        return Err(format!("configure {queue} -> {code}: {v}"));
    }
    Ok(())
}

fn push_item(queue: &str, partition: &str, payload: Value) -> Value {
    json!({"queue": queue, "partition": partition, "payload": payload})
}

async fn push(h: &Http, items: Vec<Value>) -> Result<Vec<Value>, String> {
    let (code, v) = h.req("POST", "/api/v1/push", Some(&json!({"items": items}))).await?;
    if !(200..300).contains(&code) {
        return Err(format!("push -> {code}: {v}"));
    }
    let arr = v.as_array().cloned().ok_or_else(|| format!("push result not an array: {v}"))?;
    for r in &arr {
        let st = r.get("status").and_then(|s| s.as_str()).unwrap_or("");
        if st != "queued" && st != "duplicate" {
            return Err(format!("push item not accepted: {r}"));
        }
    }
    Ok(arr)
}

/// n messages {"n": start..start+n} into one partition, one POST (one segment).
async fn push_n(h: &Http, queue: &str, partition: &str, start: i64, n: i64) -> Result<(), String> {
    let items: Vec<Value> = (start..start + n)
        .map(|i| push_item(queue, partition, json!({"n": i})))
        .collect();
    push(h, items).await.map(|_| ())
}

async fn pop_queue(h: &Http, queue: &str, qs: &str) -> Result<(u16, Value), String> {
    h.req("GET", &format!("/api/v1/pop/queue/{queue}?{qs}"), None).await
}

async fn pop_part(h: &Http, queue: &str, part: &str, qs: &str) -> Result<(u16, Value), String> {
    h.req("GET", &format!("/api/v1/pop/queue/{queue}/partition/{part}?{qs}"), None)
        .await
}

/// Pop that must be served (the refusal cases assert codes themselves). The
/// broker returns 200 with a body when it delivered and a BODILESS 204 when it
/// did not (`data.rs`: `if count == 0 { NO_CONTENT }` — the 204 strips the
/// rendered body on the wire), so an empty pop parses as Null and `msgs()`
/// reads as zero messages.
async fn pop_ok(h: &Http, queue: &str, qs: &str) -> Result<Value, String> {
    let (code, v) = pop_queue(h, queue, qs).await?;
    if code != 200 && code != 204 {
        return Err(format!("pop {queue}?{qs} -> {code}: {v}"));
    }
    Ok(v)
}

fn msgs(v: &Value) -> Vec<Value> {
    v.get("messages").and_then(|m| m.as_array()).cloned().unwrap_or_default()
}

fn msg_n(m: &Value) -> i64 {
    m.get("data").and_then(|d| d.get("n")).and_then(|n| n.as_i64()).unwrap_or(-999)
}

fn s(m: &Value, key: &str) -> String {
    m.get(key).and_then(|x| x.as_str()).unwrap_or("").to_string()
}

async fn ack(
    h: &Http,
    txn: &str,
    pid: &str,
    status: &str,
    group: &str,
    lease: &str,
) -> Result<Value, String> {
    let (code, v) = h
        .req(
            "POST",
            "/api/v1/ack",
            Some(&json!({
                "transactionId": txn, "partitionId": pid, "status": status,
                "consumerGroup": group, "leaseId": lease,
                "error": if status == "completed" { Value::Null } else { json!("test failure") },
            })),
        )
        .await?;
    if code != 200 {
        return Err(format!("ack {txn} -> {code}: {v}"));
    }
    v.as_array()
        .and_then(|a| a.first().cloned())
        .ok_or_else(|| format!("ack result not a 1-element array: {v}"))
}

async fn ack_batch_completed(h: &Http, group: &str, items: &[(String, String, String)]) -> Result<Vec<Value>, String> {
    let acks: Vec<Value> = items
        .iter()
        .map(|(txn, pid, lease)| {
            json!({"transactionId": txn, "partitionId": pid, "status": "completed", "leaseId": lease})
        })
        .collect();
    let (code, v) = h
        .req("POST", "/api/v1/ack/batch", Some(&json!({"acknowledgments": acks, "consumerGroup": group})))
        .await?;
    if code != 200 {
        return Err(format!("ack/batch -> {code}: {v}"));
    }
    v.as_array().cloned().ok_or_else(|| format!("ack/batch result not an array: {v}"))
}

// ======================================================================
// SQL inspection (same DB the broker uses; the tests own the throwaway PG)
// ======================================================================

async fn partition_id(c: &Client, queue: &str, part: &str) -> Result<String, String> {
    let rows = c
        .query(
            "SELECT p.id::text FROM queen.log_partitions p
             JOIN queen.queues q ON q.id = p.queue_id
             WHERE q.tenant_id = $1::text::uuid AND q.name = $2 AND p.name = $3",
            &[&TENANT, &queue, &part],
        )
        .await
        .map_err(|e| format!("partition_id: {e}"))?;
    rows.first()
        .map(|r| r.get::<_, String>(0))
        .ok_or_else(|| format!("partition {queue}/{part} does not exist"))
}

async fn last_offset(c: &Client, queue: &str, part: &str) -> Result<i64, String> {
    let rows = c
        .query(
            "SELECT p.last_offset FROM queen.log_partitions p
             JOIN queen.queues q ON q.id = p.queue_id
             WHERE q.tenant_id = $1::text::uuid AND q.name = $2 AND p.name = $3",
            &[&TENANT, &queue, &part],
        )
        .await
        .map_err(|e| format!("last_offset: {e}"))?;
    rows.first()
        .map(|r| r.get::<_, i64>(0))
        .ok_or_else(|| format!("partition {queue}/{part} does not exist"))
}

/// The whole `queen.log_consumers` row as JSON. `to_jsonb`, not a column list,
/// so the two columns the plan ADDS (`lease_conflated` here, `conflation` on
/// the metadata row) read as MISSING KEYS while unimplemented — an assertion
/// failure with a name, never a 42703 masquerading as a broken fixture.
async fn consumer(c: &Client, queue: &str, part: &str, group: &str) -> Result<Option<Value>, String> {
    let rows = c
        .query(
            "SELECT to_jsonb(cr)::text FROM queen.log_consumers cr
             JOIN queen.log_partitions p ON p.id = cr.partition_id
             JOIN queen.queues q ON q.id = p.queue_id
             WHERE q.tenant_id = $1::text::uuid AND q.name = $2 AND p.name = $3
               AND cr.consumer_group = $4",
            &[&TENANT, &queue, &part, &group],
        )
        .await
        .map_err(|e| format!("consumer row: {e}"))?;
    Ok(rows
        .first()
        .map(|r| serde_json::from_str(&r.get::<_, String>(0)).unwrap_or(Value::Null)))
}

async fn consumer_must(c: &Client, queue: &str, part: &str, group: &str) -> Result<Value, String> {
    consumer(c, queue, part, group)
        .await?
        .ok_or_else(|| format!("no log_consumers row for {queue}/{part} group {group}"))
}

fn ci64(row: &Value, key: &str) -> i64 {
    row.get(key).and_then(|v| v.as_i64()).unwrap_or(i64::MIN)
}

fn cnull(row: &Value, key: &str) -> bool {
    matches!(row.get(key), Some(Value::Null) | None)
}

/// The durable group declaration (queue-scoped row, partition_name = '').
async fn cgm_row(c: &Client, queue: &str, group: &str) -> Result<Option<Value>, String> {
    let rows = c
        .query(
            "SELECT to_jsonb(m)::text FROM queen.consumer_groups_metadata m
             JOIN queen.queues q ON q.id = m.queue_id
             WHERE q.tenant_id = $1::text::uuid AND q.name = $2
               AND m.consumer_group = $3 AND m.partition_name = ''",
            &[&TENANT, &queue, &group],
        )
        .await
        .map_err(|e| format!("cgm row: {e}"))?;
    Ok(rows
        .first()
        .map(|r| serde_json::from_str(&r.get::<_, String>(0)).unwrap_or(Value::Null)))
}

async fn dlq_rows(c: &Client, queue: &str) -> Result<Vec<(i64, i32, Option<String>)>, String> {
    c.query(
        "SELECT d.\"offset\", d.retry_count, d.error
         FROM queen.log_dlq d
         JOIN queen.log_partitions p ON p.id = d.partition_id
         JOIN queen.queues q ON q.id = p.queue_id
         WHERE q.tenant_id = $1::text::uuid AND q.name = $2
         ORDER BY d.failed_at",
        &[&TENANT, &queue],
    )
    .await
    .map_err(|e| format!("dlq rows: {e}"))?
    .iter()
    .map(|r| Ok((r.get(0), r.get(1), r.get(2))))
    .collect()
}

async fn column_exists(c: &Client, table: &str, col: &str) -> Result<bool, String> {
    let row = c
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'queen' AND table_name = $1 AND column_name = $2)",
            &[&table, &col],
        )
        .await
        .map_err(|e| format!("column_exists: {e}"))?;
    Ok(row.get(0))
}

/// Simulate retention having removed every segment of a partition (the tests
/// own the throwaway PG; retention itself is not the subject of §7.1.7, the
/// zero-taken cursor seal is).
async fn wipe_segments(c: &Client, pid: &str) -> Result<(), String> {
    c.execute("DELETE FROM queen.log_segments WHERE partition_id = $1::text::uuid", &[&pid])
        .await
        .map_err(|e| format!("wipe segments: {e}"))?;
    c.execute("DELETE FROM queen.log_txns WHERE partition_id = $1::text::uuid", &[&pid])
        .await
        .map_err(|e| format!("wipe txns: {e}"))?;
    Ok(())
}

/// Partitions of (queue, group) still pending: last_offset beyond the group's
/// effective cursor — the §1.3 standing pending predicate, computed from the
/// tables so it needs no SP contract.
async fn pending_partitions(c: &Client, queue: &str, group: &str) -> Result<i64, String> {
    let row = c
        .query_one(
            "SELECT count(*) FROM queen.log_partitions p
             JOIN queen.queues q ON q.id = p.queue_id
             LEFT JOIN queen.log_consumers cr
               ON cr.partition_id = p.id AND cr.consumer_group = $3
             WHERE q.tenant_id = $1::text::uuid AND q.name = $2
               AND p.last_offset > GREATEST(COALESCE(cr.committed, -1), p.log_start - 1)",
            &[&TENANT, &queue, &group],
        )
        .await
        .map_err(|e| format!("pending_partitions: {e}"))?;
    Ok(row.get(0))
}

// ======================================================================
// cases
// ======================================================================

// §7.1.1 + §7.1.2 — tail-only delivery, then the cursor jump on ack. Backlog of
// 100 in one partition: a conflating pop delivers EXACTLY the newest visible
// message, leases (committed, tail], and the clean ack advances committed to
// the tail while reporting the 99 skipped positions as `conflated = 99` — the
// author's formula, commit_to − previous cursor − 1 = 99 − (−1) − 1 = 99, with
// the delivered message itself the one retired position that is not "skipped".
//
// CORRECTED 2026-08-21 (arithmetic slip in the original red draft, and in the
// worked example at PLAN_CONFLATION §7.1.2 it was copied from): both said 98,
// which evaluates 99 − (−1) − 1 as if the double negative were a subtraction.
// 100 positions are retired and 1 is delivered, so 99 are skipped — as this
// case's own prose already said. The formula is normative at §2.4 item 2
// (`GREATEST(v_delta - 1, 0)`) and `c04_mode_composition` below pins it
// independently on a different backlog (33 retired, 32 skipped, conflated = 32);
// no implementation can satisfy both 98 here and 32 there. Nothing else in this
// case changed — the assertion still pins an exact value, not a range.
async fn c01_tail_only_delivery_and_cursor_jump(h: &Http, c: &Client) -> Case {
    let q = unique("cfl-tail");
    let g = "workers";
    configure(h, &q, json!({})).await?;
    push_n(h, &q, "Default", 0, 100).await?;

    let v = pop_ok(h, &q, &format!("consumerGroup={g}&subscriptionMode=all&conflation=true&batch=200")).await?;
    let m = msgs(&v);
    chk!(
        m.len() == 1,
        "a conflating pop of a 100-deep partition must deliver EXACTLY ONE message (the \
         newest visible, §1.2); got {} — the broker is ignoring the conflation flag",
        m.len()
    );
    chk!(msg_n(&m[0]) == 99, "the one delivered message must be the TAIL (n=99), got n={}", msg_n(&m[0]));
    chk!(
        v.get("conflation") == Some(&Value::Bool(true)),
        "a conflating pop response must echo \"conflation\":true (§3.1): {v}"
    );

    let row = consumer_must(c, &q, "Default", g).await?;
    chk!(ci64(&row, "committed") == -1, "pop must not move committed (it is the ack that jumps): {row}");
    chk!(
        ci64(&row, "batch_end") == 99,
        "the lease must span (committed, tail]: batch_end must be 99 (= commit_to, M1): {row}"
    );
    chk!(
        row.get("lease_conflated") == Some(&Value::Bool(true)),
        "log_consumers.lease_conflated must mark the conflating lease (§2.2): {row}"
    );

    let (txn, pid, lease) = (s(&m[0], "transactionId"), s(&m[0], "partitionId"), s(&m[0], "leaseId"));
    let a = ack(h, &txn, &pid, "completed", g, &lease).await?;
    chk!(a.get("success") == Some(&Value::Bool(true)), "clean ack must succeed: {a}");
    chk!(
        a.get("conflated").and_then(|x| x.as_i64()) == Some(99),
        "the ack result must report conflated = 99 (commit_to − previous cursor − 1 \
         = 99 − (−1) − 1, i.e. the 100 retired positions minus the 1 delivered; \
         §2.4 item 2): {a}"
    );

    let row = consumer_must(c, &q, "Default", g).await?;
    chk!(ci64(&row, "committed") == 99, "ack must jump committed to the tail (99): {row}");
    chk!(cnull(&row, "batch_end"), "the lease must be closed (batch_end NULL): {row}");
    chk!(
        row.get("lease_conflated") == Some(&Value::Bool(false)),
        "a released lease must not leave a stale conflated marker (§2.4): {row}"
    );
    chk!(
        ci64(&row, "total_consumed") == 100,
        "total_consumed keeps counting LOG POSITIONS RETIRED (100), not handler \
         invocations (§2.4, Q6): {row}"
    );
    Ok(())
}

// §1.3 — THE GUARANTEE, the test the whole feature exists to pass (§7.3 E2E-1's
// server-side core). Pop the tail T, push T+1..T+k while the lease is open, ack:
// committed must land on T — the offset OBSERVED at pop time — never on T+k;
// the partition must still read as pending; and the next pop must deliver the
// newest of the new span.
async fn c02_the_guarantee(h: &Http, c: &Client) -> Case {
    let q = unique("cfl-guarantee");
    let g = "workers";
    configure(h, &q, json!({})).await?;
    push_n(h, &q, "Default", 0, 5).await?; // offsets 0..4, tail T = 4

    let v = pop_ok(h, &q, &format!("consumerGroup={g}&subscriptionMode=all&conflation=true")).await?;
    let m = msgs(&v);
    chk!(m.len() == 1, "conflating pop must deliver exactly one message, got {}", m.len());
    chk!(msg_n(&m[0]) == 4, "must deliver the tail T (n=4), got n={}", msg_n(&m[0]));

    // The supersession: T+1..T+3 commit while the lease is open.
    push_n(h, &q, "Default", 5, 3).await?; // offsets 5..7

    let (txn, pid, lease) = (s(&m[0], "transactionId"), s(&m[0], "partitionId"), s(&m[0], "leaseId"));
    let a = ack(h, &txn, &pid, "completed", g, &lease).await?;
    chk!(a.get("success") == Some(&Value::Bool(true)), "ack must succeed: {a}");

    let row = consumer_must(c, &q, "Default", g).await?;
    chk!(
        ci64(&row, "committed") == 4,
        "THE GUARANTEE (§1.3): committed must be T=4 — the broker must NEVER commit past \
         an offset that was not observed at pop time; got {row}"
    );
    let lo = last_offset(c, &q, "Default").await?;
    chk!(lo == 7, "allocator sanity: last_offset must be 7, got {lo}");
    chk!(
        pending_partitions(c, &q, g).await? == 1,
        "after the ack the partition must STILL read as pending (last_offset 7 > committed 4)"
    );

    let v = pop_ok(h, &q, &format!("consumerGroup={g}&conflation=true")).await?;
    let m = msgs(&v);
    chk!(m.len() == 1, "the follow-up pop must deliver exactly one message, got {}", m.len());
    chk!(
        msg_n(&m[0]) == 7,
        "the follow-up pop must deliver the newest of the NEW span (n=7), got n={}",
        msg_n(&m[0])
    );
    let a = ack(h, &s(&m[0], "transactionId"), &s(&m[0], "partitionId"), "completed", g, &s(&m[0], "leaseId")).await?;
    chk!(a.get("success") == Some(&Value::Bool(true)), "second ack must succeed: {a}");
    let row = consumer_must(c, &q, "Default", g).await?;
    chk!(ci64(&row, "committed") == 7, "second ack lands on the new tail: {row}");
    Ok(())
}

// §7.1.3 + §7.1.4 — nack keeps the cursor; supersession does not reset the retry
// budget (the M2 pin) nor the attempt telemetry (the §1.4 fix: attempt_offset is
// the EPISODE ANCHOR committed+1, invariant across supersession); and the poison
// partition still DLQs on schedule even with a hot producer.
async fn c03_retry_supersession_budget_dlq(h: &Http, c: &Client) -> Case {
    let q = unique("cfl-retry");
    let g = "workers";
    configure(h, &q, json!({"retryLimit": 2, "deadLetterQueue": true})).await?;
    push_n(h, &q, "Default", 0, 3).await?; // offsets 0..2

    // Attempt 1: deliver the tail, fail it.
    let v = pop_ok(h, &q, &format!("consumerGroup={g}&subscriptionMode=all&conflation=true")).await?;
    let m = msgs(&v);
    chk!(m.len() == 1, "conflating pop must deliver one message, got {}", m.len());
    chk!(msg_n(&m[0]) == 2, "must deliver the tail (n=2), got n={}", msg_n(&m[0]));
    let a = ack(h, &s(&m[0], "transactionId"), &s(&m[0], "partitionId"), "failed", g, &s(&m[0], "leaseId")).await?;
    chk!(a.get("success") == Some(&Value::Bool(true)), "failed-ack must be accepted: {a}");
    chk!(a.get("dlq") != Some(&Value::Bool(true)), "first failure must NOT dead-letter: {a}");
    let row = consumer_must(c, &q, "Default", g).await?;
    chk!(ci64(&row, "committed") == -1, "a nack keeps the cursor (§7.1.3): {row}");
    chk!(ci64(&row, "batch_retry_count") == 1, "one explicit failed = one charge: {row}");

    // Supersession: a hot producer pushes a newer message before the retry.
    push_n(h, &q, "Default", 3, 1).await?; // offset 3
    let v = pop_ok(h, &q, &format!("consumerGroup={g}&conflation=true")).await?;
    let m = msgs(&v);
    chk!(m.len() == 1, "redelivery pop must deliver one message, got {}", m.len());
    chk!(
        msg_n(&m[0]) == 3,
        "redelivery must deliver the NEWEST tail (n=3) — the delivered offset changed \
         (supersession), got n={}",
        msg_n(&m[0])
    );
    let row = consumer_must(c, &q, "Default", g).await?;
    chk!(
        ci64(&row, "attempt_offset") == 0,
        "attempt_offset must be the EPISODE ANCHOR committed+1 = 0 (§1.4), invariant \
         across supersession: {row}"
    );
    chk!(
        ci64(&row, "attempt_count") == 2,
        "attempt_count must increment MONOTONICALLY across supersession (this is what \
         §1.4 fixes and what would silently regress): {row}"
    );
    let a = ack(h, &s(&m[0], "transactionId"), &s(&m[0], "partitionId"), "failed", g, &s(&m[0], "leaseId")).await?;
    chk!(a.get("success") == Some(&Value::Bool(true)), "second failed-ack accepted: {a}");
    let row = consumer_must(c, &q, "Default", g).await?;
    chk!(
        ci64(&row, "batch_retry_count") == 2,
        "the retry budget must carry ACROSS supersession (M2): {row}"
    );

    // Attempt 3 on yet another tail: budget (retryLimit=2) is exhausted → DLQ,
    // even though the producer kept pushing the whole time.
    push_n(h, &q, "Default", 4, 1).await?; // offset 4
    let v = pop_ok(h, &q, &format!("consumerGroup={g}&conflation=true")).await?;
    let m = msgs(&v);
    chk!(m.len() == 1, "third delivery must be one message, got {}", m.len());
    chk!(msg_n(&m[0]) == 4, "third delivery is the newest tail (n=4), got n={}", msg_n(&m[0]));
    let a = ack(h, &s(&m[0], "transactionId"), &s(&m[0], "partitionId"), "failed", g, &s(&m[0], "leaseId")).await?;
    chk!(
        a.get("dlq") == Some(&Value::Bool(true)),
        "the third failed ack must exhaust the budget and dead-letter (retryLimit=2): {a}"
    );
    let dl = dlq_rows(c, &q).await?;
    chk!(dl.len() == 1, "exactly one DLQ row, got {}: {dl:?}", dl.len());
    chk!(
        dl[0].0 == 4,
        "the DLQ'd message is the tail at the LAST attempt (offset 4, §1.4 'what goes to \
         the DLQ'): {dl:?}"
    );
    chk!(dl[0].1 == 2, "the DLQ row's retry_count is the exhausted budget (2): {dl:?}");
    let row = consumer_must(c, &q, "Default", g).await?;
    chk!(
        ci64(&row, "committed") == 4,
        "filing the poison jumps the cursor to it — the episode's superseded messages \
         are retired WITH the poison (§1.4): {row}"
    );
    chk!(ci64(&row, "batch_retry_count") == 0, "DLQ filing resets the budget: {row}");

    // The group resumes on the next tail.
    push_n(h, &q, "Default", 5, 1).await?;
    let v = pop_ok(h, &q, &format!("consumerGroup={g}&conflation=true")).await?;
    let m = msgs(&v);
    chk!(m.len() == 1 && msg_n(&m[0]) == 5, "after the DLQ the group resumes on the next tail: {v}");
    let a = ack(h, &s(&m[0], "transactionId"), &s(&m[0], "partitionId"), "completed", g, &s(&m[0], "leaseId")).await?;
    chk!(a.get("success") == Some(&Value::Bool(true)), "resume ack: {a}");
    Ok(())
}

// §1.5 / §7.1.9 — composition with subscription mode. mode=new + conflation
// skips history via the SEED (0 delivered, then one tail from live pushes);
// mode=all + conflation conflates the ENTIRE retained history to one message.
async fn c04_mode_composition(h: &Http, c: &Client) -> Case {
    let q = unique("cfl-modes");
    configure(h, &q, json!({})).await?;
    push_n(h, &q, "Default", 0, 30).await?; // history 0..29

    // mode=new + conflation: the seed skips the backlog.
    let v = pop_ok(h, &q, "consumerGroup=live&subscriptionMode=new&conflation=true").await?;
    chk!(
        msgs(&v).is_empty(),
        "mode=new + conflation delivers NOTHING from history (the seed skips it, §1.5): {v}"
    );
    push_n(h, &q, "Default", 30, 3).await?; // 30..32
    let v = pop_ok(h, &q, "consumerGroup=live&conflation=true").await?;
    let m = msgs(&v);
    chk!(
        m.len() == 1,
        "mode=new + conflation must deliver exactly the newest of the live span, got {} \
         messages",
        m.len()
    );
    chk!(msg_n(&m[0]) == 32, "the live tail is n=32, got n={}", msg_n(&m[0]));
    let a = ack(h, &s(&m[0], "transactionId"), &s(&m[0], "partitionId"), "completed", "live", &s(&m[0], "leaseId")).await?;
    chk!(a.get("success") == Some(&Value::Bool(true)), "live ack: {a}");

    // mode=all + conflation: 33 retained messages conflate to ONE.
    let v = pop_ok(h, &q, "consumerGroup=rebuild&subscriptionMode=all&conflation=true&batch=200").await?;
    let m = msgs(&v);
    chk!(
        m.len() == 1,
        "mode=all + conflation conflates the ENTIRE retained history (33 msgs) to one \
         message per partition (§1.5), got {}",
        m.len()
    );
    chk!(msg_n(&m[0]) == 32, "and it is the newest (n=32), got n={}", msg_n(&m[0]));
    let a = ack(h, &s(&m[0], "transactionId"), &s(&m[0], "partitionId"), "completed", "rebuild", &s(&m[0], "leaseId")).await?;
    chk!(
        a.get("conflated").and_then(|x| x.as_i64()) == Some(32),
        "the all-mode ack retires 33 positions, 32 of them skipped: conflated must be 32: {a}"
    );
    let row = consumer_must(c, &q, "Default", "rebuild").await?;
    chk!(ci64(&row, "committed") == 32, "rebuild cursor lands on the tail: {row}");
    Ok(())
}

// §3.3 — the two refused combinations, 400 with a named reason. Both are
// consumer bugs whose silent form is unfixable in production, so this is the
// one place conflation REJECTS rather than warns.
async fn c05_refusals(h: &Http) -> Case {
    let q = unique("cfl-refuse");
    configure(h, &q, json!({})).await?;
    push_n(h, &q, "Default", 0, 1).await?;

    // Control first: the legal spelling must NOT be refused.
    let (code, v) = pop_queue(h, &q, "consumerGroup=g&subscriptionMode=all&conflation=true").await?;
    chk!(code == 200, "control: a conflating pop WITH a group must be served, got {code}: {v}");

    // (a) conflation without consumerGroup = queue mode: no group identity to
    // hang a policy on (§3.3).
    let (code, v) = pop_queue(h, &q, "conflation=true").await?;
    chk!(
        code == 400,
        "conflation=true WITHOUT consumerGroup (__QUEUE_MODE__) must be refused with 400 \
         (§3.3); got {code}: {v}"
    );

    // (b) same rule on the pinned-partition route.
    let (code, v) = pop_part(h, &q, "Default", "conflation=true").await?;
    chk!(
        code == 400,
        "group-less conflation on the pinned route must also be 400 (§3.3); got {code}: {v}"
    );

    // (c) conflation + autoAck: auto-ack commits at delivery with no lease, so a
    // crashed handler would lose the tail — the §1.3 guarantee silently becomes
    // at-most-once (§3.3, Q2).
    let (code, v) = pop_queue(h, &q, "consumerGroup=g&autoAck=true&conflation=true").await?;
    chk!(
        code == 400,
        "conflation=true together with autoAck=true must be refused with 400 (§3.3); \
         got {code}: {v}"
    );
    Ok(())
}

// §3.3 — declaration conflict: the STORED value wins for every consumer of the
// group, and the disagreement is loud — the response carries
// "conflationConflict":true while the effective policy stays the stored one.
async fn c06_conflict_stored_wins(h: &Http, c: &Client) -> Case {
    let q = unique("cfl-conflict");
    let g = "workers";
    configure(h, &q, json!({})).await?;

    // First consumer registers the group WITH conflation on an empty queue (the
    // registering pop persists the requested value, §3.3).
    let v = pop_ok(h, &q, &format!("consumerGroup={g}&subscriptionMode=all&conflation=true")).await?;
    chk!(msgs(&v).is_empty(), "registration pop on an empty queue delivers nothing: {v}");
    chk!(
        cgm_row(c, &q, g).await?.is_some(),
        "the registering pop must have written the durable group row"
    );

    push_n(h, &q, "Default", 0, 5).await?;

    // Second consumer of the SAME group disagrees: conflation=false.
    let v = pop_ok(h, &q, &format!("consumerGroup={g}&conflation=false")).await?;
    chk!(
        v.get("conflationConflict") == Some(&Value::Bool(true)),
        "a request disagreeing with the stored policy must be echoed loudly: \
         \"conflationConflict\":true (§3.3): {v}"
    );
    chk!(
        v.get("conflation") == Some(&Value::Bool(true)),
        "the response echoes the EFFECTIVE (stored) policy, which is true: {v}"
    );
    let m = msgs(&v);
    chk!(
        m.len() == 1,
        "group-setting-wins: the disagreeing consumer still gets CONFLATED delivery \
         (1 message, not the 5-deep backlog); got {}",
        m.len()
    );
    chk!(msg_n(&m[0]) == 4, "and it is the tail (n=4): got n={}", msg_n(&m[0]));

    let row = cgm_row(c, &q, g).await?.ok_or("group row vanished")?;
    chk!(
        row.get("conflation") == Some(&Value::Bool(true)),
        "the stored declaration must be UNCHANGED by the disagreeing consumer (§3.3): {row}"
    );
    let a = ack(h, &s(&m[0], "transactionId"), &s(&m[0], "partitionId"), "completed", g, &s(&m[0], "leaseId")).await?;
    chk!(a.get("success") == Some(&Value::Bool(true)), "cleanup ack: {a}");
    Ok(())
}

// M5 / §3.2 — sizing. With no `partitions` param the batch budget stops sizing a
// conflating pop (each partition yields ≤1 message), so max_parts must be raised
// to `batch`, clamped to the measured 64-checkout ceiling. One message per
// partition on purpose: only that shape has an exact expected width (the ring's
// budget estimate may legally narrow claims on BACKLOGGED partitions — §10 Q4).
async fn c07_sizing_batch_partitions(h: &Http, c: &Client) -> Case {
    // Leg 1: 8 one-message partitions, batch=8, no partitions param.
    let q = unique("cfl-size8");
    let g = "workers";
    configure(h, &q, json!({})).await?;
    let items: Vec<Value> = (0..8).map(|i| push_item(&q, &format!("P{i:02}"), json!({"n": i}))).collect();
    push(h, items).await?;
    let v = pop_ok(h, &q, &format!("consumerGroup={g}&subscriptionMode=all&conflation=true&batch=8")).await?;
    let m = msgs(&v);
    chk!(
        m.len() == 8,
        "a conflating pop with batch=8 and NO partitions param must claim up to 8 \
         partitions, one tail each (M5/§3.2); got {} messages — the default partitions=1 \
         is still sizing the pop",
        m.len()
    );
    let pids: HashSet<String> = m.iter().map(|x| s(x, "partitionId")).collect();
    chk!(pids.len() == 8, "the 8 messages must come from 8 DISTINCT partitions, got {}", pids.len());
    chk!(
        v.get("partitionsClaimed").and_then(|x| x.as_i64()) == Some(8),
        "partitionsClaimed must report 8: {v}"
    );
    let acks: Vec<(String, String, String)> = m
        .iter()
        .map(|x| (s(x, "transactionId"), s(x, "partitionId"), s(x, "leaseId")))
        .collect();
    for r in ack_batch_completed(h, g, &acks).await? {
        chk!(r.get("success") == Some(&Value::Bool(true)), "leg-1 ack failed: {r}");
    }
    chk!(pending_partitions(c, &q, g).await? == 0, "leg 1 must be fully drained");

    // Leg 2: 70 one-message partitions, batch=200 — the hard 64 ceiling.
    let q2 = unique("cfl-size70");
    configure(h, &q2, json!({})).await?;
    let items: Vec<Value> = (0..70).map(|i| push_item(&q2, &format!("P{i:02}"), json!({"n": i}))).collect();
    push(h, items).await?;
    let mut total = 0usize;
    let mut largest = 0usize;
    for _ in 0..90 {
        let v = pop_ok(h, &q2, &format!("consumerGroup={g}&subscriptionMode=all&conflation=true&batch=200")).await?;
        let n = msgs(&v).len();
        chk!(
            n <= 64,
            "a conflating pop returns AT MOST 64 messages per round trip, whatever \
             `batch` says (M5: the checkout ceiling is load-bearing and measured); got {n}"
        );
        if n == 0 {
            break;
        }
        largest = largest.max(n);
        total += n;
        // Leases hold the claimed partitions; no ack needed to drain distinct ones.
    }
    chk!(total == 70, "every partition's tail must be delivered exactly once: got {total}/70");
    chk!(
        largest == 64,
        "with 70 ready one-message partitions and batch=200 the widest pop must claim \
         exactly the 64-ceiling (M5), got {largest}"
    );
    Ok(())
}

// §2.5 / §5.3 — depth. For a conflating group `pending` stays LOG depth while
// `effectivePending` is WORK depth (= partitions with work), plus the
// `partitionsPending` count and the `conflation` echo.
async fn c08_depth_fields(h: &Http, c: &Client) -> Case {
    let q = unique("cfl-depth");
    let g = "workers";
    configure(h, &q, json!({})).await?;
    // Register the group (with the flag) BEFORE the backlog exists, consuming nothing.
    let v = pop_ok(h, &q, &format!("consumerGroup={g}&subscriptionMode=all&conflation=true")).await?;
    chk!(msgs(&v).is_empty(), "registration pop delivers nothing: {v}");

    push_n(h, &q, "A", 0, 100).await?;
    push_n(h, &q, "B", 0, 1).await?;
    push_n(h, &q, "C", 0, 1).await?;
    // Retire partition C for this group so one partition has pending 0.
    let (code, v) = pop_part(h, &q, "C", &format!("consumerGroup={g}&conflation=true")).await?;
    chk!(code == 200, "pinned pop of C -> {code}: {v}");
    let m = msgs(&v);
    chk!(m.len() == 1, "C holds one message, got {}", m.len());
    let a = ack(h, &s(&m[0], "transactionId"), &s(&m[0], "partitionId"), "completed", g, &s(&m[0], "leaseId")).await?;
    chk!(a.get("success") == Some(&Value::Bool(true)), "ack C: {a}");

    // Ground truth from the tables first, so a wrong depth number reads as a
    // depth bug and never as a fixture bug.
    chk!(
        pending_partitions(c, &q, g).await? == 2,
        "fixture: exactly A and B must be pending for {g} at this point"
    );

    let (code, d) = h
        .req("GET", &format!("/api/v1/resources/queues/{q}/depth?group={g}"), None)
        .await?;
    chk!(code == 200, "depth -> {code}: {d}");
    chk!(
        d.get("pending").and_then(|x| x.as_i64()) == Some(101),
        "`pending` stays LOG depth — 101 positions to retire (§5.3 presentation rule): {d}"
    );
    chk!(
        d.get("partitionsPending").and_then(|x| x.as_i64()) == Some(2),
        "/depth must gain partitionsPending = partitions with pending > 0 (A and B) \
         (§2.5): {d}"
    );
    chk!(
        d.get("conflation") == Some(&Value::Bool(true)),
        "/depth must echo the group's stored conflation (§2.5): {d}"
    );
    chk!(
        d.get("effectivePending").and_then(|x| x.as_i64()) == Some(2),
        "effectivePending for a conflating group is WORK depth — partitions with work \
         (2), NOT log depth (101) (§2.5/§5.3): {d}"
    );
    // The pre-existing shape must survive underneath the new fields.
    let parts = d.get("partitions").and_then(|p| p.as_array()).cloned().unwrap_or_default();
    chk!(parts.len() == 3, "the per-partition array keeps listing all 3 partitions: {d}");
    Ok(())
}

// M4 / §2.3 step 2 — a group that only ever used PINNED pops must still end up
// with a durable consumer_groups_metadata row carrying the flag. Today
// log_pop_v1 reads the table but never writes it, so the row simply
// does not exist for such a group.
async fn c09_pinned_pop_registers_metadata(h: &Http, c: &Client) -> Case {
    let q = unique("cfl-pinned");
    let g = "pinned-only";
    configure(h, &q, json!({})).await?;
    push_n(h, &q, "Default", 0, 1).await?;

    let (code, v) = pop_part(h, &q, "Default", &format!("consumerGroup={g}&subscriptionMode=all&conflation=true")).await?;
    chk!(code == 200, "pinned pop -> {code}: {v}");
    let m = msgs(&v);
    chk!(m.len() == 1, "the single message is delivered, got {}", m.len());

    let row = cgm_row(c, &q, g).await?;
    let row = row.ok_or(
        "a group that only ever used pinned pops has NO durable consumer_groups_metadata \
         row — the M4 hole is open (§2.3 step 2 must write the same row the wildcard \
         path writes)",
    )?;
    chk!(
        row.get("conflation") == Some(&Value::Bool(true)),
        "and the durable row must carry conflation = true: {row}"
    );
    let a = ack(h, &s(&m[0], "transactionId"), &s(&m[0], "partitionId"), "completed", g, &s(&m[0], "leaseId")).await?;
    chk!(a.get("success") == Some(&Value::Bool(true)), "cleanup ack: {a}");
    Ok(())
}

// Default-off regression guard — a group WITHOUT the flag behaves
// byte-identically to today: full delivery spans, no new response keys, same
// counters. This case must be GREEN before the implementation lands and MUST
// STAY GREEN after it.
async fn c10_default_off_regression(h: &Http, c: &Client) -> Case {
    let q = unique("cfl-off");
    let g = "plain";
    configure(h, &q, json!({})).await?;
    for part in ["A", "B"] {
        push_n(h, &q, part, 0, 3).await?;
    }

    let v = pop_ok(h, &q, &format!("consumerGroup={g}&subscriptionMode=all&batch=100&partitions=2")).await?;
    let m = msgs(&v);
    chk!(
        m.len() == 6,
        "a flag-off pop must deliver the FULL span — all 6 messages across 2 partitions; \
         got {}",
        m.len()
    );
    chk!(
        v.get("conflation").is_none(),
        "a flag-off response must NOT carry a conflation key (byte-identical, §3.1): {v}"
    );
    chk!(
        v.get("conflationConflict").is_none(),
        "nor a conflationConflict key: {v}"
    );

    let acks: Vec<(String, String, String)> = m
        .iter()
        .map(|x| (s(x, "transactionId"), s(x, "partitionId"), s(x, "leaseId")))
        .collect();
    let results = ack_batch_completed(h, g, &acks).await?;
    for r in &results {
        chk!(r.get("success") == Some(&Value::Bool(true)), "flag-off ack failed: {r}");
        chk!(
            r.get("conflated").is_none(),
            "a flag-off ack result must NOT carry a conflated field (§2.4): {r}"
        );
    }

    // Attempt fields are deliberately NOT asserted here: which ack path serves
    // a clean full-batch ack (registry fast path vs hash path) is an internal
    // choice and the two differ on attempt-state reset today. The byte-identical
    // guard is about delivery spans, response keys and the consumption counters.
    for part in ["A", "B"] {
        let row = consumer_must(c, &q, part, g).await?;
        chk!(ci64(&row, "committed") == 2, "{part}: full span acked → committed = 2: {row}");
        chk!(ci64(&row, "total_consumed") == 3, "{part}: total_consumed = 3: {row}");
        chk!(ci64(&row, "batch_retry_count") == 0, "{part}: untouched budget: {row}");
        chk!(cnull(&row, "batch_end"), "{part}: lease closed: {row}");
    }

    let v = pop_ok(h, &q, &format!("consumerGroup={g}&partitions=2")).await?;
    chk!(msgs(&v).is_empty(), "drained queue must pop empty: {v}");
    Ok(())
}

// §3.1 / §4 — the degrade-loudly hinge: the broker echoes "conflation":true on
// EMPTY conflating pop responses too. An SDK that requested conflation and does
// not see the echo must error on the FIRST round trip, before any message is
// processed — which only works if the empty response carries the key.
async fn c11_empty_pop_echoes_conflation(h: &Http) -> Case {
    let q = unique("cfl-echo");
    configure(h, &q, json!({})).await?;

    let (code, v) = pop_queue(h, &q, "consumerGroup=g&subscriptionMode=all&conflation=true").await?;
    chk!(code == 200 || code == 204, "empty conflating pop -> {code}: {v}");
    chk!(msgs(&v).is_empty(), "the queue is empty: {v}");
    chk!(
        v.get("conflation") == Some(&Value::Bool(true)),
        "an EMPTY conflating pop response must still echo \"conflation\":true — the §8 \
         degrade-loudly contract fires on the first round trip only because of this key \
         (§3.1, §4). Today the empty pop is a BODILESS 204, which cannot carry the echo \
         at all; got {code} with body {v}"
    );

    // Control: no flag, no key — on the same empty queue.
    let v = pop_ok(h, &q, "consumerGroup=g2&subscriptionMode=all").await?;
    chk!(msgs(&v).is_empty(), "control pop is empty too: {v}");
    chk!(
        v.get("conflation").is_none(),
        "a flag-off empty response carries no conflation key: {v}"
    );
    Ok(())
}

// §7.1.5 guard — delayed_processing hides EVERYTHING: a conflating pop delivers
// zero AND the zero-taken cursor seal must NOT fire (segments exist, they are
// merely deferred). This is the data-loss test for §2.3's two-leg LATERAL; it
// is GREEN today and must stay green.
async fn c12_delayed_all_deferred(h: &Http, c: &Client) -> Case {
    let q = unique("cfl-delay-all");
    let g = "workers";
    configure(h, &q, json!({"delayedProcessing": 5})).await?;
    push_n(h, &q, "Default", 0, 10).await?;

    let v = pop_ok(h, &q, &format!("consumerGroup={g}&subscriptionMode=all&conflation=true")).await?;
    chk!(
        msgs(&v).is_empty(),
        "with every segment younger than delayedProcessing the pop must deliver ZERO \
         (§7.1.5): {v}"
    );
    let row = consumer_must(c, &q, "Default", g).await?;
    chk!(
        ci64(&row, "committed") == -1,
        "and committed must NOT move — the seal must not fire past deferred segments \
         (§7.1.5, the data-loss case): {row}"
    );
    Ok(())
}

// §7.1.6 — delayed_processing, partial: the tail is the newest VISIBLE offset,
// never the newest allocated one. Push 10, wait past the deadline, push 5 more:
// the conflating pop must deliver offset 9 — and the ack must commit to 9,
// leaving the deferred suffix pending (the §1.3 corollary at work).
async fn c13_delayed_partial(h: &Http, c: &Client) -> Case {
    let q = unique("cfl-delay-part");
    let g = "workers";
    configure(h, &q, json!({"delayedProcessing": 2})).await?;
    push_n(h, &q, "Default", 0, 10).await?; // offsets 0..9
    tokio::time::sleep(Duration::from_millis(2600)).await; // 0..9 mature
    push_n(h, &q, "Default", 10, 5).await?; // offsets 10..14, still deferred

    let v = pop_ok(h, &q, &format!("consumerGroup={g}&subscriptionMode=all&conflation=true")).await?;
    let m = msgs(&v);
    chk!(
        m.len() == 1,
        "conflating pop must deliver exactly one message (the newest VISIBLE), got {}",
        m.len()
    );
    chk!(
        msg_n(&m[0]) == 9,
        "the delivered offset must be 9 — newest VISIBLE — not 14 (§7.1.6): got n={}",
        msg_n(&m[0])
    );
    let a = ack(h, &s(&m[0], "transactionId"), &s(&m[0], "partitionId"), "completed", g, &s(&m[0], "leaseId")).await?;
    chk!(a.get("success") == Some(&Value::Bool(true)), "ack: {a}");
    let row = consumer_must(c, &q, "Default", g).await?;
    chk!(
        ci64(&row, "committed") == 9,
        "committed lands on 9 — never past an offset that was not visible at pop time \
         (§1.2/§1.3): {row}"
    );
    chk!(
        pending_partitions(c, &q, g).await? == 1,
        "the deferred suffix (10..14) must still read as pending"
    );
    Ok(())
}

// §7.1.7 guard — the empty-partition cursor seal still works for a conflating
// group: once retention has removed every segment, the pop seals the cursor to
// the partition's last_offset instead of leaving it phantom-pending forever.
// GREEN today; the conflating zero-taken path must keep reaching the seal.
async fn c14_empty_partition_seal(h: &Http, c: &Client) -> Case {
    let q = unique("cfl-seal");
    let g = "workers";
    configure(h, &q, json!({})).await?;
    push_n(h, &q, "Default", 0, 3).await?;

    // Deliver and RELEASE (budget-free retry) so committed stays -1 with a
    // last_offset of 2 — the phantom-pending shape.
    let v = pop_ok(h, &q, &format!("consumerGroup={g}&subscriptionMode=all&conflation=true")).await?;
    let m = msgs(&v);
    chk!(!m.is_empty(), "seed pop must deliver something: {v}");
    let a = ack(h, &s(&m[0], "transactionId"), &s(&m[0], "partitionId"), "retry", g, &s(&m[0], "leaseId")).await?;
    chk!(a.get("success") == Some(&Value::Bool(true)), "retry-release: {a}");
    let row = consumer_must(c, &q, "Default", g).await?;
    chk!(ci64(&row, "committed") == -1, "budget-free retry keeps the cursor: {row}");

    let pid = partition_id(c, &q, "Default").await?;
    wipe_segments(c, &pid).await?; // "retention removed every segment"

    let v = pop_ok(h, &q, &format!("consumerGroup={g}&conflation=true")).await?;
    chk!(msgs(&v).is_empty(), "nothing left to deliver: {v}");
    let row = consumer_must(c, &q, "Default", g).await?;
    chk!(
        ci64(&row, "committed") == 2,
        "the zero-taken seal must advance the cursor to last_offset (2) for a conflating \
         group too (§7.1.7): {row}"
    );
    Ok(())
}

// §7.1.8 guard — window_buffer is a partition-level all-or-nothing gate and
// composes with conflation unchanged: a conflating pop on a HOT partition
// delivers nothing; after the window it delivers, newest included.
async fn c15_window_buffer_gate(h: &Http, c: &Client) -> Case {
    let q = unique("cfl-window");
    let g = "workers";
    configure(h, &q, json!({"windowBuffer": 2})).await?;
    push_n(h, &q, "Default", 0, 3).await?;

    let v = pop_ok(h, &q, &format!("consumerGroup={g}&subscriptionMode=all&conflation=true")).await?;
    chk!(
        msgs(&v).is_empty(),
        "a conflating pop on a partition written within windowBuffer must deliver \
         NOTHING (§7.1.8): {v}"
    );
    let row = consumer_must(c, &q, "Default", g).await?;
    chk!(ci64(&row, "committed") == -1, "and must not move the cursor: {row}");

    tokio::time::sleep(Duration::from_millis(2300)).await;
    let v = pop_ok(h, &q, &format!("consumerGroup={g}&conflation=true")).await?;
    let m = msgs(&v);
    chk!(!m.is_empty(), "after the window the partition serves again: {v}");
    let newest = m.iter().map(msg_n).max().unwrap_or(-1);
    chk!(newest == 2, "and the newest (n=2) is among the delivered, got max n={newest}");
    Ok(())
}

// §7.1.10 guard — concurrency: several consumers of one conflating group over
// many partitions, and no partition is ever in two workers' hands at once
// (§1.6: the claim's FOR UPDATE SKIP LOCKED needs no new locking). GREEN today
// with the flag ignored; must stay green once the flag is honored.
async fn c16_concurrency_one_worker_per_partition(h: &Http, c: &Client) -> Case {
    const PARTS: usize = 30;
    const WORKERS: usize = 6;
    let q = unique("cfl-conc");
    let g = "swarm";
    configure(h, &q, json!({})).await?;
    let mut items = Vec::new();
    for p in 0..PARTS {
        for n in 0..5 {
            items.push(push_item(&q, &format!("P{p:02}"), json!({"n": n})));
        }
    }
    push(h, items).await?;

    let in_flight: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let violation: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let stop = Arc::new(AtomicBool::new(false));

    let mut tasks = Vec::new();
    for w in 0..WORKERS {
        let (h, q, in_flight, violation, stop) =
            (h.clone(), q.clone(), in_flight.clone(), violation.clone(), stop.clone());
        tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let v = match pop_ok(&h, &q, &format!("consumerGroup={g}&subscriptionMode=all&conflation=true&batch=64&partitions={PARTS}")).await {
                    Ok(v) => v,
                    Err(e) => return Err(format!("worker {w}: {e}")),
                };
                let m = msgs(&v);
                if m.is_empty() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                // One pop claims each partition at most once; two LIVE claims of
                // one partition across workers is the §1.6 violation.
                let pids: HashSet<String> = m.iter().map(|x| s(x, "partitionId")).collect();
                {
                    let mut fl = in_flight.lock().unwrap();
                    for pid in &pids {
                        if !fl.insert(pid.clone()) {
                            *violation.lock().unwrap() = Some(format!(
                                "partition {pid} was claimed by worker {w} while another \
                                 worker's lease on it was still open"
                            ));
                        }
                    }
                }
                let acks: Vec<(String, String, String)> = m
                    .iter()
                    .map(|x| (s(x, "transactionId"), s(x, "partitionId"), s(x, "leaseId")))
                    .collect();
                let res = ack_batch_completed(&h, g, &acks).await;
                {
                    let mut fl = in_flight.lock().unwrap();
                    for pid in &pids {
                        fl.remove(pid);
                    }
                }
                for r in res? {
                    if r.get("success") != Some(&Value::Bool(true)) {
                        return Err(format!("worker {w}: ack failed: {r}"));
                    }
                }
            }
            Ok::<(), String>(())
        }));
    }

    // Drain watch: all partitions retired for the group, or a 25s deadline.
    let deadline = Instant::now() + Duration::from_secs(25);
    let mut drained = false;
    while Instant::now() < deadline {
        if pending_partitions(c, &q, g).await? == 0 {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await.map_err(|e| format!("join: {e}"))??;
    }
    if let Some(vio) = violation.lock().unwrap().clone() {
        return Err(vio);
    }
    chk!(drained, "the swarm must drain all {PARTS} partitions within the deadline");
    Ok(())
}

// §7.1.11 — boot idempotence: restart the broker binary so the embedded schema
// re-applies on a POPULATED database, then require (a) a clean second boot (the
// three updated DROP FUNCTION guards must match, or boot re-apply breaks —
// §2.3) and (b) the two ADD COLUMN IF NOT EXISTS columns to exist.
async fn c17_boot_idempotence(broker: &mut BrokerProc, c: &Client) -> Case {
    broker
        .restart()
        .await
        .map_err(|e| format!("second boot must re-apply the schema cleanly (the DROP \
                              FUNCTION signature guards of §2.3): {e}"))?;
    let h = broker.http.clone();

    // Smoke: the re-applied schema still serves.
    let q = unique("cfl-reboot");
    configure(&h, &q, json!({})).await?;
    push_n(&h, &q, "Default", 0, 1).await?;
    let v = pop_ok(&h, &q, "consumerGroup=g&subscriptionMode=all").await?;
    chk!(msgs(&v).len() == 1, "post-restart pop must serve: {v}");

    chk!(
        column_exists(c, "consumer_groups_metadata", "conflation").await?,
        "queen.consumer_groups_metadata.conflation must exist after a double boot \
         (§2.1 ADD COLUMN IF NOT EXISTS) — the schema half of the feature is missing"
    );
    chk!(
        column_exists(c, "log_consumers", "lease_conflated").await?,
        "queen.log_consumers.lease_conflated must exist after a double boot (§2.2) — \
         the schema half of the feature is missing"
    );
    Ok(())
}

// §7.1.12 — the transaction wire: a conflating ack bundled with a push through
// POST /api/v1/transaction commits, and the cursor lands on batch_end — the
// 1.0.5 bogus-ack atomicity check must NOT fire on the cursor jump (§2.4: the
// hash always resolves; the wire never inspects the advance).
async fn c18_transaction_wire(h: &Http, c: &Client) -> Case {
    let qa = unique("cfl-txn-a");
    let qb = unique("cfl-txn-b");
    let g = "workers";
    configure(h, &qa, json!({})).await?;
    configure(h, &qb, json!({})).await?;
    push_n(h, &qa, "Default", 0, 5).await?;

    let v = pop_ok(h, &qa, &format!("consumerGroup={g}&subscriptionMode=all&conflation=true")).await?;
    let m = msgs(&v);
    chk!(m.len() == 1, "conflating pop delivers one message, got {}", m.len());
    chk!(msg_n(&m[0]) == 4, "and it is the tail (n=4), got n={}", msg_n(&m[0]));
    let (txn, pid, lease) = (s(&m[0], "transactionId"), s(&m[0], "partitionId"), s(&m[0], "leaseId"));

    let (code, t) = h
        .req(
            "POST",
            "/api/v1/transaction",
            Some(&json!({
                "operations": [
                    {"type": "ack", "transactionId": txn, "partitionId": pid,
                     "status": "completed", "consumerGroup": g, "leaseId": lease},
                    {"type": "push", "items": [{"queue": qb, "payload": {"stage": 2}}]}
                ],
                "requiredLeases": [lease]
            })),
        )
        .await?;
    chk!((200..300).contains(&code), "transaction -> {code}: {t}");
    chk!(
        t.get("success") == Some(&Value::Bool(true)),
        "the bundle must COMMIT — the bogus-ack rule must not fire on a conflating \
         cursor jump (§2.4): {t}"
    );
    let results = t.get("results").and_then(|r| r.as_array()).cloned().unwrap_or_default();
    chk!(
        !results.is_empty() && results.iter().all(|r| r.get("success") == Some(&Value::Bool(true))),
        "every op of the bundle must succeed: {t}"
    );

    let row = consumer_must(c, &qa, "Default", g).await?;
    chk!(
        ci64(&row, "committed") == 4,
        "the wire ack must land the cursor ON batch_end (4): {row}"
    );
    chk!(cnull(&row, "batch_end"), "and close the lease: {row}");

    // The bundled push is live and poppable.
    let v = pop_ok(h, &qb, "consumerGroup=audit&subscriptionMode=all").await?;
    let m = msgs(&v);
    chk!(m.len() == 1, "the bundled push must be poppable from {qb}: {v}");
    chk!(
        m[0].get("data") == Some(&json!({"stage": 2})),
        "and carry the pushed payload: {}",
        m[0]
    );
    Ok(())
}

// ======================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn conflation_semantics() {
    let (pg_host, pg_port) = pg_target();
    let mut broker = BrokerProc::spawn(&pg_host, pg_port)
        .await
        .expect("broker spawn");
    let c = connect(&pg_host, pg_port).await;
    let h = broker.http.clone();

    let mut report: Vec<(&str, Case)> = Vec::new();
    report.push(("tail_only_delivery_and_cursor_jump", c01_tail_only_delivery_and_cursor_jump(&h, &c).await));
    report.push(("the_guarantee", c02_the_guarantee(&h, &c).await));
    report.push(("retry_supersession_budget_dlq", c03_retry_supersession_budget_dlq(&h, &c).await));
    report.push(("mode_composition", c04_mode_composition(&h, &c).await));
    report.push(("refusals_400", c05_refusals(&h).await));
    report.push(("conflict_stored_wins", c06_conflict_stored_wins(&h, &c).await));
    report.push(("sizing_batch_partitions", c07_sizing_batch_partitions(&h, &c).await));
    report.push(("depth_fields", c08_depth_fields(&h, &c).await));
    report.push(("pinned_pop_registers_metadata", c09_pinned_pop_registers_metadata(&h, &c).await));
    report.push(("default_off_regression_guard", c10_default_off_regression(&h, &c).await));
    report.push(("empty_pop_echoes_conflation", c11_empty_pop_echoes_conflation(&h).await));
    report.push(("delayed_all_deferred_guard", c12_delayed_all_deferred(&h, &c).await));
    report.push(("delayed_partial_newest_visible", c13_delayed_partial(&h, &c).await));
    report.push(("empty_partition_seal_guard", c14_empty_partition_seal(&h, &c).await));
    report.push(("window_buffer_gate_guard", c15_window_buffer_gate(&h, &c).await));
    report.push(("concurrency_one_worker_per_partition_guard", c16_concurrency_one_worker_per_partition(&h, &c).await));
    report.push(("transaction_wire", c18_transaction_wire(&h, &c).await));
    // Last on purpose: it kills and re-spawns the broker.
    report.push(("boot_idempotence", c17_boot_idempotence(&mut broker, &c).await));

    println!("\n================= conflation semantics (PLAN_CONFLATION §1/§7.1) =================");
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
        "=========================== {}/{} passed ===========================\n",
        report.len() - failed,
        report.len()
    );
    let _ = broker.child.kill().await;
    assert_eq!(
        failed, 0,
        "{failed} conflation semantics case(s) failed — see the table above (RED IS THE \
         EXPECTED STATE until PLAN_CONFLATION.md is implemented)"
    );
}
