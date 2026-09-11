//! THE CONSOLE'S KV ROUTES ON THE WIRE — `GET /api/v1/resources/kv/namespaces`
//! and `POST /api/v1/resources/kv/list`, against the real broker binary on a
//! real Postgres. PLAN_DASHBOARD_ACTIONS.md §2.5.
//!
//! WHY THIS EXISTS BESIDE `kv_console_list.rs`, WHICH ALREADY TESTS THE RULES.
//! That suite calls the stored procedures directly, deliberately: every rule of
//! the listing lives in SQL so that the route, a direct SQL caller and the
//! embedded broker inherit one implementation. What it cannot see is the SEAM —
//! the three layers between an HTTP request and that procedure — and the seam
//! has failure modes of its own that no SQL test can fail on:
//!
//!   * `db::kv_list` binds seven parameters positionally, and `keys_only` and
//!     `include_expired` are ADJACENT BOOLEANS. Swap them and Postgres cannot
//!     complain: the console then asks for `keysOnly=true, includeExpired=false`
//!     and the page renders every value blank and every expired row missing —
//!     the exact two failures `kv_console_list.rs`'s header calls "one boolean
//!     away, and none of them raises". Both are pinned here by one request.
//!   * the ladder. Both handlers open with `gated(…, Surface::KvRead, 0, 0)`,
//!     which is the KV kill switch and the per-tenant read rate. Delete those
//!     three lines from either handler and every other test in this repository
//!     still passes — an operator who pauses KV reads during an incident would
//!     find the console still reading Postgres, and a Viewer would be the one
//!     party whose reads are not counted.
//!   * the envelopes. The namespaces route WRAPS the procedure's bare array as
//!     `{"namespaces": […]}`; the list route passes its object through verbatim.
//!     A change to either shape is a change to the dashboard's contract.
//!   * the query-string refusal, which is the reason the list is a POST at all
//!     (§5.5: a cursor is a key) — asserted on the SELECTOR too, where it is
//!     otherwise unobservable: that route reads no parameter, so only a test can
//!     say whether the surface's rule holds on all five KV routes or on four.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`,
//! the convention every rig suite here follows:
//!
//! ```bash
//! docker run --rm -d --name queen-w4-routes-pg -e POSTGRES_PASSWORD=postgres -p 5483:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5483 cargo test --test kv_console_routes -- --ignored --nocapture
//! ```
//!
//! ONE test function on purpose (one spawned broker per run). The cases are
//! still reported one by one: each returns `Result<(), String>` and the runner
//! prints a PASS/FAIL line per case before failing, so a red run names every
//! broken rule instead of only the first.

use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::Client;

/// `config::DEFAULT_TENANT` — tenancy is off on the spawned broker, so every row
/// this suite writes and inspects lives under the default tenant.
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
    format!("{prefix}{nanos}")
}

// ======================================================================
// throwaway-PG plumbing (the QUEEN_EMBEDDED_TEST_PG convention)
// ======================================================================

fn pg_target() -> (String, u16) {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port) for the KV console route tests");
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
    #[allow(dead_code)]
    child: tokio::process::Child,
    http: Http,
    /// Kept for post-mortem: the path is printed by every boot failure.
    #[allow(dead_code)]
    log_path: std::path::PathBuf,
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    l.local_addr().expect("local_addr").port()
}

impl BrokerProc {
    async fn spawn(pg_host: &str, pg_port: u16) -> Result<BrokerProc, String> {
        let port = free_port();
        let log_path = std::env::temp_dir().join(format!(
            "queen-kv-console-broker-{}-{}.log",
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
            log_path,
        })
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
// (the same one conflation_semantics.rs uses)
// ======================================================================

#[derive(Clone)]
struct Http {
    addr: String,
}

impl Http {
    async fn req(
        &self,
        method: &str,
        path_query: &str,
        body: Option<&Value>,
    ) -> Result<(u16, Value), String> {
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
            s.write_all(payload.as_bytes())
                .await
                .map_err(|e| format!("write body: {e}"))?;
        }
        let mut raw = Vec::with_capacity(16 * 1024);
        s.read_to_end(&mut raw).await.map_err(|e| format!("read: {e}"))?;
        parse_http(&raw)
    }
}

fn parse_http(raw: &[u8]) -> Result<(u16, Value), String> {
    let sep = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| {
            format!(
                "no header/body separator in: {:.200}",
                String::from_utf8_lossy(raw)
            )
        })?;
    let head = String::from_utf8_lossy(&raw[..sep]).to_string();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| format!("bad status line: {status_line}"))?;
    let chunked = lines.filter_map(|l| l.split_once(':')).any(|(k, v)| {
        k.trim().eq_ignore_ascii_case("transfer-encoding") && v.to_ascii_lowercase().contains("chunked")
    });
    let mut body = raw[sep + 4..].to_vec();
    if chunked {
        body = dechunk(&body)?;
    }
    if body.is_empty() {
        return Ok((code, Value::Null));
    }
    let v: Value = serde_json::from_slice(&body).map_err(|e| {
        format!(
            "non-JSON body ({e}): {:.400}",
            String::from_utf8_lossy(&body)
        )
    })?;
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
        i += size + 2;
    }
}

// ======================================================================
// helpers on the surface under test
// ======================================================================

/// Seed through the product's own write path, the KV PUT route, so the rows
/// under test are rows the product could have written.
async fn put(h: &Http, ns: &str, key: &str, value: Value) -> Result<(), String> {
    let (code, v) = h
        .req(
            "PUT",
            &format!("/api/v1/kv/{ns}/{key}"),
            Some(&json!({"value": value, "ttlSeconds": 600})),
        )
        .await?;
    chk!((200..300).contains(&code), "PUT {ns}/{key} -> {code}: {v}");
    Ok(())
}

async fn list(h: &Http, body: Value) -> Result<(u16, Value), String> {
    h.req("POST", "/api/v1/resources/kv/list", Some(&body)).await
}

fn rows_of(page: &Value) -> Vec<Value> {
    page.get("rows")
        .and_then(|r| r.as_array())
        .cloned()
        .unwrap_or_default()
}

fn row(page: &Value, key: &str) -> Option<Value> {
    rows_of(page).into_iter().find(|r| r.get("key") == Some(&json!(key)))
}

/// Push a row's expiry into the past, in place — the same move
/// `kv_console_list.rs` makes, and for the same reason: what is under test is
/// the predicate, not the sweeper.
async fn expire_now(c: &Client, ns: &str, key: &str) -> Result<(), String> {
    let n = c
        .execute(
            "UPDATE queen.kv SET expires_at = now() - interval '1 minute' \
             WHERE tenant_id = $1::text::uuid AND namespace = $2 AND key = $3",
            &[&TENANT, &ns, &key],
        )
        .await
        .map_err(|e| format!("expire_now({key}): {e}"))?;
    chk!(n == 1, "expire_now({key}) touched {n} rows, expected 1");
    Ok(())
}

async fn set_kv_surface(h: &Http, on: bool) -> Result<(), String> {
    let (code, v) = h
        .req("POST", "/api/v1/system/kv-timers", Some(&json!({"kv": on})))
        .await?;
    chk!((200..300).contains(&code), "kv switch -> {on}: {code}: {v}");
    chk!(
        v.get("kvEnabled") == Some(&Value::Bool(on)),
        "the switch must echo the state it is now in: {v}"
    );
    Ok(())
}

// ===========================================================================
// THE ENVELOPES, AND THE SEVEN BINDS UNDER THEM.
//
// One namespace with a live key and an expired one, read four ways. Every
// assertion here fails if `db::kv_list` binds its two booleans in the other
// order, which is the point: that swap is invisible to Postgres (both are
// `bool`), invisible to the SQL suite (it binds its own statement) and visible
// on screen only as "the values are all blank and some keys are missing".
// ===========================================================================
async fn case_the_two_routes_answer_their_envelopes(h: &Http, c: &Client) -> Case {
    let ns = unique("kvroute");
    put(h, &ns, "alive", json!({"a": 1})).await?;
    put(h, &ns, "dead", json!({"b": 2})).await?;
    expire_now(c, &ns, "dead").await?;

    // ---- the namespace selector -------------------------------------------
    let (code, v) = h.req("GET", "/api/v1/resources/kv/namespaces", None).await?;
    chk!(code == 200, "GET namespaces -> {code}: {v}");
    let arr = v
        .get("namespaces")
        .and_then(|n| n.as_array())
        .cloned()
        .ok_or_else(|| format!(
            "the route WRAPS the procedure's bare array as {{\"namespaces\": […]}} — the \
             dashboard reads that key: {v}"
        ))?;
    let mine = arr
        .iter()
        .find(|e| e.get("namespace") == Some(&json!(ns)))
        .cloned()
        .ok_or_else(|| format!("{ns} is missing from the selector: {v}"))?;
    chk!(
        mine.get("keys") == Some(&json!(2)),
        "the selector counts EVERY row of the namespace, the expired one included \
         (§2.5 D5), so that the page and its own count agree: {mine}"
    );

    // ---- the console's own page -------------------------------------------
    let (code, page) = list(h, json!({"namespace": ns, "includeExpired": true})).await?;
    chk!(code == 200, "POST list -> {code}: {page}");
    for f in ["rows", "truncated", "bytes"] {
        chk!(
            page.get(f).is_some(),
            "the page envelope is passed through verbatim and carries {f}: {page}"
        );
    }
    chk!(
        page.get("nextAfter").is_some(),
        "…and nextAfter, null on a page that ended: {page}"
    );
    chk!(
        rows_of(&page).len() == 2,
        "both rows must be on the page: {page}"
    );
    let alive = row(&page, "alive").ok_or("the live row is missing")?;
    chk!(
        alive.get("value") == Some(&json!({"a": 1})),
        "a row of a values page carries its value — a blank column here is the \
         keys_only/include_expired bind swap: {alive}"
    );
    chk!(
        alive.get("expired") == Some(&Value::Bool(false)),
        "a live row reads expired:false: {alive}"
    );
    let dead = row(&page, "dead").ok_or_else(|| format!(
        "the expired row must be SHOWN and labelled when the console asks for it — \
         missing here is the same bind swap seen from the other side: {page}"
    ))?;
    chk!(
        dead.get("expired") == Some(&Value::Bool(true)),
        "the expired row must carry expired:true so the page can grey it: {dead}"
    );
    chk!(
        page.get("bytes").and_then(|b| b.as_i64()).unwrap_or(0) > 0,
        "a values page charges value bytes against the 4 MiB budget: {page}"
    );

    // ---- keysOnly: no values, and 0 bytes charged -------------------------
    let (code, ko) = list(h, json!({"namespace": ns, "keysOnly": true, "includeExpired": true})).await?;
    chk!(code == 200, "POST list keysOnly -> {code}: {ko}");
    chk!(
        rows_of(&ko).iter().all(|r| r.get("value").is_none()),
        "keysOnly omits the value field entirely (a `null` would be a stored null): {ko}"
    );
    chk!(
        rows_of(&ko).len() == 2 && ko.get("bytes") == Some(&json!(0)),
        "…and it still lists every row, charging no value bytes: {ko}"
    );

    // ---- the API default is the stored procedure's, not the console's -----
    let (code, plain) = list(h, json!({"namespace": ns})).await?;
    chk!(code == 200, "POST list (defaults) -> {code}: {plain}");
    chk!(
        rows_of(&plain).len() == 1 && row(&plain, "alive").is_some(),
        "a caller who did not ask for expired rows must not get them: §5.7 holds on \
         this route as on every other read, and queen.kv_list_v1 defaults the same \
         parameter to FALSE. The CONSOLE asks, explicitly, on every page: {plain}"
    );
    Ok(())
}

// ===========================================================================
// THE CURSOR TRAVELS IN THE BODY, AND A QUERY STRING IS REFUSED — ON BOTH.
//
// The list is a POST for one reason: its cursor is a KEY, and a key in a URL
// is recorded by the access log of the browser's proxy, the ingress, the queen
// proxy and the broker (§5.5). The refusal is what makes that rule structural
// rather than documentary — without it the route answers 200 and the key is in
// four logs.
//
// The SELECTOR is here too, and its case is the one that is easy to argue away:
// it reads no parameter, so ignoring a query string on it changes no answer
// today. What it changes is what the SURFACE says. `reference/http/kv.mdx`
// states the boundary route-wide — a query string on any KV route but the batch
// is a 400 — and five routes cannot answer that question two ways: a caller who
// learns from the selector that a stray parameter is tolerated is the caller who
// appends one to a route where it IS a key. Pinned here because it is otherwise
// invisible: the assertion is on a route that has no parameters to lose.
// ===========================================================================
async fn case_the_cursor_is_a_body_field_and_a_url_is_refused(h: &Http) -> Case {
    let ns = unique("kvcur");
    for k in ["k1", "k2", "k3"] {
        put(h, &ns, k, json!(1)).await?;
    }

    let (code, first) = list(h, json!({"namespace": ns, "limit": 2})).await?;
    chk!(code == 200, "first page -> {code}: {first}");
    chk!(
        first.get("truncated") == Some(&Value::Bool(true))
            && first.get("nextAfter") == Some(&json!("k2")),
        "a truncated page hands back the last key it printed as the cursor: {first}"
    );
    let (code, second) = list(h, json!({"namespace": ns, "limit": 2, "after": "k2"})).await?;
    chk!(code == 200, "second page -> {code}: {second}");
    chk!(
        rows_of(&second).len() == 1 && row(&second, "k3").is_some(),
        "the cursor is EXCLUSIVE: {second}"
    );

    // The same cursor in the URL, which is the thing the route exists to
    // prevent. It must not be served, and it must not be ignored.
    let (code, v) = h
        .req(
            "POST",
            "/api/v1/resources/kv/list?after=k2",
            Some(&json!({"namespace": ns})),
        )
        .await?;
    chk!(
        code == 400 && v.get("reason") == Some(&json!("kv_no_query_string")),
        "a query string on the console list must be REFUSED, not ignored — a 200 \
         here means the cursor reached four access logs and the page was served \
         anyway; got {code}: {v}"
    );

    // The selector, which reads no parameter and refuses one anyway. `?prefix=`
    // is deliberate: it is the spelling a caller carries over from the batch's
    // `getPrefix`, and it is a key fragment.
    let (code, v) = h
        .req("GET", "/api/v1/resources/kv/namespaces?prefix=quota:acme:", None)
        .await?;
    chk!(
        code == 400 && v.get("reason") == Some(&json!("kv_no_query_string")),
        "the selector must refuse a query string like its four siblings — it reads \
         no parameter, so a 200 here costs nothing TODAY and teaches the caller \
         that the rule the other KV routes enforce is advisory; got {code}: {v}"
    );
    // …and the refusal must not have become the answer to every request on it:
    // a plain GET still serves.
    let (code, v) = h.req("GET", "/api/v1/resources/kv/namespaces", None).await?;
    chk!(
        code == 200 && v.get("namespaces").is_some(),
        "the selector without a query string still answers its envelope: {code}: {v}"
    );
    Ok(())
}

// ===========================================================================
// THE LADDER APPLIES TO BOTH ROUTES.
//
// `gated(&st, tenant, Surface::KvRead, 0, 0)` is the first statement of both
// handlers, and it is the KV kill switch plus the per-tenant read rate. Nothing
// else in this repository fails when it is deleted, which is why this case
// exists: an operator who pauses KV reads mid-incident must not find the
// console still reading Postgres, and the dashboard's "paused on this cell"
// quiet state is branched on exactly this body (503 `kv_disabled`).
// ===========================================================================
async fn case_the_ladder_answers_on_both_routes(h: &Http) -> Case {
    let ns = unique("kvgate");
    put(h, &ns, "k", json!(1)).await?;

    set_kv_surface(h, false).await?;
    let paused = check_paused(h, &ns).await;
    // The switch is process-wide and mirrored into queen.system_state, so it is
    // restored whatever the assertions said.
    set_kv_surface(h, true).await?;
    paused?;

    let (code, _) = h.req("GET", "/api/v1/resources/kv/namespaces", None).await?;
    chk!(code == 200, "the selector must come back when the switch does: {code}");
    let (code, _) = list(h, json!({"namespace": ns})).await?;
    chk!(code == 200, "and so must the page: {code}");
    Ok(())
}

async fn check_paused(h: &Http, ns: &str) -> Case {
    let (code, v) = h.req("GET", "/api/v1/resources/kv/namespaces", None).await?;
    chk!(
        code == 503 && v.get("error") == Some(&json!("kv_disabled")),
        "with KV reads paused the selector must answer 503 kv_disabled — the code \
         the dashboard's quiet state branches on; got {code}: {v}"
    );
    let (code, v) = list(h, json!({"namespace": ns})).await?;
    chk!(
        code == 503 && v.get("error") == Some(&json!("kv_disabled")),
        "…and so must the page, or a paused cell is still serving reads from \
         Postgres to whoever has the console open; got {code}: {v}"
    );
    Ok(())
}

// ===========================================================================
// runner
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn kv_console_routes() {
    let (host, port) = pg_target();
    let broker = BrokerProc::spawn(&host, port).await.expect("spawn broker");
    let h = broker.http.clone();
    let c = connect(&host, port).await;

    let mut report: Vec<(&str, Case)> = Vec::new();
    report.push((
        "the_two_routes_answer_their_envelopes",
        case_the_two_routes_answer_their_envelopes(&h, &c).await,
    ));
    report.push((
        "the_cursor_is_a_body_field_and_a_url_is_refused",
        case_the_cursor_is_a_body_field_and_a_url_is_refused(&h).await,
    ));
    report.push((
        "the_ladder_answers_on_both_routes",
        case_the_ladder_answers_on_both_routes(&h).await,
    ));

    println!("\n======= KV console routes (PLAN_DASHBOARD_ACTIONS §2.5) =======");
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
        "==================== {}/{} passed ====================\n",
        report.len() - failed,
        report.len()
    );
    assert_eq!(failed, 0, "{failed} KV console route case(s) failed — see the table above");
}
