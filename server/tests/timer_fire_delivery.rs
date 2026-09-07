//! A timer reaches its consumer when it fires, not a reseed later.
//!
//! Two gaps, each worth up to 30 s with default settings, made a 2000 ms timer
//! arrive after about 30 s on an idle broker — through the transaction rider and
//! through `POST /api/v1/timers` alike (measured 2026-09-07 on 1.4.0; present from
//! 1.0.3 through 1.5.1):
//!
//!   A. the sweeper's fire never announced its commit. Every other path that lands
//!      frames marks the hot list and wakes parked pops after its commit; the fire
//!      only counted its verdicts. A fired frame therefore sat in the log with no
//!      group ring aware of its partition, and the queue-scoped pop routes — which
//!      consult their rings, not the log — did not consider it until the periodic
//!      reseed (`QUEEN_HOTLIST_RESEED_MS`, 30 s). A parked long-poll was woken
//!      only by that reseed's promotion.
//!   B. the sweeper's wake hint had no caller. The waker existed and the sleep loop
//!      listened, but neither the timers handler nor the transaction wire rang it,
//!      so a timer scheduled while the table had been empty waited out the idle
//!      backoff, up to `QUEEN_SWEEPER_IDLE_MAX_SLEEP_MS` (30 s), whatever its delay.
//!
//! WHY THIS LEVEL — the real binary against a real Postgres, as in
//! `conflation_semantics`. The embedded `queen::Broker` runs no sweeper, so nothing
//! below the HTTP wire can exercise a fire, and gap B is a property of the sweeper's
//! sleep loop. Default sleeps and the default reseed interval, on purpose: shortening
//! either is the workaround, not the fix, and this test has to fail on a broker that
//! has only the workaround. Run against the 1.5.1 binary (2026-09-07, this machine)
//! it reports the pinned control at 12.9 s — the fire waited out the idle sleep —
//! and both queue-scoped pops at 29.7 s, the reseed on top; against the fix, all
//! three at about 2.07 s.
//!
//! The pinned single-partition route is the CONTROL. It reads the partition's log
//! directly, ring or no ring, so it says whether the fire itself was on time and
//! separates the two gaps when one of them regresses: pinned late and queue-scoped
//! late is B (or both); pinned on time and queue-scoped late is A alone.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`:
//!
//! ```bash
//! docker run --rm -d --name queen-timerfire-pg -e POSTGRES_PASSWORD=postgres -p 5481:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5481 cargo test --test timer_fire_delivery -- --ignored --nocapture
//! ```
//!
//! To run it against ANOTHER build of the broker — the control that shows the test
//! goes red without the fix, or a bisect — point `QUEEN_TIMER_TEST_BIN` at that
//! binary; by default it drives the one cargo just built.
//!
//! Its own test binary (and so its own process) because the spawned broker is a
//! process global here, as it is in `conflation_semantics`.

use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The bound a delivered timer has to meet, measured from the schedule call. The
/// fix delivers a 2000 ms timer in about 2.1 s: two 1 s sleeps to reach the due
/// instant, a fire transaction, a wake tick (5 ms) and one targeted pop. Either
/// gap alone pushes that past 10 s in the arrangement below, so the bound has room
/// for a slow machine and none for a regression.
const DELAY_MS: i64 = 2_000;
const BOUND: Duration = Duration::from_millis(3_500);

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

fn pg_target() -> (String, u16) {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port) for the timer delivery test");
    target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432))
}

// ======================================================================
// the broker under test: the REAL binary, DEFAULT sleeps, DEFAULT reseed
// ======================================================================

struct BrokerProc {
    #[allow(dead_code)] // kill_on_drop is the point of holding it
    child: tokio::process::Child,
    http: Http,
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
            "queen-timerfire-broker-{}-{}.log",
            std::process::id(),
            port
        ));
        let log = std::fs::File::create(&log_path)
            .map_err(|e| format!("create broker log {}: {e}", log_path.display()))?;
        let log2 = log.try_clone().map_err(|e| format!("clone log: {e}"))?;
        let bin = std::env::var("QUEEN_TIMER_TEST_BIN")
            .unwrap_or_else(|_| env!("CARGO_BIN_EXE_queen").to_string());
        println!("broker binary: {bin}");
        let child = tokio::process::Command::new(bin)
            .env("PG_HOST", pg_host)
            .env("PG_PORT", pg_port.to_string())
            .env("PG_USER", "postgres")
            .env("PG_PASSWORD", "postgres")
            .env("PG_DATABASE", "postgres")
            .env("PORT", port.to_string())
            .env("DB_POOL_SIZE", "16")
            // `info`, so the log names every fire ("swept") for a post-mortem.
            .env("LOG_LEVEL", "info")
            // Deliberately NOT set: QUEEN_SWEEPER_IDLE_MAX_SLEEP_MS,
            // QUEEN_SWEEPER_MAX_SLEEP_MS, QUEEN_HOTLIST_RESEED_MS. Their defaults
            // are what the test is about.
            .stdout(std::process::Stdio::from(log))
            .stderr(std::process::Stdio::from(log2))
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("spawn queen binary: {e}"))?;
        let http = Http { addr: format!("127.0.0.1:{port}") };
        wait_health(&http, 40, &log_path).await?;
        Ok(BrokerProc { child, http, log_path })
    }
}

async fn wait_health(http: &Http, secs: u64, log_path: &std::path::Path) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut last = String::from("<no response yet>");
    while Instant::now() < deadline {
        match http.req("GET", "/health", None, Duration::from_secs(5)).await {
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
    async fn req(
        &self,
        method: &str,
        path_query: &str,
        body: Option<&Value>,
        timeout: Duration,
    ) -> Result<(u16, Value), String> {
        let fut = self.req_inner(method, path_query, body);
        tokio::time::timeout(timeout, fut)
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
        .any(|(k, v)| {
            k.trim().eq_ignore_ascii_case("transfer-encoding")
                && v.to_ascii_lowercase().contains("chunked")
        });
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
        i += size + 2;
    }
}

// ======================================================================
// the two schedule seams, and the pops
// ======================================================================

const GROUP: &str = "g";
/// base64 of `{"n":1}` — the payload column is BYTEA, so the wire carries it encoded.
const PAYLOAD_B64: &str = "eyJuIjoxfQ==";

fn schedule_op(queue: &str) -> Value {
    json!({
        "op": "schedule",
        "queue": queue,
        "timerKey": "k",
        "partition": "Default",
        "delayMs": DELAY_MS,
        "txn": unique("txn"),
        "payload": PAYLOAD_B64,
    })
}

/// `POST /api/v1/timers`, the standalone route.
async fn schedule_via_route(h: &Http, queue: &str) -> Result<(), String> {
    let (code, v) = h
        .req("POST", "/api/v1/timers", Some(&json!([schedule_op(queue)])), Duration::from_secs(10))
        .await?;
    if code != 200 {
        return Err(format!("POST /api/v1/timers -> {code}: {v}"));
    }
    let ok = v["results"][0]["ok"].as_bool().unwrap_or(false);
    if !ok {
        return Err(format!("schedule refused: {v}"));
    }
    Ok(())
}

/// The transaction wire with a timers rider and no operations at all: the
/// riders-only bundle the wire accepts.
async fn schedule_via_wire(h: &Http, queue: &str) -> Result<(), String> {
    let (code, v) = h
        .req(
            "POST",
            "/api/v1/transaction",
            Some(&json!({"operations": [], "timers": [schedule_op(queue)]})),
            Duration::from_secs(10),
        )
        .await?;
    if code != 200 || v["success"].as_bool() != Some(true) {
        return Err(format!("POST /api/v1/transaction -> {code}: {v}"));
    }
    Ok(())
}

fn pop_qs(wait: bool, timeout_ms: u64) -> String {
    format!("consumerGroup={GROUP}&subscriptionMode=all&batch=10&wait={wait}&timeout={timeout_ms}")
}

/// One empty, non-waiting pop: creates the queue and registers the group so its
/// ring exists. A group's first contact SEEDS its ring from the log; a ring that
/// did not exist at fire time would be seeded on first contact rather than marked,
/// and the test would be measuring the seed, not the announce.
async fn first_contact(h: &Http, queue: &str) -> Result<(), String> {
    let (code, v) = h
        .req("GET", &format!("/api/v1/pop/queue/{queue}?{}", pop_qs(false, 0)), None, Duration::from_secs(10))
        .await?;
    if code != 200 && code != 204 {
        return Err(format!("first contact pop {queue} -> {code}: {v}"));
    }
    if !msgs(&v).is_empty() {
        return Err(format!("a fresh queue delivered something: {v}"));
    }
    Ok(())
}

fn msgs(v: &Value) -> Vec<Value> {
    v.get("messages").and_then(|m| m.as_array()).cloned().unwrap_or_default()
}

/// A long-poll parked on the queue (`partition` = None: the queue-scoped route,
/// the one gap A blinds) or on one partition (the pinned route, the control).
/// Resolves with the instant the response arrived and the messages it carried.
async fn parked_pop(h: Http, queue: String, partition: Option<&'static str>) -> Result<(Instant, Vec<Value>), String> {
    let path = match partition {
        Some(p) => format!("/api/v1/pop/queue/{queue}/partition/{p}?{}", pop_qs(true, 30_000)),
        None => format!("/api/v1/pop/queue/{queue}?{}", pop_qs(true, 30_000)),
    };
    let (code, v) = h.req("GET", &path, None, Duration::from_secs(45)).await?;
    if code != 200 && code != 204 {
        return Err(format!("parked pop {path} -> {code}: {v}"));
    }
    Ok((Instant::now(), msgs(&v)))
}

fn check(name: &str, t0: Instant, got: Result<(Instant, Vec<Value>), String>) -> Result<(), String> {
    let (at, messages) = got?;
    let took = at.saturating_duration_since(t0);
    if messages.is_empty() {
        return Err(format!(
            "{name}: the parked pop returned empty after {took:?} — the timer was not \
             delivered within the pop's 30 s deadline"
        ));
    }
    let n = messages[0]["data"]["n"].as_i64();
    if n != Some(1) {
        return Err(format!("{name}: delivered something else: {}", messages[0]));
    }
    if took > BOUND {
        return Err(format!(
            "{name}: a {DELAY_MS} ms timer was delivered after {took:?}, bound {BOUND:?}"
        ));
    }
    println!("  {name}: {DELAY_MS} ms timer delivered after {took:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn a_fired_timer_reaches_a_parked_consumer_in_the_same_cycle() {
    let (pg_host, pg_port) = pg_target();
    let broker = BrokerProc::spawn(&pg_host, pg_port).await.expect("broker");
    let h = broker.http.clone();

    // Let the sweeper's idle backoff engage. With the defaults — 1 s ceiling,
    // 30 s idle cap, the doubling starting after five idle cycles — an empty
    // table sleeps 1 s six times, then 2, 4, 8, 16 and 30 s: the loop enters its
    // 16 s sleep about 19 s after boot and its 30 s sleep about 35 s after. At
    // 22 s it is inside the 16 s sleep, so without gap B's ring a timer scheduled
    // now fires about 13 s late, whatever its delay. (The kv prune and the usage
    // rollup move no rows on an empty database, so they do not reset the count.)
    println!("broker up on {}; log {}", h.addr, broker.log_path.display());
    println!("waiting 22 s for the sweeper's idle backoff to engage");
    tokio::time::sleep(Duration::from_secs(22)).await;

    // Three queues, three consumers, all parked BEFORE the schedules so a WAKE,
    // not a re-poll, is what delivers. The queue-scoped pops are the two seams
    // under test; the pinned pop is the control.
    let via_route = unique("tfd-route");
    let via_wire = unique("tfd-wire");
    let via_pinned = unique("tfd-pinned");
    for q in [&via_route, &via_wire, &via_pinned] {
        first_contact(&h, q).await.expect("first contact");
    }
    let parked_route = tokio::spawn(parked_pop(h.clone(), via_route.clone(), None));
    let parked_wire = tokio::spawn(parked_pop(h.clone(), via_wire.clone(), None));
    let parked_pinned = tokio::spawn(parked_pop(h.clone(), via_pinned.clone(), Some("Default")));
    tokio::time::sleep(Duration::from_millis(300)).await; // let the three park

    let t0 = Instant::now();
    schedule_via_route(&h, &via_route).await.expect("schedule via POST /api/v1/timers");
    schedule_via_wire(&h, &via_wire).await.expect("schedule via the transaction wire");
    schedule_via_route(&h, &via_pinned).await.expect("schedule for the pinned control");

    let (r, w, p) = tokio::join!(parked_route, parked_wire, parked_pinned);
    let cases = [
        ("pinned partition route (control: the fire was on time)", p),
        ("queue-scoped pop, scheduled via POST /api/v1/timers", r),
        ("queue-scoped pop, scheduled via the transaction wire", w),
    ];
    let mut failed = Vec::new();
    for (name, joined) in cases {
        let got = joined.map_err(|e| format!("join: {e}"))
            .and_then(|r| r);
        match check(name, t0, got) {
            Ok(()) => {}
            Err(e) => {
                println!("  FAIL {e}");
                failed.push(e);
            }
        }
    }
    assert!(
        failed.is_empty(),
        "{} of 3 deliveries missed the bound; broker log: {}\n{}",
        failed.len(),
        broker.log_path.display(),
        failed.join("\n")
    );
}
