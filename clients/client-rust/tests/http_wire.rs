//! What the client actually puts on the socket, and how it reacts to what
//! comes back.
//!
//! These run against [`support::FakeBroker`], not a real one, and so they need
//! no stack: `cargo test` on a laptop runs every one of them. That is the
//! point. The retry loop, the 429 budget, the failover walk and the `Host`
//! pinning are all decisions the client makes about responses a healthy broker
//! never sends, so a live-broker suite cannot reach them — and until this file
//! existed, nothing did.

mod support;

use std::time::Duration;

use queen_mq::{Config, Queen, Retry429, Strategy};
use support::{FakeBroker, Reply};

/// A pop response with one message, as the broker renders it.
fn one_message() -> String {
    r#"{"messages":[{"id":"m1","transactionId":"t1","data":{"n":1},
        "createdAt":"2026-08-04T10:00:00.000Z","partitionId":"p1",
        "partition":"one","leaseId":"L1","consumerGroup":"g"}]}"#
        .into()
}

fn client(broker: &FakeBroker) -> Queen {
    Queen::connect(Config::new(broker.url())).expect("fake broker url")
}

// ===========================================================================
// 429: the budget, and who is unbounded
// ===========================================================================

#[tokio::test]
async fn a_throttled_request_is_retried_and_then_succeeds() {
    let broker = FakeBroker::start(vec![
        Reply::status(429).header("Retry-After", "0"),
        Reply::ok(r#"{"messages":[]}"#),
    ])
    .await;

    let msgs = client(&broker)
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .expect("a 429 followed by a 200 must succeed");

    assert!(msgs.is_empty());
    assert_eq!(
        broker.hit_count(),
        2,
        "exactly one retry: {:?}",
        broker.hits().iter().map(|h| h.route()).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn retry_after_is_read_from_the_header() {
    // 400ms is long enough to tell from the jittered default base and short
    // enough not to slow the suite down.
    let broker = FakeBroker::start(vec![
        Reply::status(429).header("Retry-After", "0.4"),
        Reply::ok(r#"{"messages":[]}"#),
    ])
    .await;

    client(&broker)
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .unwrap();

    let gap = broker.gaps()[0];
    // The client jitters ±20% around the header value.
    assert!(
        gap >= Duration::from_millis(300) && gap <= Duration::from_millis(560),
        "Retry-After was not honoured: waited {gap:?}, expected ~400ms"
    );
}

#[tokio::test]
async fn a_push_gives_up_after_the_default_budget() {
    // Nothing but 429s. A push is bounded, so the client must stop asking and
    // surface the error rather than hammering a rate limiter forever.
    //
    // The bound is the PRODUCT of two loops, which is worth stating out loud:
    // `attempt_with_429` absorbs up to `max_attempts` (10 by default) throttles,
    // then gives up with a 429 error — which `attempt_with_retry` classifies as
    // retryable and runs again, `retry_attempts` (3) times over. So a push that
    // is throttled forever makes 30 requests, not 10, spread over the outer
    // loop's 1s and 2s backoffs.
    let broker = FakeBroker::start(vec![Reply::status(429).header("Retry-After", "0")]).await;

    let err = client(&broker)
        .queue("orders")
        .partition("one")
        .push(serde_json::json!({ "n": 1 }))
        .await
        .expect_err("an endless 429 must end as an error, not a hang");

    assert_eq!(
        broker.hit_count(),
        30,
        "the composed 429 budget for a push is retry_attempts(3) x max_attempts(10); \
         got {} — err: {err}",
        broker.hit_count()
    );
}

#[tokio::test]
async fn a_pop_keeps_waiting_out_a_throttle_past_the_push_budget() {
    // The asymmetry is deliberate and load-bearing: a consumer must not fall
    // out of its loop because a rate limiter had a bad minute, so `pop` is
    // unbounded unless the application sets a budget itself. Nothing else in
    // the suite pins it, and inverting the branch in `attempt_with_429` is a
    // silent failure — the consumer just stops.
    let broker = FakeBroker::start_with(|n, _| {
        if n < 14 {
            Reply::status(429).header("Retry-After", "0")
        } else {
            Reply::ok(r#"{"messages":[]}"#)
        }
    })
    .await;

    client(&broker)
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .expect("a default pop must ride out more throttles than a push");

    assert_eq!(
        broker.hit_count(),
        15,
        "the pop gave up at the push budget instead of waiting the throttle out"
    );
}

#[tokio::test]
async fn an_explicit_budget_binds_the_pop_as_well() {
    let broker = FakeBroker::start(vec![Reply::status(429).header("Retry-After", "0")]).await;
    let queen = Queen::connect(Config::new(broker.url()).retry_429(Retry429 {
        max_attempts: Some(3),
        base: Duration::from_millis(10),
        cap: Duration::from_millis(20),
    }))
    .unwrap();

    queen
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .expect_err("the explicit budget must run out");

    // Three throttles absorbed per outer attempt, three outer attempts: the
    // explicit budget binds the pop (an unbounded pop would never return), and
    // the outer retry loop multiplies it exactly as it does for a push.
    assert_eq!(
        broker.hit_count(),
        9,
        "an explicit max_attempts must bind pop too, not just push"
    );
}

#[tokio::test]
async fn the_backoff_grows_between_throttles() {
    // No Retry-After: the client falls back to its own exponential base.
    let broker = FakeBroker::start_with(|n, _| {
        if n < 3 {
            Reply::status(429)
        } else {
            Reply::ok(r#"{"messages":[]}"#)
        }
    })
    .await;
    let queen = Queen::connect(Config::new(broker.url()).retry_429(Retry429 {
        max_attempts: Some(6),
        base: Duration::from_millis(120),
        cap: Duration::from_secs(5),
    }))
    .unwrap();

    queen
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .unwrap();

    let gaps = broker.gaps();
    assert_eq!(gaps.len(), 3);
    assert!(
        gaps[2] > gaps[0],
        "the backoff did not grow across attempts: {gaps:?}"
    );
}

// ===========================================================================
// Terminal statuses: asking again is the bug
// ===========================================================================

#[tokio::test]
async fn a_forbidden_response_is_not_retried() {
    let broker = FakeBroker::start(vec![Reply::json(
        403,
        r#"{"error":"cluster suspended","code":"cluster_suspended"}"#,
    )])
    .await;

    let err = client(&broker)
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .expect_err("a 403 must surface");

    assert_eq!(
        broker.hit_count(),
        1,
        "a 403 was retried; a suspended tenant would be hammered: {err}"
    );
}

#[tokio::test]
async fn a_bad_request_is_not_retried() {
    let broker = FakeBroker::start(vec![Reply::json(400, r#"{"error":"bad body"}"#)]).await;

    client(&broker)
        .queue("orders")
        .partition("one")
        .push(serde_json::json!({ "n": 1 }))
        .await
        .expect_err("a 400 must surface");

    assert_eq!(broker.hit_count(), 1, "a malformed request was sent again");
}

#[tokio::test]
async fn an_unauthorized_response_surfaces_rather_than_looking_empty() {
    let broker = FakeBroker::start(vec![Reply::json(401, r#"{"error":"bad token"}"#)]).await;

    let err = client(&broker)
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .expect_err("an expired token must not read as an idle queue");

    assert!(
        format!("{err}").contains("401") || format!("{err}").to_lowercase().contains("token"),
        "the error says nothing useful: {err}"
    );
    assert_eq!(broker.hit_count(), 1);
}

// ===========================================================================
// Bodies that are not what the client hoped for
// ===========================================================================

#[tokio::test]
async fn an_html_error_page_still_produces_a_usable_error() {
    // What a 502 looks like behind nginx: the client tries to read the broker's
    // JSON error envelope, finds a web page, and must fall back to the status
    // rather than reporting a JSON parse failure.
    let broker = FakeBroker::start(vec![Reply::text(
        502,
        "<html><head><title>502 Bad Gateway</title></head><body>nginx</body></html>",
    )])
    .await;

    let err = client(&broker)
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .expect_err("a 502 must surface");

    let text = format!("{err}");
    assert!(
        !text.to_lowercase().contains("expected value"),
        "a serde parse error leaked to the caller instead of the status: {text}"
    );
}

#[tokio::test]
async fn an_empty_body_on_a_no_content_reply_is_not_an_error() {
    // The maintenance path answers 204 with no body at all.
    let broker = FakeBroker::start(vec![Reply::status(204)]).await;

    let msgs = client(&broker)
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .expect("a 204 is an empty pop, not a failure");
    assert!(msgs.is_empty());
}

// ===========================================================================
// What goes out on the wire
// ===========================================================================

#[tokio::test]
async fn the_bearer_token_is_attached_to_every_request() {
    // The only existing auth test runs against a broker with auth switched
    // off, so it passes just as happily if the header is dropped entirely.
    let broker = FakeBroker::start(vec![Reply::ok(r#"{"messages":[]}"#)]).await;
    let queen = Queen::connect(Config::new(broker.url()).bearer_token("tok-123")).unwrap();

    queen
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .unwrap();

    assert_eq!(
        broker.hits()[0].header("authorization"),
        Some("Bearer tok-123"),
        "no Authorization header reached the broker"
    );
}

#[tokio::test]
async fn a_custom_header_survives_to_the_socket() {
    let broker = FakeBroker::start(vec![Reply::ok(r#"{"messages":[]}"#)]).await;
    let queen = Queen::connect(Config::new(broker.url()).header("x-queen-tenant", "acme")).unwrap();

    queen
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .unwrap();

    assert_eq!(broker.hits()[0].header("x-queen-tenant"), Some("acme"));
}

#[tokio::test]
async fn the_host_header_is_the_configured_authority_not_the_address() {
    // `host_header` exists so a client can dial an IP while announcing the
    // virtual host a proxy routes on. Until now only the URL string was
    // asserted, never the header — and with `queen_proxy` in front, a Host
    // that silently reverts to the address does not fail, it lands in another
    // tenant's data.
    // curl --resolve semantics: the base URL is the address actually dialled,
    // `host_header` is the authority announced on the wire.
    let broker = FakeBroker::start(vec![Reply::ok(r#"{"messages":[]}"#)]).await;
    let authority = format!("acme.eu1.queenmq.cloud:{}", broker.port());
    let queen = Queen::connect(Config::new(broker.url()).host_header(&authority).unwrap()).unwrap();

    queen
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .expect("the request must still reach the dialled address");

    let host = broker.hits()[0].header("host").unwrap().to_string();
    assert_eq!(
        host, authority,
        "the socket went to the right place but announced the wrong Host, \
         which is how a request lands in another tenant's cluster"
    );
}

#[tokio::test]
async fn without_a_host_override_the_host_is_the_address() {
    let broker = FakeBroker::start(vec![Reply::ok(r#"{"messages":[]}"#)]).await;

    client(&broker)
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .unwrap();

    assert_eq!(
        broker.hits()[0].header("host"),
        Some(format!("127.0.0.1:{}", broker.port()).as_str())
    );
}

#[tokio::test]
async fn a_pop_sends_the_defaults_the_other_sdks_send() {
    let broker = FakeBroker::start(vec![Reply::ok(r#"{"messages":[]}"#)]).await;

    client(&broker)
        .queue("orders")
        .partition("one")
        .group("workers")
        .batch(7)
        .wait(false)
        .pop()
        .await
        .unwrap();

    let hit = &broker.hits()[0];
    assert_eq!(hit.method, "GET");
    // Queue and partition address the route; everything else is query string.
    assert_eq!(hit.route(), "/api/v1/pop/queue/orders/partition/one");
    assert_eq!(hit.query("batch"), Some("7"));
    assert_eq!(hit.query("wait"), Some("false"));
    assert_eq!(hit.query("consumerGroup"), Some("workers"));
}

// ===========================================================================
// Failover across backends
// ===========================================================================

#[tokio::test]
async fn a_server_error_moves_the_next_request_to_a_healthy_backend() {
    let sick = FakeBroker::start(vec![Reply::json(500, r#"{"error":"boom"}"#)]).await;
    let healthy = FakeBroker::start(vec![Reply::ok(r#"{"messages":[]}"#)]).await;

    let queen = Queen::connect(
        Config::urls([sick.url(), healthy.url()])
            .strategy(Strategy::Session)
            .failover(true),
    )
    .unwrap();

    queen
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .expect("the healthy backend should have answered");

    assert_eq!(sick.hit_count(), 1, "the sick backend was not tried once");
    assert_eq!(
        healthy.hit_count(),
        1,
        "the request never moved on to the healthy backend"
    );
}

#[tokio::test]
async fn every_backend_failing_reports_the_walk_rather_than_one_error() {
    let a = FakeBroker::start(vec![Reply::json(500, r#"{"error":"a"}"#)]).await;
    let b = FakeBroker::start(vec![Reply::json(503, r#"{"error":"b"}"#)]).await;

    let queen = Queen::connect(
        Config::urls([a.url(), b.url()])
            .strategy(Strategy::Session)
            .failover(true)
            .retry_attempts(1),
    )
    .unwrap();

    let err = queen
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .expect_err("both backends are down");

    assert!(
        a.hit_count() >= 1 && b.hit_count() >= 1,
        "one was never tried"
    );
    let text = format!("{err}");
    assert!(
        text.contains("backend") || text.contains("503") || text.contains("500"),
        "the error does not say the walk failed: {text}"
    );
}

#[tokio::test]
async fn a_client_error_does_not_take_a_backend_out_of_rotation() {
    // 4xx is the caller's fault, not the backend's. Marking the node unhealthy
    // on a 400 would drain a perfectly good broker out of the pool.
    let a = FakeBroker::start(vec![Reply::json(400, r#"{"error":"bad body"}"#)]).await;
    let b = FakeBroker::start(vec![Reply::ok(r#"{"messages":[]}"#)]).await;

    let queen = Queen::connect(
        Config::urls([a.url(), b.url()])
            .strategy(Strategy::Session)
            .failover(true),
    )
    .unwrap();

    queen
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .expect_err("a 400 is terminal");

    assert_eq!(
        b.hit_count(),
        0,
        "a 400 was treated as an unhealthy backend and failed over"
    );
}

// ===========================================================================
// Transport failures
// ===========================================================================

#[tokio::test]
async fn a_stalled_pop_ends_as_a_timeout_not_a_hang() {
    // A pop is governed by `poll_timeout`, NOT by `Config::timeout`: both
    // branches of `pop_with_auto_ack` override the per-request timeout, so the
    // client-wide one never applies here even with `wait(false)`. Anyone
    // reaching for `Config::timeout` to bound a pop gets the 30s default
    // instead, which is worth having pinned.
    let broker = FakeBroker::start(vec![Reply::hang()]).await;
    let queen = Queen::connect(
        Config::new(broker.url())
            .timeout(Duration::from_millis(400))
            .retry_attempts(1),
    )
    .unwrap();

    let started = std::time::Instant::now();
    let err = queen
        .queue("orders")
        .partition("one")
        .wait(false)
        .poll_timeout(Duration::from_millis(400))
        .pop()
        .await
        .expect_err("a server that never answers must time out");

    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the pop ignored poll_timeout: waited {:?}",
        started.elapsed()
    );
    let text = format!("{err}").to_lowercase();
    assert!(
        text.contains("time"),
        "a stall should be reported as a timeout, not as {text}"
    );
}

#[tokio::test]
async fn the_client_timeout_bounds_a_push() {
    // The counterpart: on every route that is not a pop, `Config::timeout` is
    // what applies.
    let broker = FakeBroker::start(vec![Reply::hang()]).await;
    let queen = Queen::connect(
        Config::new(broker.url())
            .timeout(Duration::from_millis(400))
            .retry_attempts(1),
    )
    .unwrap();

    let started = std::time::Instant::now();
    let err = queen
        .queue("orders")
        .partition("one")
        .push(serde_json::json!({ "n": 1 }))
        .await
        .expect_err("a stalled push must time out");

    assert!(
        started.elapsed() < Duration::from_secs(5),
        "Config::timeout did not bound the push: waited {:?}",
        started.elapsed()
    );
    assert!(
        format!("{err}").to_lowercase().contains("time"),
        "expected a timeout, got {err}"
    );
}

#[tokio::test]
async fn a_dropped_connection_is_a_network_error() {
    let broker = FakeBroker::start(vec![Reply::close()]).await;
    let queen = Queen::connect(Config::new(broker.url()).retry_attempts(1)).unwrap();

    let err = queen
        .queue("orders")
        .partition("one")
        .wait(false)
        .pop()
        .await
        .expect_err("a closed connection must surface");
    assert!(!format!("{err}").is_empty());
}

// ===========================================================================
// The consume loop's three ways out
// ===========================================================================

#[tokio::test]
async fn the_consume_loop_stops_dead_on_a_terminal_refusal() {
    // `is_terminal_refusal` exists so a suspended tenant stops the worker
    // instead of spinning. If that classification moved, the loop would retry
    // a 403 forever — one request per second, per worker, indefinitely.
    let broker = FakeBroker::start(vec![Reply::json(
        403,
        r#"{"error":"cluster suspended","code":"cluster_suspended"}"#,
    )])
    .await;

    let err = client(&broker)
        .queue("orders")
        .partition("one")
        .wait(false)
        .idle(Duration::from_secs(30))
        .consume(|_msg| async { Ok::<_, std::convert::Infallible>(()) })
        .await
        .expect_err("the consumer must stop on a terminal refusal");

    assert_eq!(
        broker.hit_count(),
        1,
        "the loop kept polling after a terminal refusal: {err}"
    );
}

#[tokio::test]
async fn the_consume_loop_backs_off_through_a_throttle_and_carries_on() {
    // 429 on the first poll, then work. The consumer must survive the throttle
    // and still deliver, without spinning: two polls, not two hundred.
    let broker = FakeBroker::start_with(|n, hit| {
        if hit.route().contains("/ack") {
            return Reply::ok(r#"[{"success":true}]"#);
        }
        match n {
            0 => Reply::status(429).header("Retry-After", "0"),
            1 => Reply::ok(one_message()),
            _ => Reply::ok(r#"{"messages":[]}"#),
        }
    })
    .await;

    let summary = client(&broker)
        .queue("orders")
        .partition("one")
        .wait(false)
        .limit(1)
        .idle(Duration::from_secs(20))
        .consume(|_msg| async { Ok::<_, std::convert::Infallible>(()) })
        .await
        .expect("a throttled poll must not kill the consumer");

    assert_eq!(summary.processed, 1);
}

// ===========================================================================
// Conflation — PLAN_CONFLATION §3.1 (the wire) and §4 (degrade loudly)
//
// The whole feature turns on one asymmetry: `conflation=true` is a query
// parameter, and an older broker DROPS unknown query parameters silently. So a
// 1.1.0 SDK against a 1.0.6 broker asks for last-value delivery, is told
// nothing, and quietly drains the entire backlog message by message — the exact
// failure the plan forbids. The only signal is the response echo, which is why
// these tests are here and not in the live-broker suite: a healthy broker of the
// right version can never produce the failing shape.
// ===========================================================================

/// A conflating claim as a 1.1.0 broker renders it: one message — the tail —
/// and the `conflation` echo that rides EVERY conflating response.
fn one_conflated_message() -> String {
    r#"{"messages":[{"id":"m1","transactionId":"t1","data":{"n":1},
        "createdAt":"2026-08-04T10:00:00.000Z","partitionId":"p1",
        "partition":"one","leaseId":"L1","consumerGroup":"workers"}],
        "partitionsClaimed":1,"conflation":true}"#
        .into()
}

/// An EMPTY conflating pop. A conflating broker answers 200 with a body rather
/// than a bodiless 204 precisely so the echo reaches the SDK on the first round
/// trip, before a single message is processed.
fn empty_conflated() -> String {
    r#"{"messages":[],"partitionsClaimed":0,"conflation":true}"#.into()
}

#[tokio::test]
async fn a_conflating_pop_declares_itself_on_the_wire() {
    let broker = FakeBroker::start(vec![Reply::ok(empty_conflated())]).await;

    client(&broker)
        .queue("orders")
        .group("workers")
        .conflation(true)
        .wait(false)
        .pop()
        .await
        .expect("the broker applied conflation, so the pop succeeds");

    let hit = &broker.hits()[0];
    assert_eq!(hit.query("conflation"), Some("true"), "{}", hit.path);
    assert_eq!(hit.query("consumerGroup"), Some("workers"));
}

// The plan's standing hazard (§4): in every SDK but this one the pop() and
// consume() parameter builders are SEPARATE code, and an option wired into one
// and not the other is the bug that comment at QueueBuilder.js:395 was left
// behind by. Rust shares `pop_params`, so this test exists to keep that true —
// if consume ever grows its own builder, this goes red.
#[tokio::test]
async fn a_conflating_consume_declares_itself_on_the_wire() {
    let broker = FakeBroker::start_with(|_n, hit| {
        if hit.route().contains("/ack") {
            return Reply::ok(r#"[{"success":true}]"#);
        }
        Reply::ok(one_conflated_message())
    })
    .await;

    let summary = client(&broker)
        .queue("orders")
        .group("workers")
        .conflation(true)
        .wait(false)
        .limit(1)
        .idle(Duration::from_secs(20))
        .consume(|_msg| async { Ok::<_, std::convert::Infallible>(()) })
        .await
        .expect("a conflating consume against a 1.1.0 broker runs normally");

    assert_eq!(summary.processed, 1);
    let pop = broker
        .hits()
        .into_iter()
        .find(|h| h.route().starts_with("/api/v1/pop"))
        .expect("the consumer polled");
    assert_eq!(pop.query("conflation"), Some("true"), "{}", pop.path);
}

// Default off, and `conflation(false)` is the ABSENCE of the key rather than
// `conflation=false`: sending false would read to the broker as a disagreement
// with a group whose stored policy is true, and would earn a conflict counter
// for a consumer that never asked for anything.
#[tokio::test]
async fn conflation_off_leaves_the_query_byte_identical() {
    let broker = FakeBroker::start(vec![Reply::ok(r#"{"messages":[]}"#)]).await;
    let q = client(&broker);

    q.queue("orders")
        .group("workers")
        .wait(false)
        .pop()
        .await
        .unwrap();
    q.queue("orders")
        .group("workers")
        .conflation(false)
        .wait(false)
        .pop()
        .await
        .unwrap();

    for hit in broker.hits() {
        assert_eq!(hit.query("conflation"), None, "{}", hit.path);
    }
}

#[tokio::test]
async fn a_broker_that_ignored_conflation_fails_the_pop_loudly() {
    // 1.0.6 to the byte: the unknown query parameter is dropped, the whole
    // backlog comes back, and nothing in the body mentions conflation.
    let broker = FakeBroker::start(vec![Reply::ok(one_message())]).await;

    let err = client(&broker)
        .queue("orders")
        .group("workers")
        .conflation(true)
        .wait(false)
        .pop()
        .await
        .expect_err(
            "a pop that asked for conflation and was silently given a full batch must \
             not return that batch as if it were the tail",
        );

    let text = err.to_string();
    assert!(
        text.contains("conflation was requested but this broker did not apply it"),
        "the error must say what happened: {text}"
    );
    assert!(
        text.contains("1.1.0"),
        "the error must name the version that fixes it: {text}"
    );
}

// The check has to fire on the FIRST round trip, and the first round trip on a
// quiet queue is an empty one. A 1.1.0 broker answers a conflating empty pop
// with 200 + `"conflation":true`; a 1.0.6 broker answers 204 with no body at
// all. Without this branch an idle consumer would sit there for hours looking
// healthy and degrade the moment traffic arrived.
#[tokio::test]
async fn an_old_brokers_bodiless_204_is_caught_on_the_first_round_trip() {
    let broker = FakeBroker::start(vec![Reply::status(204)]).await;

    let err = client(&broker)
        .queue("orders")
        .group("workers")
        .conflation(true)
        .wait(false)
        .pop()
        .await
        .expect_err("a bodiless 204 cannot carry the echo, so it is an old broker");

    assert!(
        err.to_string().contains("requires broker >= 1.1.0"),
        "{err}"
    );

    // ...and the same reply is still a perfectly ordinary empty poll when
    // conflation was never asked for.
    let ok = client(&broker)
        .queue("orders")
        .group("workers")
        .wait(false)
        .pop()
        .await
        .expect("204 is an empty poll for everyone else");
    assert!(ok.is_empty());
}

#[tokio::test]
async fn a_conflating_consumer_stops_instead_of_draining_the_backlog() {
    // The failure this test exists for: the handler runs 10 000 times on a
    // backlog the consumer asked to skip. It must run zero times.
    let broker = FakeBroker::start(vec![Reply::ok(one_message())]).await;

    // Bounded, because the failure mode is not a wrong answer but an endless
    // one: without the check this loop pops, handles, acks and pops again for
    // as long as the broker has backlog to hand it.
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        client(&broker)
            .queue("orders")
            .group("workers")
            .conflation(true)
            .wait(false)
            .idle(Duration::from_secs(30))
            .consume(|_msg| async { Ok::<_, std::convert::Infallible>(()) }),
    )
    .await
    .expect("the consumer never stopped: it is draining the backlog it asked to skip")
    .expect_err("the consume loop must stop, not process the backlog");

    assert!(err.to_string().contains("1.1.0"), "{err}");
    let routes: Vec<String> = broker
        .hits()
        .iter()
        .map(|h| h.route().to_string())
        .collect();
    assert!(
        !routes.iter().any(|r| r.contains("/ack")),
        "a message was handled and acked before the version check fired: {routes:?}"
    );
    assert_eq!(
        broker.hit_count(),
        1,
        "the loop kept polling after the degrade error: {routes:?}"
    );
}

// §3.3: the stored group policy wins and BOTH consumers keep working. A
// conflict is a rolling-deploy artefact, not an outage — rejecting it would
// take down the half of the fleet that is already correct.
#[tokio::test]
async fn a_declaration_conflict_does_not_stop_the_consumer() {
    let broker = FakeBroker::start_with(|_n, hit| {
        if hit.route().contains("/ack") {
            return Reply::ok(r#"[{"success":true}]"#);
        }
        Reply::ok(
            r#"{"messages":[{"id":"m1","transactionId":"t1","data":{"n":1},
                "createdAt":"2026-08-04T10:00:00.000Z","partitionId":"p1",
                "partition":"one","leaseId":"L1","consumerGroup":"workers"}],
                "conflation":true,"conflationConflict":true}"#,
        )
    })
    .await;

    let summary = client(&broker)
        .queue("orders")
        .group("workers")
        .conflation(true)
        .wait(false)
        .limit(2)
        .idle(Duration::from_secs(20))
        .consume(|_msg| async { Ok::<_, std::convert::Infallible>(()) })
        .await
        .expect("a conflict warns; it does not fail the consumer");

    assert_eq!(summary.processed, 2);
}

// §5.3: depth is where a conflating queue stops looking like an incident.
// `pending` is log depth and `effectivePending` is work depth, and this client
// returns the broker's JSON verbatim — so the only way these fields can go
// missing is somebody typing the response. Pinned.
#[tokio::test]
async fn queue_depth_carries_the_conflation_fields() {
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"queue":"orders","group":"workers","pending":4000000,
            "partitionsPending":12,"conflation":true,"effectivePending":12,
            "partitions":[{"partition":"one","pending":4000000}]}"#,
    )])
    .await;

    let depth = client(&broker)
        .admin()
        .queue_depth("orders", Some("workers"))
        .await
        .expect("depth is a plain GET");

    let hit = &broker.hits()[0];
    assert_eq!(hit.route(), "/api/v1/resources/queues/orders/depth");
    assert_eq!(hit.query("group"), Some("workers"));

    assert_eq!(depth["pending"], serde_json::json!(4_000_000i64));
    assert_eq!(depth["partitionsPending"], serde_json::json!(12));
    assert_eq!(depth["conflation"], serde_json::json!(true));
    assert_eq!(
        depth["effectivePending"],
        serde_json::json!(12),
        "4 000 000 pending with 12 handler invocations left is a HEALTHY conflating \
         queue; dropping effectivePending turns it back into an incident"
    );
}

// The same trap end to end. A group registered before anyone wanted conflation
// stores `false`; §3.3 gives the stored value the last word, so this consumer is
// answered "no" — the conflict key with no `conflation` echo, which is what an
// old broker's response ALSO lacks. It must keep consuming: E2E-4's whole point
// is that a rolling deploy where half the fleet declares the flag does not take
// the other half down.
#[tokio::test]
async fn a_group_that_stores_conflation_off_keeps_the_consumer_running() {
    let broker = FakeBroker::start_with(|_n, hit| {
        if hit.route().contains("/ack") {
            return Reply::ok(r#"[{"success":true}]"#);
        }
        Reply::ok(
            r#"{"messages":[{"id":"m1","transactionId":"t1","data":{"n":1},
                "createdAt":"2026-08-04T10:00:00.000Z","partitionId":"p1",
                "partition":"one","leaseId":"L1","consumerGroup":"workers"}],
                "conflationConflict":true}"#,
        )
    })
    .await;

    let summary = tokio::time::timeout(
        Duration::from_secs(10),
        client(&broker)
            .queue("orders")
            .group("workers")
            .conflation(true)
            .wait(false)
            .limit(2)
            .idle(Duration::from_secs(20))
            .consume(|_msg| async { Ok::<_, std::convert::Infallible>(()) }),
    )
    .await
    .expect("the consumer stopped responding")
    .expect(
        "the group's stored policy said no; that is a conflict to warn about, not a \
         broker version to fail on",
    );

    assert_eq!(summary.processed, 2);
}
