//! What the KV and timer surfaces actually put on the socket.
//!
//! These run against [`support::FakeBroker`] and need no stack, which is the
//! point: the body is the contract with the broker, and a wrong field name is
//! not an error anywhere — the stored procedures read JSONB key by key, so a key
//! they cannot find is a default or a `RAISE`, never a complaint that the client
//! sent the wrong thing. `ttlSecs` instead of `ttlSeconds` is a 400 nobody can
//! diagnose from the client side; `delaySeconds` instead of `delayMs` is a
//! server-owned field and a different 400; `kv` inside `operations` instead of
//! beside it is a transaction that commits with its gate silently missing.
//!
//! So every test below asserts the **exact bytes**, not a shape.
//!
//! The other half of the file is the status-code rule (PLAN_KV_TIMERS.md §8.1):
//! `applied:false`, `found:false`, `absent` and `too_late` are HTTP 200
//! verdicts, and none of them may reach a caller as an error or a retry.

mod support;

use std::time::Duration;

use queen_mq::{Config, Expiry, KvOperation, KvReason, Queen, TimerOperation, TimerStatus};
use support::{FakeBroker, Reply};

fn client(broker: &FakeBroker) -> Queen {
    Queen::connect(Config::new(broker.url())).expect("fake broker url")
}

/// One KV result element, as the path routes answer.
fn kv_element(body: &str) -> Reply {
    Reply::ok(body)
}

/// The batch envelope.
fn kv_results(elements: &str) -> Reply {
    Reply::ok(format!(r#"{{"results":[{elements}]}}"#))
}

// ===========================================================================
// KV: the exact body of every operation
// ===========================================================================

#[tokio::test]
async fn a_put_uses_the_path_route_and_names_its_ttl_the_way_the_broker_reads_it() {
    let broker = FakeBroker::start(vec![kv_element(
        r#"{"index":0,"op":"put","applied":true,"key":"idem:9137","value":true,"version":1}"#,
    )])
    .await;

    let res = client(&broker)
        .kv()
        .put(
            "orders",
            "idem:9137",
            serde_json::json!(true),
            Expiry::seconds(86_400),
        )
        .send()
        .await
        .expect("a put must not fail");
    assert!(res.applied());

    let hit = &broker.hits()[0];
    assert_eq!(hit.method, "PUT");
    // The key is percent-encoded whole. `:` and `/` both survive: the broker
    // decodes the catch-all once, so `a%2Fb` and `a/b` address the same key.
    assert_eq!(hit.path, "/api/v1/kv/orders/idem%3A9137");
    assert_eq!(
        hit.body, r#"{"value":true,"ttlSeconds":86400}"#,
        "the path route names ns and key in the URL, and a body that repeated \
         them would be a 400 rather than an ignored field"
    );
}

#[tokio::test]
async fn a_forever_put_declares_forever_and_no_ttl() {
    // Exactly one of the two, ever. Both is the same error as neither.
    let broker = FakeBroker::start(vec![kv_element(
        r#"{"index":0,"op":"put","applied":true,"key":"k","version":1}"#,
    )])
    .await;

    client(&broker)
        .kv()
        .put("ns", "k", serde_json::json!({"a": 1}), Expiry::Forever)
        .send()
        .await
        .unwrap();

    assert_eq!(broker.hits()[0].body, r#"{"value":{"a":1},"forever":true}"#);
}

#[tokio::test]
async fn a_put_if_absent_goes_through_the_batch_route_under_its_own_name() {
    // There is no path route for it, and there must never be: any literal
    // segment under /api/v1/kv/:ns/ makes a key of that name unreachable.
    let broker = FakeBroker::start(vec![kv_results(
        r#"{"index":0,"op":"put","applied":true,"key":"idem:9137","value":true,"version":1}"#,
    )])
    .await;

    client(&broker)
        .kv()
        .put_if_absent(
            "orders",
            "idem:9137",
            serde_json::json!(true),
            Expiry::seconds(86_400),
        )
        .send()
        .await
        .unwrap();

    let hit = &broker.hits()[0];
    assert_eq!(hit.method, "POST");
    assert_eq!(hit.path, "/api/v1/kv");
    assert_eq!(
        hit.body,
        r#"{"operations":[{"op":"putIfAbsent","ns":"orders","key":"idem:9137","value":true,"ttlSeconds":86400}]}"#,
        "no expect of its own: the alias IS expect:0, and a different one is a \
         contradiction the broker refuses"
    );
}

#[tokio::test]
async fn a_get_sends_no_body_and_no_query_string() {
    // The query string is the point. A prefix in a URL is recorded by the
    // broker's access log, the proxy's, the meter sample and any ingress in
    // front, so this route rejects any query outright — and the client must
    // never give it one.
    let broker = FakeBroker::start(vec![kv_element(
        r#"{"index":0,"op":"get","found":true,"key":"saga:1","value":{"step":2},"version":7,
            "expiresAt":"2026-08-18T10:00:00Z","updatedAt":"2026-08-17T10:00:00Z"}"#,
    )])
    .await;

    let res = client(&broker).kv().get("orders", "saga:1").await.unwrap();
    assert!(res.found());
    assert_eq!(res.version, Some(7));

    let hit = &broker.hits()[0];
    assert_eq!(hit.method, "GET");
    assert_eq!(hit.path, "/api/v1/kv/orders/saga%3A1");
    assert_eq!(hit.body, "");
}

#[tokio::test]
async fn a_key_with_slashes_stays_one_key() {
    // `order/9f1/items` is a normal key. Percent-encoding it whole addresses
    // the same row the raw form would, because the catch-all is decoded once —
    // and encoding is the only form that is also safe for a key containing a
    // literal slash.
    let broker = FakeBroker::start(vec![kv_element(
        r#"{"index":0,"op":"get","found":false,"key":"x"}"#,
    )])
    .await;

    client(&broker)
        .kv()
        .get("orders", "order/9f1/items")
        .await
        .unwrap();

    assert_eq!(
        broker.hits()[0].path,
        "/api/v1/kv/orders/order%2F9f1%2Fitems"
    );
}

#[tokio::test]
async fn a_delete_carries_its_fence_and_nothing_else() {
    let broker = FakeBroker::start(vec![kv_element(
        r#"{"index":0,"op":"delete","applied":false,"reason":"version","key":"k","version":9}"#,
    )])
    .await;

    let res = client(&broker)
        .kv()
        .delete("orders", "k")
        .expect(7)
        .send()
        .await
        .expect("a fenced delete that loses is a verdict, not an error");
    assert!(!res.applied());
    assert_eq!(res.reason, Some(KvReason::Version));

    let hit = &broker.hits()[0];
    assert_eq!(hit.method, "DELETE");
    assert_eq!(hit.path, "/api/v1/kv/orders/k");
    assert_eq!(hit.body, r#"{"expect":7}"#);
}

#[tokio::test]
async fn an_unfenced_delete_sends_an_empty_object() {
    let broker = FakeBroker::start(vec![kv_element(
        r#"{"index":0,"op":"delete","applied":true,"key":"k","version":0}"#,
    )])
    .await;

    client(&broker).kv().delete("ns", "k").send().await.unwrap();
    assert_eq!(
        broker.hits()[0].body,
        "{}",
        "no expect means no precondition, not expect:0 — which would mean \
         `must not exist`"
    );
}

#[tokio::test]
async fn an_incr_sends_its_bounds_and_never_an_expect() {
    let broker = FakeBroker::start(vec![kv_results(
        r#"{"index":0,"op":"incr","applied":true,"key":"acme:minute","value":1,"version":1}"#,
    )])
    .await;

    let res = client(&broker)
        .kv()
        .incr("quota", "acme:minute", 1, Expiry::seconds(60))
        .max(100)
        .send()
        .await
        .unwrap();
    assert_eq!(res.counter().unwrap(), 1);

    assert_eq!(
        broker.hits()[0].body,
        r#"{"operations":[{"op":"incr","ns":"quota","key":"acme:minute","delta":1,"max":100,"ttlSeconds":60}]}"#
    );
}

#[tokio::test]
async fn a_refused_increment_reports_the_current_value_and_is_not_an_error() {
    // With `max`, applied IS the admission decision. The request that would
    // have broken the budget must not have spent it first.
    let broker = FakeBroker::start(vec![kv_results(
        r#"{"index":0,"op":"incr","applied":false,"reason":"limit","key":"acme:minute",
            "value":100,"version":42}"#,
    )])
    .await;

    let res = client(&broker)
        .kv()
        .incr("quota", "acme:minute", 1, Expiry::seconds(60))
        .max(100)
        .send()
        .await
        .expect("a refused increment is a verdict");
    assert!(!res.applied());
    assert_eq!(res.reason, Some(KvReason::Limit));
    assert_eq!(res.counter().unwrap(), 100);
    assert_eq!(broker.hit_count(), 1, "a verdict must never be retried");
}

#[tokio::test]
async fn a_get_many_asks_for_its_keys_and_reads_absence_as_a_list() {
    let broker = FakeBroker::start(vec![kv_results(
        r#"{"index":0,"op":"getMany","rows":[{"key":"a","value":1,"version":2}],
            "missing":["b"],"truncated":false}"#,
    )])
    .await;

    let res = client(&broker)
        .kv()
        .get_many("orders", vec!["a".into(), "b".into()])
        .await
        .unwrap();
    assert_eq!(res.rows().len(), 1);
    assert_eq!(res.missing(), ["b".to_string()]);

    assert_eq!(
        broker.hits()[0].body,
        r#"{"operations":[{"op":"getMany","ns":"orders","keys":["a","b"]}]}"#
    );
}

#[tokio::test]
async fn a_prefix_scan_lives_in_the_body_and_never_in_the_url() {
    let broker = FakeBroker::start(vec![kv_results(
        r#"{"index":0,"op":"getPrefix","rows":[{"key":"saga:1","version":1}],
            "truncated":true,"nextAfter":"saga:1"}"#,
    )])
    .await;

    let page = client(&broker)
        .kv()
        .get_prefix("orders", "saga:")
        .after("saga:0")
        .limit(50)
        .keys_only()
        .send()
        .await
        .unwrap();
    assert!(page.truncated());
    assert_eq!(page.next_after.as_deref(), Some("saga:1"));

    let hit = &broker.hits()[0];
    assert_eq!(hit.path, "/api/v1/kv", "no query string, ever");
    assert!(
        !hit.path.contains("saga"),
        "the prefix must not reach a URL"
    );
    assert_eq!(
        hit.body,
        r#"{"operations":[{"op":"getPrefix","ns":"orders","prefix":"saga:","after":"saga:0","limit":50,"keysOnly":true}]}"#
    );
}

#[tokio::test]
async fn a_lost_race_is_returned_with_the_winners_value_rather_than_raised() {
    // The most frequent outcome of this feature. A 4xx here would put it inside
    // seven clients' retry policies and error dashboards.
    let broker = FakeBroker::start(vec![kv_results(
        r#"{"index":0,"op":"put","applied":false,"reason":"exists","key":"idem:9137",
            "value":{"by":"worker-2"},"version":90101}"#,
    )])
    .await;

    let res = client(&broker)
        .kv()
        .put_if_absent(
            "orders",
            "idem:9137",
            serde_json::json!(true),
            Expiry::seconds(60),
        )
        .send()
        .await
        .expect("losing a putIfAbsent is not an error");
    assert!(!res.applied());
    assert_eq!(res.reason, Some(KvReason::Exists));
    assert_eq!(res.value.unwrap()["by"], "worker-2");
    assert_eq!(broker.hit_count(), 1);
}

#[tokio::test]
async fn a_found_key_holding_null_is_not_a_miss() {
    // `'null'::jsonb` is a legal value. Collapsing the two makes an idempotency
    // marker whose value is null read as "not claimed", and the external effect
    // happens twice.
    let broker = FakeBroker::start(vec![kv_element(
        r#"{"index":0,"op":"get","found":true,"key":"k","value":null,"version":3}"#,
    )])
    .await;

    let res = client(&broker).kv().get("ns", "k").await.unwrap();
    assert!(res.found());
    assert_eq!(res.value, Some(serde_json::Value::Null));
}

#[tokio::test]
async fn a_required_write_that_lost_comes_back_as_the_verdict_it_is() {
    // The standalone route answers the precondition envelope instead of a
    // results array. It is HTTP 200 and it must not read as a decode failure.
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"ok":false,"reason":"kv_precondition","failedIndex":0,"kvReason":"exists",
            "version":90101,"value":{"by":"worker-2"}}"#,
    )])
    .await;

    let res = client(&broker)
        .kv()
        .put_if_absent(
            "orders",
            "idem:9137",
            serde_json::json!(true),
            Expiry::seconds(60),
        )
        .required()
        .send()
        .await
        .expect("a lost precondition is the expected outcome of a redelivery");
    assert!(!res.applied());
    assert_eq!(res.reason, Some(KvReason::Exists));
    assert_eq!(res.version, Some(90101));
    assert_eq!(res.value.unwrap()["by"], "worker-2");
}

#[tokio::test]
async fn a_short_results_array_is_refused_rather_than_misattributed() {
    // Two operations, one result. Zipping them would report the wrong key as
    // written — the same guard the ack path carries.
    let broker = FakeBroker::start(vec![kv_results(
        r#"{"index":0,"op":"get","found":false,"key":"a"}"#,
    )])
    .await;

    let err = client(&broker)
        .kv()
        .batch(vec![
            KvOperation::get("ns", "a"),
            KvOperation::get("ns", "b"),
        ])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("1 results for 2"), "{err}");
}

#[tokio::test]
async fn a_paused_surface_answers_503_and_that_reaches_the_caller_as_an_error() {
    // There is no such thing as a cell without KV. There used to be — the boot
    // flags `QUEEN_KV_ENABLED` / `QUEEN_TIMERS_ENABLED` decided whether the
    // routes were registered at all, and a cell without them answered 404 — and
    // both are gone: the surfaces ship with the binary, like push and pop.
    //
    // What remains is the operator's RUNTIME kill switch, which is a different
    // thing and still has to reach the caller intact. It is 503 with
    // `Retry-After`, not 404: the surface exists, an operator has paused it, and
    // it is expected to come back. The one thing a client may never do is read
    // this as a verdict — a `get` that reported `found: false` here would tell a
    // saga its marker was never written while the database still holds it.
    let broker = FakeBroker::start(vec![Reply::json(
        503,
        r#"{"error":"kv_disabled","reason":"kv_disabled"}"#,
    )
    .header("Retry-After", "1")])
    .await;

    let err = client(&broker)
        .kv()
        .get("ns", "k")
        .await
        .expect_err("a paused surface is an error, never a miss");
    assert_eq!(err.status(), Some(503));
    // `kv_disabled` arrives in `error`, which is where the BROKER's closed
    // taxonomy lives; `Error::code` carries the PROXY's, a different vocabulary
    // in a different field, and is `None` for a broker-origin refusal. So the
    // identifier reaches the caller through the message here, and that is not a
    // slip of this test — see the note in `error.rs`.
    assert!(
        err.to_string().contains("kv_disabled"),
        "the operator's reason did not survive the trip: {err}"
    );
    // 503 is a cell condition, so the ordinary retry budget applies and then
    // gives up. It does not sit there for ever, and it does not give up at once.
    assert_eq!(broker.hit_count(), 3, "retry_attempts is 3 by default");
}

#[tokio::test]
async fn a_paused_surface_refuses_a_transaction_rider_permanently() {
    // Same switch, opposite advice, and deliberately so: on the routes a pause
    // is 503 "come back", but a bundle carries messages and retrying it in a
    // loop against a cell somebody has just pulled the lever on is a storm on
    // the hot path. So the wire answers 403 with no Retry-After — terminal —
    // and the client must not retry it.
    let broker = FakeBroker::start(vec![Reply::json(
        403,
        r#"{"error":"kv_disabled","reason":"kv_disabled"}"#,
    )])
    .await;

    let err = client(&broker)
        .transaction()
        .kv_put_if_absent(
            "sent",
            "T",
            serde_json::json!(true),
            Expiry::seconds(86_400),
        )
        .expect("a rider builds")
        .commit()
        .await
        .expect_err("a paused rider fails the whole bundle");
    assert_eq!(err.status(), Some(403));
    assert!(
        err.is_terminal_refusal(),
        "403 says the retry will not help, and the caller needs to hear that"
    );
    assert_eq!(broker.hit_count(), 1, "a 403 is not retried");
}

// ===========================================================================
// Timers: the exact body of every operation
// ===========================================================================

fn timer_results(elements: &str) -> Reply {
    Reply::ok(format!(r#"{{"results":[{elements}]}}"#))
}

#[tokio::test]
async fn a_schedule_sends_milliseconds_and_a_base64_payload() {
    let broker = FakeBroker::start(vec![timer_results(
        r#"{"ok":true,"status":"scheduled","queue":"compensations","timerKey":"saga:9137",
            "txn":"comp-9137","messageId":"0198f0aa-0000-7000-8000-000000000001",
            "deliverAt":"2026-08-17T10:15:00.000000Z"}"#,
    )])
    .await;

    let res = client(&broker)
        .timers()
        .schedule("compensations", "saga:9137", Duration::from_secs(900))
        .partition("customer-42")
        .txn("comp-9137")
        .payload_json(&serde_json::json!({"sagaId": 9137}))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(res.status, TimerStatus::Scheduled);
    assert_eq!(
        res.message_id.as_deref(),
        Some("0198f0aa-0000-7000-8000-000000000001"),
        "the id is promised at schedule time, so the delivered frame can be \
         correlated without waiting for it"
    );

    let hit = &broker.hits()[0];
    assert_eq!(hit.method, "POST");
    assert_eq!(hit.path, "/api/v1/timers");
    assert_eq!(
        hit.body,
        r#"{"operations":[{"op":"schedule","queue":"compensations","timerKey":"saga:9137","partition":"customer-42","delayMs":900000,"txn":"comp-9137","payload":"eyJzYWdhSWQiOjkxMzd9"}]}"#
    );
}

#[tokio::test]
async fn a_schedule_never_sends_a_server_owned_field() {
    // `producer_sub` is the one non-repudiable field of a frame. A client that
    // could set it would get a broker-attested lie a second later, so the
    // broker refuses the whole call — which means an SDK that sent one would
    // simply not work, loudly, for everyone.
    let broker = FakeBroker::start(vec![timer_results(
        r#"{"ok":true,"status":"scheduled","queue":"q","timerKey":"k"}"#,
    )])
    .await;

    client(&broker)
        .timers()
        .schedule("q", "k", Duration::from_secs(1))
        .payload(b"x".to_vec())
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = broker.hits()[0].json();
    let op = &body["operations"][0];
    for name in [
        "producerSub",
        "producer_sub",
        "messageId",
        "message_id",
        "tenant",
        "tenantId",
        "deliverAt",
        "deliver_at",
        "delaySeconds",
        "delay_seconds",
        "attempts",
        "claimToken",
        "claimedUntil",
    ] {
        assert!(op.get(name).is_none(), "{name} is server-owned");
    }
    assert!(op.as_object().unwrap().keys().all(|k| !k.starts_with('_')));
}

#[tokio::test]
async fn a_cancel_uses_the_route_that_can_never_be_blocked() {
    // §9.6: DELETE /api/v1/timers/:queue/*timerKey has its own authorization
    // class precisely so a tenant over quota can still stop what it started —
    // the fire never switches itself off. A cancel sent through POST
    // /api/v1/timers inherits the schedule's class and can be refused with it.
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"ok":true,"status":"cancelled","queue":"compensations","timerKey":"saga:9137",
            "txn":"comp-9137"}"#,
    )])
    .await;

    let res = client(&broker)
        .timers()
        .cancel("compensations", "saga:9137")
        .await
        .unwrap();
    assert!(res.ok);
    assert_eq!(res.status, TimerStatus::Cancelled);

    let hit = &broker.hits()[0];
    assert_eq!(hit.method, "DELETE");
    assert_eq!(hit.path, "/api/v1/timers/compensations/saga%3A9137");
    assert_eq!(hit.body, "");
}

#[tokio::test]
async fn a_cancel_can_echo_the_txn_it_expects() {
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"ok":false,"status":"absent","queue":"q","timerKey":"k","txn":"comp-9137"}"#,
    )])
    .await;

    let res = client(&broker)
        .timers()
        .cancel_expecting("q", "k", "comp-9137")
        .await
        .unwrap();

    // absent carries ok:false, and the txn comes back so the caller can look
    // for it in the destination queue: there is no tombstone, so absent MAY
    // mean already delivered.
    assert!(!res.ok);
    assert_eq!(res.status, TimerStatus::Absent);
    assert_eq!(res.txn.as_deref(), Some("comp-9137"));
    assert_eq!(broker.hits()[0].path, "/api/v1/timers/q/k?txn=comp-9137");
}

#[tokio::test]
async fn too_late_is_a_verdict_and_not_a_failure() {
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"ok":false,"status":"too_late","queue":"q","timerKey":"k"}"#,
    )])
    .await;

    let res = client(&broker).timers().cancel("q", "k").await.unwrap();
    assert_eq!(res.status, TimerStatus::TooLate);
    assert!(!res.ok && !res.is_pending());
    assert_eq!(broker.hit_count(), 1, "a verdict must never be retried");
}

#[tokio::test]
async fn a_peek_reads_one_key_and_decodes_its_payload() {
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"found":true,"queue":"q","timerKey":"k","partition":"Default",
            "deliverAt":"2026-08-17T10:15:00.000000Z","txn":"t","messageId":"m",
            "payload":"eyJzYWdhSWQiOjkxMzd9","payloadZstd":false,"encrypted":false,
            "attempts":0,"claimed":false}"#,
    )])
    .await;

    let peek = client(&broker).timers().peek("q", "k").await.unwrap();
    assert!(peek.found);
    assert_eq!(
        peek.payload_bytes().unwrap().unwrap(),
        br#"{"sagaId":9137}"#
    );
    let hit = &broker.hits()[0];
    assert_eq!(hit.method, "GET");
    assert_eq!(hit.path, "/api/v1/timers/q/k");
}

#[tokio::test]
async fn a_list_pages_by_cursor() {
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"rows":[],"truncated":false,"nextAfter":null}"#,
    )])
    .await;

    let page = client(&broker)
        .timers()
        .list("compensations")
        .after("saga:100")
        .limit(50)
        .send()
        .await
        .unwrap();
    assert!(!page.truncated);
    assert_eq!(page.next_after, None);
    assert_eq!(
        broker.hits()[0].path,
        "/api/v1/timers/compensations?after=saga%3A100&limit=50"
    );
}

#[tokio::test]
async fn a_batch_of_timers_keeps_its_order_and_is_length_checked() {
    let broker = FakeBroker::start(vec![timer_results(
        r#"{"ok":true,"status":"scheduled","queue":"q","timerKey":"a"}"#,
    )])
    .await;

    let err = client(&broker)
        .timers()
        .batch(vec![
            TimerOperation::schedule("q", "a", 1000, "t1", b"x"),
            TimerOperation::cancel("q", "b"),
        ])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("1 results for 2"), "{err}");
}

// ===========================================================================
// The transaction riders
// ===========================================================================

#[tokio::test]
async fn the_riders_are_top_level_fields_of_the_request() {
    // The single most important assertion in this file. If these ever became
    // operations, a typed client would send a body whose KV array was silently
    // absent and the broker would commit a transaction with no gate: the
    // putIfAbsent the bundle existed for would never have happened, and nothing
    // anywhere would say so.
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"transactionId":"T","success":true,"results":[
            {"index":0,"type":"push","success":true,"transactionId":"t1","messageId":"m1","queueName":"invoices"},
            {"index":1,"opIndex":0,"type":"kv","op":"put","applied":true,"key":"idem:9137","value":true,"version":1},
            {"index":2,"opIndex":0,"type":"timer","ok":true,"status":"scheduled","queue":"compensations","timerKey":"saga:9137","txn":"c-1","messageId":"m2","deliverAt":"2026-08-17T10:15:00.000000Z"}]}"#,
    )])
    .await;

    let resp = client(&broker)
        .transaction()
        .push("invoices", serde_json::json!({"order": 9137}))
        .unwrap()
        .kv_put_if_absent(
            "orders",
            "idem:9137",
            serde_json::json!(true),
            Expiry::seconds(86_400),
        )
        .unwrap()
        .schedule(
            "compensations",
            "saga:9137",
            Duration::from_secs(900),
            b"{}".to_vec(),
        )
        .commit()
        .await
        .unwrap();
    assert!(resp.success);

    let body: serde_json::Value = broker.hits()[0].json();
    assert!(body["kv"].is_array(), "kv must be a top-level field");
    assert!(
        body["timers"].is_array(),
        "timers must be a top-level field"
    );
    for op in body["operations"].as_array().unwrap() {
        assert_eq!(op["type"], "push");
    }
    assert_eq!(body["kv"][0]["op"], "putIfAbsent");
    assert_eq!(
        body["kv"][0]["required"], true,
        "the gate has to be required, or the bundle commits when the marker \
         is already there"
    );
    assert_eq!(body["timers"][0]["delayMs"], 900_000);

    // The rider results, read back through the flat index space.
    assert_eq!(resp.results.len(), 3);
    let kv = resp.kv_results();
    assert_eq!(kv.len(), 1);
    assert!(kv[0].applied());
    let timers = resp.timer_results();
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[0].status, TimerStatus::Scheduled);
    assert_eq!(timers[0].message_id.as_deref(), Some("m2"));
}

#[tokio::test]
async fn a_transaction_without_riders_sends_exactly_what_it_always_sent() {
    // Two new fields on the request type must not add two keys to every bundle
    // that does not use them.
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"transactionId":"T","success":true,"results":[
            {"index":0,"type":"push","success":true,"transactionId":"t1","messageId":"m1","queueName":"q"}]}"#,
    )])
    .await;

    client(&broker)
        .transaction()
        .push("q", serde_json::json!(1))
        .unwrap()
        .commit()
        .await
        .unwrap();

    let body = &broker.hits()[0].body;
    assert!(!body.contains("\"kv\""), "{body}");
    assert!(!body.contains("\"timers\""), "{body}");
}

#[tokio::test]
async fn commit_returns_on_a_lost_precondition_and_does_not_raise() {
    // The wire change §10.2 lists for this client. A redelivery meeting its own
    // idempotency marker is the system working; raising would put the most
    // frequent verdict of the feature into the caller's error path.
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"transactionId":"T","success":false,"reason":"kv_precondition",
            "error":"kv_precondition_failed","results":[],"ok":false,
            "failedIndex":2,"kvReason":"exists","version":90101,"value":{"by":"worker-2"}}"#,
    )])
    .await;

    let resp = client(&broker)
        .transaction()
        .push("q", serde_json::json!(1))
        .unwrap()
        .kv_put_if_absent(
            "orders",
            "idem:9137",
            serde_json::json!(true),
            Expiry::seconds(60),
        )
        .unwrap()
        .commit()
        .await
        .expect("a lost precondition must RETURN, not raise");

    assert!(!resp.success, "the bundle did not commit, and says so");
    let lost = resp.lost_precondition().expect("the verdict is readable");
    assert_eq!(lost.reason, Some(KvReason::Exists));
    assert_eq!(
        lost.failed_index,
        Some(2),
        "the index is in the flat space, so it points at the caller's own \
         operation and not at somebody else's"
    );
    assert_eq!(lost.version, Some(90101));
    assert_eq!(lost.value.unwrap()["by"], "worker-2");
    assert_eq!(broker.hit_count(), 1, "a verdict must never be retried");
}

#[tokio::test]
async fn commit_still_raises_on_every_other_rollback() {
    // The other half of the same rule: a duplicate push and a rejected ack are
    // still errors, and a caller who ignored them would believe a handoff
    // happened that did not.
    for reason in ["duplicate", "ack_rejected", "db_error", "misaligned"] {
        let broker = FakeBroker::start(vec![Reply::ok(format!(
            r#"{{"transactionId":"T","success":false,"reason":"{reason}",
                 "error":"QDUP duplicate transactionId","results":[]}}"#
        ))])
        .await;

        let err = client(&broker)
            .transaction()
            .push("q", serde_json::json!(1))
            .unwrap()
            .kv_put_if_absent("ns", "k", serde_json::json!(true), Expiry::seconds(60))
            .unwrap()
            .commit()
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("rolled back"),
            "{reason} must stay an error: {err}"
        );
    }
}

#[tokio::test]
async fn an_old_broker_rolling_back_without_a_reason_still_raises() {
    // New client, old broker: no `reason` at all in the failure body. It must
    // not read as a lost precondition, or a rejected ack would look like a
    // successful call.
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"transactionId":"T","success":false,"error":"QTXN ack references unknown transactionId","results":[]}"#,
    )])
    .await;

    let err = client(&broker)
        .transaction()
        .push("q", serde_json::json!(1))
        .unwrap()
        .commit()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("QTXN"), "{err}");
}

#[tokio::test]
async fn a_riders_only_bundle_is_a_legal_transaction() {
    // Two KV writes and nothing else is a valid bundle: the atomicity is
    // between the writes themselves.
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"transactionId":"T","success":true,"results":[
            {"index":0,"opIndex":0,"type":"kv","op":"put","applied":true,"key":"a","version":1},
            {"index":1,"opIndex":1,"type":"kv","op":"delete","applied":true,"key":"b","version":0}]}"#,
    )])
    .await;

    let resp = client(&broker)
        .transaction()
        .kv_put("ns", "a", serde_json::json!(1), Expiry::seconds(60))
        .unwrap()
        .kv_delete("ns", "b")
        .commit()
        .await
        .unwrap();
    assert_eq!(resp.kv_results().len(), 2);

    let body: serde_json::Value = broker.hits()[0].json();
    assert_eq!(
        body["operations"].as_array().unwrap().len(),
        0,
        "an empty operations array is still required by the broker's reader"
    );
}

#[tokio::test]
async fn a_transaction_that_stages_nothing_at_all_is_still_refused() {
    let broker = FakeBroker::start(vec![Reply::ok("{}")]).await;
    let err = client(&broker).transaction().commit().await.unwrap_err();
    assert!(err.to_string().contains("at least one operation"), "{err}");
    assert_eq!(broker.hit_count(), 0, "it never reached the network");
}
