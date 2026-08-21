//! What the ephemeral surface actually puts on the socket
//! (EPHEMERAL_QUEUES.md §3.1, §4, §4.1).
//!
//! These run against [`support::FakeBroker`] and need no stack, which is the
//! point: the body is the contract with the broker, and on this family a wrong
//! shape is especially quiet. The push handler parses with serde, so a
//! `message` array where `messages` was meant is a 400 nobody can diagnose from
//! the client side; a misspelt configure knob is an option the broker never
//! sees, i.e. a ring that grows until a global budget answers 503.
//!
//! So every test below asserts the **exact bytes**, not a shape.
//!
//! Three things are pinned beyond the eight bodies:
//!
//!  1. THE 404 RULE. Against a broker or proxy older than 1.1 the whole family
//!     answers 404, and every verb must turn that into one message rather than
//!     into eight different "not found" stories.
//!  2. THE EMPTY POP. `messages` reaches the caller as an empty vector however
//!     the broker spelled it.
//!  3. THE DURABLE BUFFER IS UNTOUCHED. Buffered ephemeral push reuses the
//!     durable machinery through the `Destination` seam, and the durable body is
//!     asserted byte for byte so that reuse cannot become drift.

mod support;

use std::time::Duration;

use queen_mq::ephemeral::{is_queue_not_found, is_unsupported, EPHEMERAL_UNSUPPORTED};
use queen_mq::{
    BufferOptions, Config, EphemeralAck, EphemeralDelivered, EphemeralOptions, EphemeralOutcome,
    EphemeralPolicy, EphemeralStatus, EphemeralWindowBuffer, Queen, Retry429,
};
use support::{FakeBroker, Hit, Reply};

fn client(broker: &FakeBroker) -> Queen {
    Queen::connect(Config::new(broker.url())).expect("fake broker url")
}

/// A broker that never registered these routes.
fn gone() -> Reply {
    Reply::json(404, r#"{"error":"Not Found"}"#)
}

fn created(body: &str) -> Reply {
    Reply::json(201, body)
}

/// Wait for the flusher, which drains on a spawned task rather than inline.
async fn one_hit(broker: &FakeBroker) -> Hit {
    assert_eq!(
        broker.wait_for_hits(1, Duration::from_secs(5)).await,
        1,
        "expected exactly one request"
    );
    broker.hits().remove(0)
}

// ===========================================================================
// configure / reset / delete
// ===========================================================================

#[tokio::test]
async fn configure_sends_the_seven_knobs_and_nothing_else() {
    let broker = FakeBroker::start(vec![created(r#"{"queue":"inbox"}"#)]).await;

    client(&broker)
        .ephemeral()
        .configure(
            "inbox",
            EphemeralOptions {
                max_bytes: Some(1_048_576),
                max_length: Some(500),
                policy: Some(EphemeralPolicy::DropOldest),
                ttl_seconds: Some(30),
                lease_seconds: Some(10),
                retry_limit: Some(3),
                window_buffer: Some(EphemeralWindowBuffer { ms: 25, count: 50 }),
            },
        )
        .await
        .expect("configure");

    let hit = &broker.hits()[0];
    assert_eq!(hit.method, "POST");
    assert_eq!(hit.path, "/api/v1/ephemeral/configure");
    assert_eq!(
        hit.body,
        r#"{"queue":"inbox","options":{"maxBytes":1048576,"maxLength":500,"policy":"dropOldest","ttlSeconds":30,"leaseSeconds":10,"retryLimit":3,"windowBuffer":{"ms":25,"count":50}}}"#
    );
}

#[tokio::test]
async fn configure_omits_every_knob_it_was_not_given() {
    // The knobs are Options so that "not supplied" and "supplied as zero" stay
    // different things: an omitted knob leaves the broker's default in charge,
    // while `"retryLimit":0` is a queue that drops on the first nack.
    let broker = FakeBroker::start(vec![created(r#"{"queue":"inbox"}"#)]).await;

    client(&broker)
        .ephemeral()
        .configure("inbox", EphemeralOptions::default())
        .await
        .expect("configure");

    assert_eq!(broker.hits()[0].body, r#"{"queue":"inbox","options":{}}"#);
}

#[tokio::test]
async fn reset_sends_the_queue_and_reads_the_dropped_count() {
    let broker = FakeBroker::start(vec![Reply::ok(r#"{"dropped":4211}"#)]).await;

    let dropped = client(&broker)
        .ephemeral()
        .reset("inbox")
        .await
        .expect("reset");
    assert_eq!(dropped, 4211);

    let hit = &broker.hits()[0];
    assert_eq!(hit.method, "POST");
    assert_eq!(hit.path, "/api/v1/ephemeral/reset");
    assert_eq!(hit.body, r#"{"queue":"inbox"}"#);
}

#[tokio::test]
async fn delete_uses_the_path_route_and_sends_no_body() {
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"queue":"reply/42","deleted":true,"declared":true}"#,
    )])
    .await;

    let deleted = client(&broker)
        .ephemeral()
        .delete("reply/42")
        .await
        .expect("delete");
    assert!(deleted.deleted && deleted.declared);
    assert_eq!(deleted.queue, "reply/42");

    let hit = &broker.hits()[0];
    assert_eq!(hit.method, "DELETE");
    // The name is percent-encoded whole: a `/` in a queue name must not add a
    // path segment, or the route stops matching and the answer is a 404 that
    // this client would then report as "upgrade your broker".
    assert_eq!(hit.path, "/api/v1/ephemeral/queue/reply%2F42");
    assert_eq!(hit.body, "");
}

// ===========================================================================
// push
// ===========================================================================

#[tokio::test]
async fn push_hoists_the_identity_to_the_envelope() {
    // The whole reason the buffered drain takes a destination: the DURABLE push
    // repeats {queue, partition} on every item, this one carries them once and
    // the elements are `{payload}` and nothing else — no transactionId, because
    // there is no dedup index to hold one.
    let broker = FakeBroker::start(vec![created(r#"{"pushed":2}"#)]).await;

    let out = client(&broker)
        .ephemeral()
        // Single-key payloads on purpose: `serde_json::Value` keeps objects in a
        // BTreeMap here (no `preserve_order`), so a multi-key payload would make
        // this assertion about serde's key ordering rather than about the
        // envelope, which is what is under test.
        .push_many(
            "presence",
            vec![
                serde_json::json!({"user":"a"}),
                serde_json::json!({"user":"b"}),
            ],
        )
        .partition("room-7")
        .send()
        .await
        .expect("push");
    assert_eq!(out.pushed, 2);
    assert_eq!(out.count, 2);
    assert!(!out.buffered);

    let hit = &broker.hits()[0];
    assert_eq!(hit.method, "POST");
    assert_eq!(hit.path, "/api/v1/ephemeral/push");
    assert_eq!(
        hit.body,
        r#"{"queue":"presence","partition":"room-7","messages":[{"payload":{"user":"a"}},{"payload":{"user":"b"}}]}"#
    );
}

#[tokio::test]
async fn push_omits_the_partition_it_was_not_given() {
    // Never defaulted client-side: which ring a push without a partition lands
    // on is the broker's rule, and inventing "Default" here would take that
    // decision away from it in a way the caller never asked for.
    let broker = FakeBroker::start(vec![created(r#"{"pushed":1}"#)]).await;

    client(&broker)
        .ephemeral()
        .push("presence", serde_json::json!(1))
        .send()
        .await
        .expect("push");

    assert_eq!(
        broker.hits()[0].body,
        r#"{"queue":"presence","messages":[{"payload":1}]}"#
    );
}

#[tokio::test]
async fn a_null_payload_is_a_payload() {
    let broker = FakeBroker::start(vec![created(r#"{"pushed":1}"#)]).await;

    client(&broker)
        .ephemeral()
        .push("shapes", serde_json::Value::Null)
        .send()
        .await
        .expect("push");

    assert_eq!(
        broker.hits()[0].body,
        r#"{"queue":"shapes","messages":[{"payload":null}]}"#
    );
}

#[tokio::test]
async fn an_empty_push_never_reaches_the_network() {
    let broker = FakeBroker::start(vec![created(r#"{"pushed":0}"#)]).await;

    let out = client(&broker)
        .ephemeral()
        .push_many("presence", Vec::<serde_json::Value>::new())
        .send()
        .await
        .expect("push");
    assert_eq!(out.count, 0);
    assert_eq!(broker.hit_count(), 0);
}

// ===========================================================================
// pop
// ===========================================================================

#[tokio::test]
async fn a_plain_pop_sends_the_shortest_query_this_route_can_receive() {
    let broker = FakeBroker::start(vec![Reply::ok(r#"{"queue":"inbox","messages":[]}"#)]).await;

    client(&broker)
        .ephemeral()
        .pop("inbox")
        .send()
        .await
        .expect("pop");

    let hit = &broker.hits()[0];
    assert_eq!(hit.method, "GET");
    assert_eq!(
        hit.path, "/api/v1/ephemeral/pop?queue=inbox",
        "a pop that asked for nothing must send nothing but its queue"
    );
}

#[tokio::test]
async fn a_waiting_pop_always_carries_an_explicit_timeout() {
    // wait=true without a timeout would leave the window to the broker's
    // default while the HTTP deadline is computed from THIS client's number:
    // the two disagreeing is a client that aborts a request the broker was
    // about to answer.
    let broker = FakeBroker::start(vec![Reply::ok(r#"{"queue":"inbox","messages":[]}"#)]).await;

    client(&broker)
        .ephemeral()
        .pop("inbox")
        .wait(true)
        .poll_timeout(Duration::from_millis(1500))
        .send()
        .await
        .expect("pop");

    let hit = &broker.hits()[0];
    assert_eq!(hit.query("wait"), Some("true"));
    assert_eq!(hit.query("timeout"), Some("1500"));
}

#[tokio::test]
async fn a_pop_carries_every_option_it_was_given_in_a_stable_order() {
    let broker = FakeBroker::start(vec![Reply::ok(r#"{"queue":"inbox","messages":[]}"#)]).await;

    client(&broker)
        .ephemeral()
        .pop("inbox")
        .partition("room-7")
        .batch(10)
        .wait(true)
        .poll_timeout(Duration::from_millis(1500))
        .group("workers")
        .auto_ack(true)
        .send()
        .await
        .expect("pop");

    assert_eq!(
        broker.hits()[0].path,
        "/api/v1/ephemeral/pop?queue=inbox&partition=room-7&batch=10&wait=true&timeout=1500&group=workers&autoAck=true"
    );
}

#[tokio::test]
async fn a_declined_flag_is_indistinguishable_from_an_unset_one() {
    // Byte-identical requests for every consumer that does not opt in: the flags
    // are emitted ONLY when true, so a plain pop cannot be told apart from one
    // that explicitly declined at-most-once delivery.
    let broker = FakeBroker::start(vec![Reply::ok(r#"{"queue":"inbox","messages":[]}"#)]).await;

    client(&broker)
        .ephemeral()
        .pop("inbox")
        .wait(false)
        .auto_ack(false)
        .send()
        .await
        .expect("pop");

    assert_eq!(broker.hits()[0].path, "/api/v1/ephemeral/pop?queue=inbox");
}

#[tokio::test]
async fn a_pop_percent_encodes_its_values() {
    let broker = FakeBroker::start(vec![Reply::ok(r#"{"queue":"a b","messages":[]}"#)]).await;

    client(&broker)
        .ephemeral()
        .pop("a b")
        .group("g&h")
        .send()
        .await
        .expect("pop");

    assert_eq!(
        broker.hits()[0].path,
        "/api/v1/ephemeral/pop?queue=a%20b&group=g%26h"
    );
}

#[tokio::test]
async fn a_pop_parses_the_delivered_message_and_keeps_the_group_with_it() {
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"queue":"inbox","messages":[{"id":"e:9f1:room-7:12","partition":"room-7","attempts":2,"payload":{"n":1}}]}"#,
    )])
    .await;

    let batch = client(&broker)
        .ephemeral()
        .pop("inbox")
        .group("workers")
        .send()
        .await
        .expect("pop");

    assert_eq!(batch.queue, "inbox");
    assert_eq!(batch.group.as_deref(), Some("workers"));
    assert_eq!(batch.len(), 1);
    let msg: &EphemeralDelivered = &batch.messages[0];
    assert_eq!(msg.id, "e:9f1:room-7:12");
    assert_eq!(msg.partition, "room-7");
    assert_eq!(msg.attempts, 2);
    assert_eq!(msg.payload["n"], 1);
}

#[tokio::test]
async fn an_empty_pop_is_always_an_empty_batch() {
    // Three spellings of "nothing", one shape at the caller.
    for body in [
        r#"{"queue":"inbox","messages":[]}"#,
        r#"{"queue":"inbox"}"#,
        r#"{"queue":"","messages":[]}"#,
    ] {
        let broker = FakeBroker::start(vec![Reply::ok(body)]).await;
        let batch = client(&broker)
            .ephemeral()
            .pop("inbox")
            .send()
            .await
            .unwrap_or_else(|e| panic!("pop ({body}): {e}"));
        assert!(batch.is_empty(), "{body}");
        assert_eq!(batch.queue, "inbox", "{body} lost the queue name");
    }
}

// ===========================================================================
// ack
// ===========================================================================

#[tokio::test]
async fn acking_a_batch_reads_the_queue_and_the_group_off_it() {
    // The rule this crate keeps everywhere: a forgotten group cannot ack the
    // wrong cursor. The wire alone would not give it — an ephemeral delivery
    // carries no group — so the batch does.
    let broker = FakeBroker::start(vec![
        Reply::ok(
            r#"{"queue":"inbox","messages":[{"id":"e:1:Default:1","partition":"Default","attempts":1,"payload":1}]}"#,
        ),
        Reply::ok(r#"{"results":[{"id":"e:1:Default:1","outcome":"acked"}]}"#),
    ])
    .await;

    let queen = client(&broker);
    let batch = queen
        .ephemeral()
        .pop("inbox")
        .group("workers")
        .send()
        .await
        .expect("pop");
    let results = queen.ephemeral().ack(&batch).await.expect("ack");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].outcome, EphemeralOutcome::Acked);

    let hit = &broker.hits()[1];
    assert_eq!(hit.method, "POST");
    assert_eq!(hit.path, "/api/v1/ephemeral/ack");
    assert_eq!(
        hit.body,
        r#"{"queue":"inbox","group":"workers","acks":[{"id":"e:1:Default:1","status":"completed"}]}"#
    );
}

#[tokio::test]
async fn a_nack_carries_its_reason_even_though_the_broker_drops_it() {
    // The `error` field exists on this wire so one ack builder can serve both
    // engines; the broker accepts and ignores it, having no DLQ row to record
    // it on. Sending it anyway is what keeps the two families' call sites
    // identical.
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"results":[{"id":"e:1:Default:1","outcome":"redelivered"}]}"#,
    )])
    .await;

    let queen = client(&broker);
    let batch = queen_mq::EphemeralBatch {
        queue: "inbox".into(),
        group: None,
        messages: vec![EphemeralDelivered {
            id: "e:1:Default:1".into(),
            partition: "Default".into(),
            payload: serde_json::json!(1),
            attempts: 1,
        }],
    };
    let results = queen.ephemeral().nack(&batch, "boom").await.expect("nack");
    assert_eq!(results[0].outcome, EphemeralOutcome::Redelivered);

    assert_eq!(
        broker.hits()[0].body,
        r#"{"queue":"inbox","acks":[{"id":"e:1:Default:1","status":"failed","error":"boom"}]}"#
    );
}

#[tokio::test]
async fn hand_built_acks_can_mix_statuses_in_one_request() {
    // How a handler that partially succeeded travels: two completed and one
    // retry, in a single round trip.
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"results":[{"id":"a","outcome":"acked"},{"id":"b","outcome":"redelivered"},{"id":"c","outcome":"stale"}]}"#,
    )])
    .await;

    let results = client(&broker)
        .ephemeral()
        .ack_ids(
            "inbox",
            vec![
                EphemeralAck::new("a").status(EphemeralStatus::Completed),
                EphemeralAck::new("b").status(EphemeralStatus::Retry),
                EphemeralAck::new("c"),
            ],
        )
        .group("workers")
        .send()
        .await
        .expect("ack");

    // `stale` is an answer, not an error: it is how this class fences a restart.
    assert_eq!(results[2].outcome, EphemeralOutcome::Stale);
    assert_eq!(
        broker.hits()[0].body,
        r#"{"queue":"inbox","group":"workers","acks":[{"id":"a","status":"completed"},{"id":"b","status":"retry"},{"id":"c"}]}"#
    );
}

#[tokio::test]
async fn a_misaligned_ack_answer_is_refused() {
    // Silently zipping one result onto three acks would report the wrong
    // message as acknowledged — the same guard the durable ack path carries.
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"results":[{"id":"a","outcome":"acked"}]}"#,
    )])
    .await;

    let err = client(&broker)
        .ephemeral()
        .ack_ids(
            "inbox",
            vec![
                EphemeralAck::new("a"),
                EphemeralAck::new("b"),
                EphemeralAck::new("c"),
            ],
        )
        .send()
        .await
        .expect_err("a short array must not be attributed");
    assert!(err.to_string().contains("1 results for 3"), "{err}");
}

// ===========================================================================
// status
// ===========================================================================

#[tokio::test]
async fn the_status_reads_go_to_their_routes() {
    let broker = FakeBroker::start(vec![
        Reply::ok(r#"{"queues":[{"queue":"inbox","depth":3}]}"#),
        Reply::ok(r#"{"queue":"in box","depth":3,"bytes":128}"#),
    ])
    .await;

    let queen = client(&broker);
    let queues = queen.ephemeral().queues().await.expect("queues");
    assert_eq!(queues["queues"][0]["depth"], 3);

    let depth = queen.ephemeral().depth("in box").await.expect("depth");
    assert_eq!(depth["bytes"], 128);

    let hits = broker.hits();
    assert_eq!(hits[0].path, "/api/v1/ephemeral/queues");
    assert_eq!(hits[1].path, "/api/v1/ephemeral/queues/in%20box/depth");
}

// ===========================================================================
// the 404 rule (§4, §8)
// ===========================================================================

#[tokio::test]
async fn every_verb_maps_a_404_to_one_message() {
    // One answer for the whole family, because a 404 here always means the same
    // thing: the routes are not there. An old broker never registered them; an
    // old proxy answers `route_blocked` because it fails closed on unknown API
    // paths. Neither is "your queue is missing" — these verbs answer an absent
    // queue with a normal body, because naming one is what creates it.
    let broker = FakeBroker::start(vec![gone()]).await;
    let queen = client(&broker);
    let eph = queen.ephemeral();

    let errors = vec![
        (
            "configure",
            eph.configure("inbox", EphemeralOptions::default())
                .await
                .err(),
        ),
        ("reset", eph.reset("inbox").await.err()),
        ("delete", eph.delete("inbox").await.err()),
        (
            "push",
            eph.push("inbox", serde_json::json!(1)).send().await.err(),
        ),
        ("pop", eph.pop("inbox").send().await.err()),
        (
            "ack",
            eph.ack_ids("inbox", vec![EphemeralAck::new("e:1:Default:1")])
                .send()
                .await
                .err(),
        ),
        ("queues", eph.queues().await.err()),
        ("depth", eph.depth("inbox").await.err()),
    ];

    for (verb, err) in errors {
        let err = err.unwrap_or_else(|| panic!("{verb}: a 404 was not reported at all"));
        assert!(is_unsupported(&err), "{verb}: {err}");
        assert!(
            err.to_string().contains(EPHEMERAL_UNSUPPORTED),
            "{verb} reported {err}, not the fixed upgrade message"
        );
    }
}

#[tokio::test]
async fn a_proxy_route_block_keeps_its_code_and_gains_the_message() {
    // The proxy's spelling of the same fact. The code is what a client branches
    // on; the message is what it means.
    let broker = FakeBroker::start(vec![Reply::json(
        404,
        r#"{"error":"route not available","code":"route_blocked"}"#,
    )])
    .await;

    let err = client(&broker)
        .ephemeral()
        .pop("inbox")
        .send()
        .await
        .expect_err("route_blocked must not be silent");
    assert!(is_unsupported(&err));
    assert_eq!(err.code().map(|c| c.as_str()), Some("route_blocked"));
    assert!(err.to_string().contains(EPHEMERAL_UNSUPPORTED), "{err}");
}

#[tokio::test]
async fn depths_own_404_is_not_an_upgrade_error() {
    // The one 404 this family answers about a QUEUE rather than about the
    // routes. Reporting it as "upgrade your broker" would send an operator
    // looking at versions because they typed a name that no longer has a ring
    // behind it — which on this class is the ordinary end of an implicit queue.
    let broker = FakeBroker::start(vec![Reply::json(
        404,
        r#"{"error":"no ephemeral queue by that name exists on this broker","code":"ephemeral_queue_not_found"}"#,
    )])
    .await;

    let err = client(&broker)
        .ephemeral()
        .depth("gone")
        .await
        .expect_err("a missing queue must be reported");
    assert!(is_queue_not_found(&err), "{err}");
    assert!(!is_unsupported(&err), "{err}");
    // The broker's own explanation survives — it is the useful one here.
    assert!(!err.to_string().contains(EPHEMERAL_UNSUPPORTED), "{err}");
}

#[tokio::test]
async fn a_bare_404_on_the_same_route_is_still_the_upgrade_case() {
    // Same route, same status, no ephemeral code: an old broker that never
    // registered it. The code is the whole difference.
    let broker = FakeBroker::start(vec![gone()]).await;

    let err = client(&broker)
        .ephemeral()
        .depth("inbox")
        .await
        .expect_err("a codeless 404 must be reported");
    assert!(is_unsupported(&err), "{err}");
    assert!(!is_queue_not_found(&err), "{err}");
    assert!(err.to_string().contains(EPHEMERAL_UNSUPPORTED), "{err}");
}

#[tokio::test]
async fn a_refusal_is_not_an_upgrade() {
    // 429 `queue_full` is the ring's own backpressure (§1.6) and 503
    // `ephemeral_unavailable` is the cell's byte ceiling. Both are transient and
    // neither is an upgrade, so both must reach the caller as themselves.
    let broker = FakeBroker::start(vec![Reply::json(
        429,
        r#"{"error":"the ephemeral queue is at its maxBytes/maxLength and its policy is reject","code":"queue_full"}"#,
    )])
    .await;

    let queen = Queen::connect(
        Config::new(broker.url())
            .retry_attempts(1)
            .retry_429(Retry429 {
                max_attempts: Some(1),
                ..Default::default()
            }),
    )
    .expect("fake broker url");

    let err = queen
        .ephemeral()
        .push("inbox", serde_json::json!(1))
        .send()
        .await
        .expect_err("a full ring must be reported");
    assert!(
        !is_unsupported(&err),
        "a 429 was mapped to the upgrade case"
    );
    assert_eq!(err.status(), Some(429));
    assert_eq!(err.code().map(|c| c.as_str()), Some("queue_full"));
    assert!(err.is_rate_limited());
}

// ===========================================================================
// buffering (§4.1) — the destination, the address, and the durable pin
// ===========================================================================

#[tokio::test]
async fn a_buffered_push_drains_to_the_ephemeral_wire() {
    let broker = FakeBroker::start(vec![created(r#"{"pushed":2}"#)]).await;
    let queen = client(&broker);

    let out = queen
        .ephemeral()
        .push_many(
            "presence",
            vec![serde_json::json!({"n":1}), serde_json::json!({"n":2})],
        )
        .partition("room-7")
        .buffered(BufferOptions {
            message_count: 2,
            time: Duration::from_secs(60),
            ..Default::default()
        })
        .send()
        .await
        .expect("buffered push");

    // A buffered push resolves once the messages are IN the buffer, so the
    // answer says `buffered`, never `pushed`.
    assert!(out.buffered);
    assert_eq!(out.count, 2);
    assert_eq!(out.pushed, 0);

    let hit = one_hit(&broker).await;
    assert_eq!(hit.path, "/api/v1/ephemeral/push");
    // The SAME envelope the unbuffered push builds: the destination is where
    // the shape lives, so the two paths cannot drift.
    assert_eq!(
        hit.body,
        r#"{"queue":"presence","partition":"room-7","messages":[{"payload":{"n":1}},{"payload":{"n":2}}]}"#
    );
}

#[tokio::test]
async fn the_durable_buffered_body_is_unchanged() {
    // THE PIN. `Destination::Durable` is today's request, byte for byte, and it
    // is what every buffer created through the durable path gets. This test
    // exists for no other reason than to fail if that stops being true:
    // identity per ITEM, envelope `{items}`, and not one key more.
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"[{"index":0,"message_id":"m1","transaction_id":"t1","queueName":"orders","status":"queued"}]"#,
    )])
    .await;
    let queen = client(&broker);

    queen
        .queue("orders")
        .partition("eu")
        .buffer(BufferOptions {
            message_count: 1,
            time: Duration::from_secs(60),
            ..Default::default()
        })
        .push_items(vec![queen_mq::PushItem {
            queue: "orders".into(),
            partition: Some("eu".into()),
            payload: serde_json::json!({"total":19.99}),
            transaction_id: Some("11111111-1111-4111-8111-111111111111".into()),
        }])
        .await
        .expect("durable buffered push");

    let hit = one_hit(&broker).await;
    assert_eq!(hit.path, "/api/v1/push");
    assert_eq!(
        hit.body,
        r#"{"items":[{"queue":"orders","partition":"eu","payload":{"total":19.99},"transactionId":"11111111-1111-4111-8111-111111111111"}]}"#
    );
}

#[tokio::test]
async fn the_two_families_never_share_a_buffer_or_a_drain() {
    // An ephemeral `orders` and a durable `orders` are unrelated objects
    // (§10 Q8). A shared buffer address would post one family's messages to the
    // other family's route, which is a message loss with a 201 on it.
    let broker = FakeBroker::start_with(|_, hit| {
        if hit.path.starts_with("/api/v1/ephemeral/push") {
            Reply::json(201, r#"{"pushed":1}"#)
        } else {
            Reply::ok(
                r#"[{"index":0,"message_id":"m1","transaction_id":"t1","queueName":"orders","status":"queued"}]"#,
            )
        }
    })
    .await;
    let queen = client(&broker);

    let opts = BufferOptions {
        message_count: 1,
        time: Duration::from_secs(60),
        ..Default::default()
    };
    queen
        .queue("orders")
        .buffer(opts)
        .push(serde_json::json!({"durable":true}))
        .await
        .expect("durable buffered push");
    queen
        .ephemeral()
        .push("orders", serde_json::json!({"ephemeral":true}))
        .buffered(opts)
        .send()
        .await
        .expect("ephemeral buffered push");

    assert_eq!(
        broker.wait_for_hits(2, Duration::from_secs(5)).await,
        2,
        "each family should have drained once"
    );

    let hits = broker.hits();
    let durable = hits.iter().find(|h| h.path == "/api/v1/push");
    let ephemeral = hits.iter().find(|h| h.path == "/api/v1/ephemeral/push");
    let durable = durable.expect("the durable buffer never drained to its own route");
    let ephemeral = ephemeral.expect("the ephemeral buffer never drained to its own route");

    assert!(
        durable.body.contains(r#""durable":true"#),
        "{}",
        durable.body
    );
    assert!(
        ephemeral.body.contains(r#""ephemeral":true"#),
        "{}",
        ephemeral.body
    );
    // ...and neither family's payload reached the other's route.
    assert!(!durable.body.contains("ephemeral"), "{}", durable.body);
    assert!(!ephemeral.body.contains("durable"), "{}", ephemeral.body);
}

#[tokio::test]
async fn flush_buffer_drains_one_ephemeral_address() {
    // A high message_count means nothing flushes on its own; the explicit flush
    // is what puts the batch on the wire.
    let broker = FakeBroker::start(vec![created(r#"{"pushed":1}"#)]).await;
    let queen = client(&broker);

    queen
        .ephemeral()
        .push("presence", serde_json::json!({"n":1}))
        .buffered(BufferOptions {
            message_count: 1000,
            time: Duration::from_secs(60),
            ..Default::default()
        })
        .send()
        .await
        .expect("buffered push");
    assert_eq!(broker.hit_count(), 0, "nothing should have gone out yet");

    queen
        .ephemeral()
        .flush_buffer("presence", None)
        .await
        .expect("flush");

    let hit = one_hit(&broker).await;
    assert_eq!(hit.path, "/api/v1/ephemeral/push");
    assert_eq!(
        hit.body,
        r#"{"queue":"presence","messages":[{"payload":{"n":1}}]}"#
    );
}

#[tokio::test]
async fn close_drains_ephemeral_buffers_too() {
    // Buffered messages live in this process's memory. `close` is the one moment
    // the client promises to get them out, and an ephemeral buffer it did not
    // know about would be a silent loss on every orderly shutdown.
    let broker = FakeBroker::start(vec![created(r#"{"pushed":1}"#)]).await;
    let queen = client(&broker);

    queen
        .ephemeral()
        .push("presence", serde_json::json!({"n":1}))
        .buffered(BufferOptions {
            message_count: 1000,
            time: Duration::from_secs(60),
            ..Default::default()
        })
        .send()
        .await
        .expect("buffered push");

    queen.close().await.expect("close");

    let hit = one_hit(&broker).await;
    assert_eq!(hit.path, "/api/v1/ephemeral/push");
}

#[tokio::test]
async fn a_named_partition_is_a_different_buffer_from_an_unnamed_one() {
    // A push that named no partition is a DIFFERENT destination from any named
    // one, because the broker picks — merging the two into one buffer would
    // send a batch to a ring the caller never chose.
    let broker = FakeBroker::start(vec![created(r#"{"pushed":1}"#)]).await;
    let queen = client(&broker);

    let opts = BufferOptions {
        message_count: 1000,
        time: Duration::from_secs(60),
        ..Default::default()
    };
    queen
        .ephemeral()
        .push("presence", serde_json::json!(1))
        .buffered(opts)
        .send()
        .await
        .expect("push");
    queen
        .ephemeral()
        .push("presence", serde_json::json!(2))
        .partition("room-7")
        .buffered(opts)
        .send()
        .await
        .expect("push");

    assert_eq!(
        queen.buffer_stats().active_buffers,
        2,
        "one address per (queue, partition), and no partition is its own address"
    );
}
