//! What pop autopilot actually puts on the socket.
//!
//! These run against [`support::FakeBroker`] and need no stack. The four things
//! a client can be wrong about here, and why each is asserted against the WHOLE
//! query string rather than against one parameter:
//!
//!  1. THE PIN AND THE DELEGATION ARE DIFFERENT REQUESTS. `partitions(1)` and
//!     "never called partitions" both used to reach the wire as nothing at all;
//!     they are now different requests, and the pinned one must survive
//!     autopilot or a consumer that asked for one lane gets swept across many.
//!  2. NOT ENGAGING AUTOPILOT MUST BE BYTE-IDENTICAL TO THE OLD SDK. The escape
//!     hatch is only worth having if it is exact, and "exact" is not something a
//!     test of one parameter can show: a stray `autopilot=true`, or a `batch`
//!     that stopped being emitted, is a different request. Hence full-string
//!     equality, key order included.
//!  3. POP AND CONSUME MUST AGREE. This client renders both through one
//!     `PopParams`, unlike its siblings — the assertion is here so that stays
//!     true rather than being taken on trust.
//!  4. THE ADDITIVE RESPONSE FIELD MUST NOT BE LOAD-BEARING. A broker that does
//!     not send it, sends it half-filled, or sends it with fields this SDK has
//!     never heard of, all have to work — decoding a pop must never fail over a
//!     field nothing depends on.

mod support;

use std::time::Duration;

use queen_mq::{Config, Queen};
use support::{FakeBroker, Reply};

fn client(broker: &FakeBroker) -> Queen {
    Queen::connect(Config::new(broker.url())).expect("fake broker url")
}

/// A pop answer with one message, shaped like `render_pop_parts`.
fn one_message(extra: &str) -> Reply {
    Reply::ok(format!(
        concat!(
            r#"{{"success":true,"queue":"orders","partition":"Default","#,
            r#""partitionId":"0198f0aa-0000-7000-8000-0000000000e1","#,
            r#""leaseId":"0198f0aa-0000-7000-8000-00000000a001","#,
            r#""consumerGroup":"workers","messages":[{{"#,
            r#""id":"0198f0aa-0000-7000-8000-000000000001","transactionId":"t1","#,
            r#""traceId":null,"data":{{"n":1}},"producerSub":null,"#,
            r#""createdAt":"2026-08-24T09:15:00.000Z","#,
            r#""partitionId":"0198f0aa-0000-7000-8000-0000000000e1","partition":"Default","#,
            r#""leaseId":"0198f0aa-0000-7000-8000-00000000a001","consumerGroup":"workers"}}"#,
            r#"],"partitionsClaimed":1{}}}"#,
        ),
        extra
    ))
}

/// The query string of the first pop, exactly as it arrived.
async fn pop_query(build: impl FnOnce(queen_mq::QueueBuilder) -> queen_mq::QueueBuilder) -> String {
    let broker = FakeBroker::start(vec![one_message("")]).await;
    let q = build(client(&broker).queue("orders").group("workers").wait(false));
    q.pop().await.expect("pop");
    let hit = broker.hits().remove(0);
    hit.path
        .split_once('?')
        .map(|(_, q)| q.to_string())
        .unwrap_or_default()
}

// The shared spine of every case: a named queue and group, no long poll, default
// timeout. Everything that varies below is sizing.
const TAIL: &str = "wait=false&timeout=30000&consumerGroup=workers";

// ===========================================================================
// 1. Param assembly
// ===========================================================================

#[tokio::test]
async fn nothing_set_delegates_both_knobs_and_sends_neither() {
    assert_eq!(pop_query(|b| b).await, format!("autopilot=true&{TAIL}"));
}

#[tokio::test]
async fn a_pinned_width_travels_and_the_batch_is_delegated() {
    assert_eq!(
        pop_query(|b| b.partitions(4)).await,
        format!("autopilot=true&{TAIL}&partitions=4")
    );
}

/// The pin that used to be indistinguishable from unset. `partitions(1)` is a
/// decision — hold this consumer to one lane — and the broker has to be told, or
/// autopilot would widen it.
#[tokio::test]
async fn a_width_pinned_to_one_travels_under_autopilot() {
    assert_eq!(
        pop_query(|b| b.partitions(1)).await,
        format!("autopilot=true&{TAIL}&partitions=1")
    );
}

#[tokio::test]
async fn a_pinned_batch_travels_and_the_width_is_delegated() {
    assert_eq!(
        pop_query(|b| b.batch(50)).await,
        format!("autopilot=true&batch=50&{TAIL}")
    );
}

/// Both set: nothing left to decide, so no autopilot parameter and the exact
/// request the pre-autopilot SDK sent.
#[tokio::test]
async fn setting_both_knobs_sends_the_pre_autopilot_request() {
    assert_eq!(
        pop_query(|b| b.batch(50).partitions(4)).await,
        format!("batch=50&{TAIL}&partitions=4")
    );
    // ...including the one where the old SDK never emitted partitions=1.
    assert_eq!(
        pop_query(|b| b.batch(50).partitions(1)).await,
        format!("batch=50&{TAIL}")
    );
}

#[tokio::test]
async fn the_escape_hatch_restores_the_client_side_defaults() {
    assert_eq!(
        pop_query(|b| b.autopilot(false)).await,
        format!("batch=1&{TAIL}")
    );
    // A pin of 1 stays off the wire, exactly as before autopilot existed.
    assert_eq!(
        pop_query(|b| b.autopilot(false).partitions(1)).await,
        format!("batch=1&{TAIL}")
    );
    assert_eq!(
        pop_query(|b| b.autopilot(false).batch(50).partitions(4)).await,
        format!("batch=50&{TAIL}&partitions=4")
    );
}

/// `autopilot(true)` is the default spelled out. It must not change anything,
/// including for a caller who set both knobs.
#[tokio::test]
async fn spelling_out_the_default_changes_nothing() {
    assert_eq!(
        pop_query(|b| b.autopilot(true)).await,
        format!("autopilot=true&{TAIL}")
    );
    assert_eq!(
        pop_query(|b| b.autopilot(true).batch(50).partitions(4)).await,
        format!("batch=50&{TAIL}&partitions=4")
    );
}

/// `batch(0)` is not "a batch of zero" and never was: it is the absence of an
/// opinion, which now means the broker decides.
#[tokio::test]
async fn a_zero_batch_is_unset() {
    assert_eq!(pop_query(|b| b.batch(0)).await, format!("autopilot=true&{TAIL}"));
}

/// This client renders pop and consume through one `PopParams`; the assertion is
/// here so that stays true rather than being taken on trust.
#[tokio::test]
async fn consume_sends_the_same_query_as_pop() {
    let broker = FakeBroker::start(vec![one_message(""), Reply::ok("[]")]).await;
    let client = client(&broker);
    client
        .queue("orders")
        .group("workers")
        .wait(false)
        .partitions(1)
        .limit(1)
        .consume(|_msg| async { Ok::<(), queen_mq::Error>(()) })
        .await
        .expect("consume");

    let hits = broker.hits();
    let pop = hits
        .iter()
        .find(|h| h.route().starts_with("/api/v1/pop"))
        .expect("consume made no pop request");
    let query = pop.path.split_once('?').expect("a pop always has a query").1;
    assert_eq!(query, format!("autopilot=true&{TAIL}&partitions=1"));
}

// ===========================================================================
// 2. The additive response field
// ===========================================================================

#[tokio::test]
async fn pop_result_reports_what_the_broker_chose() {
    let broker =
        FakeBroker::start(vec![one_message(r#","autopilot":{"partitions":8,"batch":200,"waitMs":25}"#)])
            .await;
    let out = client(&broker)
        .queue("orders")
        .group("workers")
        .wait(false)
        .pop_result()
        .await
        .expect("pop");

    assert_eq!(out.messages.len(), 1);
    let ap = out.autopilot.expect("the broker sent an echo");
    assert_eq!(ap.partitions, 8);
    assert_eq!(ap.batch, 200);
    assert_eq!(ap.wait_ms, Some(25));
}

#[tokio::test]
async fn an_absent_echo_is_none_not_an_error() {
    let broker = FakeBroker::start(vec![one_message("")]).await;
    let out = client(&broker)
        .queue("orders")
        .group("workers")
        .wait(false)
        .pop_result()
        .await
        .expect("a broker older than 1.2 is not an error");

    assert_eq!(out.messages.len(), 1);
    assert!(out.autopilot.is_none());
}

/// Forward compatibility, and it is not a nicety: refusing to decode a pop
/// because a newer broker grew a field — or spelled one of these as a string —
/// would stop a consumer over a value nothing depends on.
#[tokio::test]
async fn an_echo_this_client_does_not_understand_never_fails_the_pop() {
    for extra in [
        r#","autopilot":{"partitions":2,"batch":10,"waitMs":5,"reason":"ready_age"}"#,
        r#","autopilot":{"partitions":"eight","batch":10}"#,
        r#","autopilot":null"#,
        r#","autopilot":true"#,
        r#","autopilot":[]"#,
    ] {
        let broker = FakeBroker::start(vec![one_message(extra)]).await;
        let out = client(&broker)
            .queue("orders")
            .group("workers")
            .wait(false)
            .pop_result()
            .await
            .unwrap_or_else(|e| panic!("{extra} must not fail the pop: {e}"));
        assert_eq!(out.messages.len(), 1, "{extra}");
    }

    // ...and the fields it DOES understand survive an unknown neighbour.
    let broker = FakeBroker::start(vec![one_message(
        r#","autopilot":{"partitions":2,"batch":10,"waitMs":5,"reason":"ready_age"}"#,
    )])
    .await;
    let out = client(&broker)
        .queue("orders")
        .group("workers")
        .wait(false)
        .pop_result()
        .await
        .expect("pop");
    let ap = out.autopilot.expect("echo");
    assert_eq!((ap.partitions, ap.batch, ap.wait_ms), (2, 10, Some(5)));

    // A field of the wrong type is dropped, not fatal.
    let broker = FakeBroker::start(vec![one_message(r#","autopilot":{"partitions":"eight","batch":10}"#)])
        .await;
    let out = client(&broker)
        .queue("orders")
        .group("workers")
        .wait(false)
        .pop_result()
        .await
        .expect("pop");
    let ap = out.autopilot.expect("echo");
    assert_eq!((ap.partitions, ap.batch, ap.wait_ms), (0, 10, None));
}

// ===========================================================================
// 3. Empty-poll pacing
// ===========================================================================

/// The advice replaces the loop's own delay between empty NON-waiting pops. It
/// shows only in the gap between two pops, so the assertion is a comparison
/// against the same loop with no advice: an absolute pop count would be a claim
/// about the machine, this is a claim about the client.
#[tokio::test]
async fn the_brokers_pacing_advice_replaces_the_historical_delay() {
    async fn pops_in_a_window(echo: &str) -> usize {
        let body = format!(
            concat!(
                r#"{{"success":true,"queue":"orders","partition":"","partitionId":"","#,
                r#""leaseId":"","consumerGroup":"workers","messages":[],"#,
                r#""partitionsClaimed":0{}}}"#,
            ),
            echo
        );
        let broker = FakeBroker::start(vec![Reply::ok(body)]).await;
        let cancel = queen_mq::Cancel::new();
        let c = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(600)).await;
            c.cancel();
        });

        client(&broker)
            .queue("orders")
            .group("workers")
            .wait(false)
            .cancel(cancel)
            .consume(|_msg| async { Ok::<(), queen_mq::Error>(()) })
            .await
            .expect("consume");

        broker.hits().len()
    }

    let advised = pops_in_a_window(r#","autopilot":{"partitions":1,"batch":100,"waitMs":400}"#).await;
    let historical = pops_in_a_window("").await;

    assert!(
        advised < historical,
        "a 400ms advice must slow the loop below its own 100ms default: \
         {advised} pops advised vs {historical} unadvised"
    );
    assert!(advised >= 1, "the loop has to poll at least once");
}
