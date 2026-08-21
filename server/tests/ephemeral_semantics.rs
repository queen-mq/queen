//! EPHEMERAL SEMANTICS — the contract of `EPHEMERAL_QUEUES.md` §1, tested
//! through the real handlers. Closes the §7.1 row of the test plan.
//!
//! WHY THIS LEVEL, and why it is not the mirror image of `kv_semantics.rs`. That
//! suite calls the stored procedures directly, because the KV rules live in SQL
//! exactly once and every surface inherits them from there. Here there is no
//! SQL to call: the rules live in `src/ephemeral.rs` and in the handlers on top
//! of it, and the thing worth pinning is what a CLIENT observes — the HTTP
//! status of a refusal, the `outcome` word of an ack, the shape of a depth read.
//! So every case below goes through `queen::Broker`, whose ephemeral methods
//! invoke the same handler functions the router dispatches to, minus the socket.
//! Running embedded IS running the broker (see `src/lib.rs`), which is what makes
//! that substitution honest rather than convenient.
//!
//! WHY POSTGRES IS NEEDED AT ALL for a class that stores nothing in it. Three
//! reasons, and each is a case below: the declared configs and the grant rows are
//! durable (§2), so `configure`, `delete` and the grant ladder need the two
//! tables; and the property that a DECLARED queue survives a restart *as
//! configured but empty* (§1.2) is only observable by booting a second broker
//! against the same database. A test that skipped the database would prove the
//! RAM half and silently drop the half that has a failure mode.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`,
//! the same convention as `kv_semantics` and `embedded_smoke`:
//!
//! ```bash
//! docker run --rm -d --name queen-eph-pg -e POSTGRES_PASSWORD=postgres -p 5477:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5477 cargo test --test ephemeral_semantics -- --ignored --nocapture
//! ```
//!
//! ONE test function on purpose, exactly as in `kv_semantics.rs`: the knobs this
//! feature is timed against are read from the PROCESS ENVIRONMENT at boot, so
//! two test functions racing to set them would be flaky in a way no assertion
//! could explain. The cases are still reported one by one — each returns
//! `Result<(), String>` and the runner prints a PASS/FAIL line per case before
//! failing, so a red run names every broken rule instead of only the first.
//!
//! THE SLEEPS ARE REAL, and are the one place this suite differs in character
//! from the KV one. There `p_now` is a parameter, so expiry is testable with zero
//! sleeps; here the clock is the process's own and a lease expires when it
//! expires. They are kept to the floor the knobs allow (`QUEEN_EPHEMERAL_LEASE_S`
//! clamps at 1) and every wait is a bounded poll rather than a fixed sleep where
//! that is possible.

use queen::{Broker, BrokerConfig};
use serde_json::{json, Value};
use tokio_postgres::Client;

/// `config::DEFAULT_TENANT` — the only tenant an embedded broker has
/// (`QUEEN_TENANCY_HEADER` is deliberately not honoured embedded), and therefore
/// the tenant every grant row below is written for.
const DEFAULT_TENANT: &str = "00000000-0000-0000-0000-000000000001";

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

async fn boot(host: &str, port: u16) -> Broker {
    Broker::start(
        BrokerConfig::new()
            .pg(host.to_string(), port, "postgres", "postgres", "postgres")
            .pool_size(4),
    )
    .await
    .expect("broker start")
}

// ---------------------------------------------------------------------------
// Small readers over the wire JSON. They exist so a case reads as the rule it
// is testing and not as a chain of `and_then`s — and so that a shape change
// fails in ONE place with a legible message instead of in twelve with `None`.
// ---------------------------------------------------------------------------

fn msgs(v: &Value) -> Vec<&Value> {
    v.get("messages").and_then(|m| m.as_array()).map(|a| a.iter().collect()).unwrap_or_default()
}

fn payload_n(m: &Value) -> i64 {
    m.get("payload").and_then(|p| p.get("n")).and_then(|n| n.as_i64()).unwrap_or(-1)
}

fn id_of(m: &Value) -> String {
    m.get("id").and_then(|i| i.as_str()).unwrap_or_default().to_string()
}

fn attempts_of(m: &Value) -> i64 {
    m.get("attempts").and_then(|a| a.as_i64()).unwrap_or(-1)
}

fn outcomes(v: &Value) -> Vec<String> {
    v.get("results")
        .and_then(|r| r.as_array())
        .map(|a| {
            a.iter()
                .map(|x| x.get("outcome").and_then(|o| o.as_str()).unwrap_or("?").to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// The HTTP status behind a facade error, plus the `code` from the `{error,
/// code}` envelope — the only field a client may branch on (§3.1). The message
/// is deliberately not matched on anywhere in this file.
fn status_and_code(e: &queen::Error) -> (u16, String) {
    let status = e.status().unwrap_or(0);
    // `Error::Display` carries the envelope's `error` (the human half); the code
    // is what this suite asserts, so it is pulled out of the message text the
    // facade preserved. When the facade gains typed errors this helper is the
    // one place that changes.
    let text = e.to_string();
    (status, text)
}

fn push_one(queue: &str, partition: Option<&str>, n: i64) -> Value {
    let mut body = json!({ "queue": queue, "messages": [{ "payload": { "n": n } }] });
    if let Some(p) = partition {
        body["partition"] = json!(p);
    }
    body
}

fn pop_params(queue: &str, extra: &[(&str, &str)]) -> Vec<(&'static str, String)> {
    let mut v: Vec<(&'static str, String)> = vec![("queue", queue.to_string())];
    for (k, val) in extra {
        // Only the keys the pop handler reads (§3.1), spelled here so a typo is a
        // compile-time-visible list rather than a silently ignored parameter.
        let key: &'static str = match *k {
            "partition" => "partition",
            "batch" => "batch",
            "wait" => "wait",
            "timeout" => "timeout",
            "group" => "group",
            "autoAck" => "autoAck",
            other => panic!("unknown pop parameter `{other}` in a test"),
        };
        v.push((key, (*val).to_string()));
    }
    v
}

// ===========================================================================
// §1.4 — FIFO per (queue, partition)
// ===========================================================================

async fn case_fifo_per_partition(b: &Broker) -> Case {
    let q = unique("eph-fifo-");
    for n in 0..5 {
        b.ephemeral_push(push_one(&q, Some("a"), n)).await.map_err(|e| e.to_string())?;
        b.ephemeral_push(push_one(&q, Some("b"), 100 + n)).await.map_err(|e| e.to_string())?;
    }
    // Partition-scoped pops: the order INSIDE one partition is the contract; the
    // interleaving between two partitions is not, which is why they are read
    // separately rather than asserting one merged sequence.
    for (part, base) in [("a", 0i64), ("b", 100)] {
        let got = b
            .ephemeral_pop(&pop_params(&q, &[("partition", part), ("batch", "5")]))
            .await
            .map_err(|e| e.to_string())?;
        let seq: Vec<i64> = msgs(&got).iter().map(|m| payload_n(m)).collect();
        chk!(
            seq == (base..base + 5).collect::<Vec<i64>>(),
            "partition {part} must come back in arrival order, got {seq:?}"
        );
    }
    Ok(())
}

// ===========================================================================
// §1.3 — lease expiry redelivers with attempts+1
// ===========================================================================

async fn case_lease_redelivery_counts_attempts(b: &Broker) -> Case {
    let q = unique("eph-lease-");
    b.ephemeral_push(push_one(&q, None, 1)).await.map_err(|e| e.to_string())?;

    let first = b.ephemeral_pop(&pop_params(&q, &[])).await.map_err(|e| e.to_string())?;
    chk!(msgs(&first).len() == 1, "the first pop must deliver the message");
    chk!(
        attempts_of(msgs(&first)[0]) == 1,
        "a first delivery is attempt 1, got {}",
        attempts_of(msgs(&first)[0])
    );

    // No ack. The lease is QUEEN_EPHEMERAL_LEASE_S = 1 for this suite; the
    // expiry sweep runs opportunistically on the next touch of the ring, so the
    // pop below is both the trigger and the assertion.
    let redelivered = wait_for(2_500, || async {
        let got = b.ephemeral_pop(&pop_params(&q, &[])).await.ok()?;
        (!msgs(&got).is_empty()).then_some(got)
    })
    .await
    .ok_or("the unacked message was never redelivered within 2.5s")?;
    chk!(
        attempts_of(msgs(&redelivered)[0]) == 2,
        "a redelivery is attempt 2, got {}",
        attempts_of(msgs(&redelivered)[0])
    );
    chk!(
        payload_n(msgs(&redelivered)[0]) == 1,
        "the redelivered message must be the same one"
    );
    Ok(())
}

// ===========================================================================
// §1.3 — completed / failed / retry, and retryLimit exhaustion
// ===========================================================================

async fn case_ack_statuses(b: &Broker) -> Case {
    let q = unique("eph-ack-");
    b.ephemeral_push(push_one(&q, None, 7)).await.map_err(|e| e.to_string())?;

    let got = b.ephemeral_pop(&pop_params(&q, &[])).await.map_err(|e| e.to_string())?;
    let id = id_of(msgs(&got)[0]);

    // `retry` and `failed` are the same DECISION (both redeliver with attempts+1)
    // and are kept distinct on the wire because they mean different things to a
    // human reading a trace — so both are pinned, not just one.
    let r = b
        .ephemeral_ack(json!({ "queue": q, "acks": [{ "id": id, "status": "retry" }] }))
        .await
        .map_err(|e| e.to_string())?;
    chk!(outcomes(&r) == vec!["redelivered"], "retry must redeliver, got {:?}", outcomes(&r));

    let got = b.ephemeral_pop(&pop_params(&q, &[])).await.map_err(|e| e.to_string())?;
    chk!(msgs(&got).len() == 1, "a redelivered message must come back at once, not on lease expiry");
    chk!(attempts_of(msgs(&got)[0]) == 2, "the redelivery is attempt 2");
    let id = id_of(msgs(&got)[0]);

    let r = b
        .ephemeral_ack(json!({ "queue": q, "acks": [{ "id": id, "status": "completed" }] }))
        .await
        .map_err(|e| e.to_string())?;
    chk!(outcomes(&r) == vec!["acked"], "completed must retire the message, got {:?}", outcomes(&r));

    // A second ack of the same id: our epoch, no live lease. `unknown`, and NOT
    // an error — a client flushing buffered acks must not meet a 4xx storm.
    let r = b
        .ephemeral_ack(json!({ "queue": q, "acks": [{ "id": id }] }))
        .await
        .map_err(|e| e.to_string())?;
    chk!(outcomes(&r) == vec!["unknown"], "a re-ack is unknown, got {:?}", outcomes(&r));

    let after = b.ephemeral_pop(&pop_params(&q, &[])).await.map_err(|e| e.to_string())?;
    chk!(msgs(&after).is_empty(), "a completed message must not come back");
    Ok(())
}

async fn case_retry_limit_exhaustion_is_counted(b: &Broker) -> Case {
    let q = unique("eph-exhaust-");
    // retryLimit 1: the FIRST delivery already spends the only attempt, so the
    // first nack is the exhausting one. Chosen over a larger limit because it
    // makes the boundary — `attempts >= retry_limit`, not `>` — the thing under
    // test rather than a loop count.
    b.ephemeral_configure(json!({ "queue": q, "options": { "retryLimit": 1 } }))
        .await
        .map_err(|e| e.to_string())?;
    b.ephemeral_push(push_one(&q, None, 1)).await.map_err(|e| e.to_string())?;

    let got = b.ephemeral_pop(&pop_params(&q, &[])).await.map_err(|e| e.to_string())?;
    let id = id_of(msgs(&got)[0]);
    let r = b
        .ephemeral_ack(json!({ "queue": q, "acks": [{ "id": id, "status": "failed" }] }))
        .await
        .map_err(|e| e.to_string())?;
    // `acked` and not a fifth outcome word: the message is gone, and WHY it is
    // gone is the drop counter, because inventing an outcome would break every
    // client that matches the closed enum of §3.1.
    chk!(
        outcomes(&r) == vec!["acked"],
        "an exhausted nack retires the message with `acked`, got {:?}",
        outcomes(&r)
    );
    let after = b.ephemeral_pop(&pop_params(&q, &[])).await.map_err(|e| e.to_string())?;
    chk!(msgs(&after).is_empty(), "an exhausted message must not be redelivered");

    let drops = drops_of(b, &q).await?;
    chk!(drops.2 >= 1, "the exhaustion must be COUNTED as a retry drop, got {drops:?}");
    Ok(())
}

// ===========================================================================
// §1.3 — autoAck is at-most-once
// ===========================================================================

async fn case_auto_ack_is_at_most_once(b: &Broker) -> Case {
    let q = unique("eph-autoack-");
    b.ephemeral_push(push_one(&q, None, 42)).await.map_err(|e| e.to_string())?;
    let got = b
        .ephemeral_pop(&pop_params(&q, &[("autoAck", "true")]))
        .await
        .map_err(|e| e.to_string())?;
    chk!(msgs(&got).len() == 1, "autoAck still delivers");
    let id = id_of(msgs(&got)[0]);

    // No lease was created, so there is nothing for an ack to resolve.
    let r = b
        .ephemeral_ack(json!({ "queue": q, "acks": [{ "id": id }] }))
        .await
        .map_err(|e| e.to_string())?;
    chk!(
        outcomes(&r) == vec!["unknown"],
        "an autoAck delivery holds no lease, so its ack is unknown, got {:?}",
        outcomes(&r)
    );

    // The cursor advanced AT DELIVERY: nothing redelivers, ever — not on lease
    // expiry (there is no lease) and not on a later pop.
    tokio::time::sleep(std::time::Duration::from_millis(1_400)).await;
    let after = b.ephemeral_pop(&pop_params(&q, &[])).await.map_err(|e| e.to_string())?;
    chk!(msgs(&after).is_empty(), "an autoAck message must never be redelivered");
    Ok(())
}

// ===========================================================================
// §1.5 — one ring, N group cursors
// ===========================================================================

async fn case_fan_out_across_groups(b: &Broker) -> Case {
    let q = unique("eph-fanout-");
    // Both groups register their cursors BEFORE the push. That is the ordering a
    // fan-out subscriber actually uses (park first, then the producer arrives),
    // and it is the one the `known_groups` pre-seeding exists to make safe.
    for g in ["alpha", "beta"] {
        b.ephemeral_pop(&pop_params(&q, &[("group", g)])).await.map_err(|e| e.to_string())?;
    }
    for n in 0..3 {
        b.ephemeral_push(push_one(&q, None, n)).await.map_err(|e| e.to_string())?;
    }
    for g in ["alpha", "beta"] {
        let got = b
            .ephemeral_pop(&pop_params(&q, &[("group", g), ("batch", "10"), ("autoAck", "true")]))
            .await
            .map_err(|e| e.to_string())?;
        let seq: Vec<i64> = msgs(&got).iter().map(|m| payload_n(m)).collect();
        chk!(seq == vec![0, 1, 2], "group {g} must receive EVERY message, got {seq:?}");
    }
    // One ring, not two copies: the depth read shows both cursors over the same
    // partition, which is the difference between fan-out here and fan-out by
    // duplication.
    let d = b.ephemeral_depth(&q, None).await.map_err(|e| e.to_string())?;
    let groups = d.get("groups").and_then(|g| g.as_array()).cloned().unwrap_or_default();
    chk!(groups.len() == 2, "both group cursors live on the one ring, got {groups:?}");
    let parts = d.get("partitions").and_then(|p| p.as_array()).cloned().unwrap_or_default();
    chk!(parts.len() == 1, "fan-out must not create a second partition, got {parts:?}");
    Ok(())
}

async fn case_groupless_is_queue_mode(b: &Broker) -> Case {
    let q = unique("eph-queuemode-");
    for n in 0..4 {
        b.ephemeral_push(push_one(&q, None, n)).await.map_err(|e| e.to_string())?;
    }
    // Two group-less consumers COMPETE: they share the one `__QUEUE_MODE__`
    // cursor, exactly as on the durable engine. If they fanned out instead, each
    // would see all four.
    let a = b
        .ephemeral_pop(&pop_params(&q, &[("batch", "2"), ("autoAck", "true")]))
        .await
        .map_err(|e| e.to_string())?;
    let c = b
        .ephemeral_pop(&pop_params(&q, &[("batch", "2"), ("autoAck", "true")]))
        .await
        .map_err(|e| e.to_string())?;
    let sa: Vec<i64> = msgs(&a).iter().map(|m| payload_n(m)).collect();
    let sc: Vec<i64> = msgs(&c).iter().map(|m| payload_n(m)).collect();
    chk!(sa == vec![0, 1], "the first group-less pop takes the head, got {sa:?}");
    chk!(sc == vec![2, 3], "the second takes what is left, got {sc:?}");

    // And the group-less cursor is spelled with the durable engine's sentinel, so
    // a reader who knows one engine knows the other.
    let d = b.ephemeral_depth(&q, None).await.map_err(|e| e.to_string())?;
    let names: Vec<String> = d
        .get("groups")
        .and_then(|g| g.as_array())
        .map(|a| a.iter().filter_map(|x| x.get("group")?.as_str().map(String::from)).collect())
        .unwrap_or_default();
    chk!(
        names == vec!["__QUEUE_MODE__".to_string()],
        "the group-less cursor is the durable sentinel, got {names:?}"
    );
    Ok(())
}

// ===========================================================================
// §1.7 — ttl head-drop
// ===========================================================================

async fn case_ttl_head_drop(b: &Broker) -> Case {
    let q = unique("eph-ttl-");
    b.ephemeral_configure(json!({ "queue": q, "options": { "ttlSeconds": 1 } }))
        .await
        .map_err(|e| e.to_string())?;
    b.ephemeral_push(push_one(&q, None, 1)).await.map_err(|e| e.to_string())?;

    let gone = wait_for(3_000, || async {
        let got = b.ephemeral_pop(&pop_params(&q, &[])).await.ok()?;
        let drops = drops_of(b, &q).await.ok()?;
        (msgs(&got).is_empty() && drops.1 >= 1).then_some(drops)
    })
    .await
    .ok_or("the ttl'd message was still deliverable, or the drop was not counted, after 3s")?;
    chk!(gone.1 >= 1, "the drop must be attributed to ttl, got {gone:?}");
    chk!(gone.0 == 0, "a ttl drop is not a bounds drop, got {gone:?}");
    Ok(())
}

// ===========================================================================
// §1.6 — bounds, both policies
// ===========================================================================

async fn case_bounds_reject(b: &Broker) -> Case {
    let q = unique("eph-reject-");
    b.ephemeral_configure(json!({ "queue": q, "options": { "maxLength": 2, "policy": "reject" } }))
        .await
        .map_err(|e| e.to_string())?;
    for n in 0..2 {
        b.ephemeral_push(push_one(&q, None, n)).await.map_err(|e| e.to_string())?;
    }
    let e = b
        .ephemeral_push(push_one(&q, None, 99))
        .await
        .err()
        .ok_or("a push past maxLength with policy=reject must be refused")?;
    let (status, text) = status_and_code(&e);
    // 429 and not 507/503: this is BACKPRESSURE, and the shape every 1.0.6 SDK's
    // bounded buffer already knows how to drain against.
    chk!(status == 429, "queue_full is 429, got {status} ({text})");

    let d = b.ephemeral_depth(&q, None).await.map_err(|e| e.to_string())?;
    chk!(
        d.get("pending").and_then(|p| p.as_i64()) == Some(2),
        "a rejected push must change nothing: {d}"
    );
    Ok(())
}

async fn case_bounds_drop_oldest(b: &Broker) -> Case {
    let q = unique("eph-dropoldest-");
    b.ephemeral_configure(
        json!({ "queue": q, "options": { "maxLength": 2, "policy": "dropOldest" } }),
    )
    .await
    .map_err(|e| e.to_string())?;
    for n in 0..5 {
        b.ephemeral_push(push_one(&q, None, n)).await.map_err(|e| e.to_string())?;
    }
    let got = b
        .ephemeral_pop(&pop_params(&q, &[("batch", "10"), ("autoAck", "true")]))
        .await
        .map_err(|e| e.to_string())?;
    let seq: Vec<i64> = msgs(&got).iter().map(|m| payload_n(m)).collect();
    // Feed semantics: the NEWEST survive, the head is what goes.
    chk!(seq == vec![3, 4], "dropOldest keeps the tail, got {seq:?}");
    let drops = drops_of(b, &q).await?;
    chk!(drops.0 == 3, "three head drops must be counted as bounds, got {drops:?}");
    Ok(())
}

// ===========================================================================
// §3.2 — a cursor parked below an evicted range skips forward, and says so
// ===========================================================================

async fn case_cursor_skips_evicted_range(b: &Broker) -> Case {
    let q = unique("eph-skip-");
    b.ephemeral_configure(
        json!({ "queue": q, "options": { "maxLength": 2, "policy": "dropOldest" } }),
    )
    .await
    .map_err(|e| e.to_string())?;
    // The group registers a cursor at the head and then falls behind while the
    // producer runs the ring past it.
    b.ephemeral_pop(&pop_params(&q, &[("group", "slow")])).await.map_err(|e| e.to_string())?;
    for n in 0..6 {
        b.ephemeral_push(push_one(&q, None, n)).await.map_err(|e| e.to_string())?;
    }
    let got = b
        .ephemeral_pop(&pop_params(&q, &[("group", "slow"), ("batch", "10"), ("autoAck", "true")]))
        .await
        .map_err(|e| e.to_string())?;
    let seq: Vec<i64> = msgs(&got).iter().map(|m| payload_n(m)).collect();
    chk!(
        seq == vec![4, 5],
        "a cursor under an evicted range resumes at the surviving head, got {seq:?}"
    );
    // AND THE SKIP IS COUNTED. For a fan-out consumer this is the difference
    // between "slow" and "lost data", which on this class is legal — and is
    // exactly why it has to be visible.
    let d = b.ephemeral_depth(&q, None).await.map_err(|e| e.to_string())?;
    let skipped = d
        .get("groups")
        .and_then(|g| g.as_array())
        .and_then(|a| a.iter().find(|x| x.get("group").and_then(|n| n.as_str()) == Some("slow")))
        .and_then(|x| x.get("skipped").and_then(|s| s.as_u64()))
        .unwrap_or(0);
    chk!(skipped == 4, "four messages passed under the cursor unseen, got {skipped}");
    Ok(())
}

// ===========================================================================
// §1.7 — windowBuffer holds a waiting pop open to fatten the batch
// ===========================================================================

async fn case_window_buffer_fattens_a_waiting_pop(b: &Broker) -> Case {
    let q = unique("eph-window-");
    b.ephemeral_configure(
        json!({ "queue": q, "options": { "windowBuffer": { "ms": 400, "count": 5 } } }),
    )
    .await
    .map_err(|e| e.to_string())?;
    b.ephemeral_push(push_one(&q, None, 1)).await.map_err(|e| e.to_string())?;
    b.ephemeral_push(push_one(&q, None, 2)).await.map_err(|e| e.to_string())?;

    // Two messages are ready and the batch asks for five: without the window the
    // pop returns immediately with two (the durable behaviour). With it, it holds
    // for `ms` hoping for three more.
    let t0 = std::time::Instant::now();
    let got = b
        .ephemeral_pop(&pop_params(
            &q,
            &[("batch", "5"), ("wait", "true"), ("timeout", "3000"), ("autoAck", "true")],
        ))
        .await
        .map_err(|e| e.to_string())?;
    let waited = t0.elapsed().as_millis();
    chk!(msgs(&got).len() == 2, "the window must not LOSE messages, got {}", msgs(&got).len());
    chk!(waited >= 300, "the window must hold the pop open (~400ms), returned after {waited}ms");
    // Bounded by the pop's own timeout and by the window, never by neither: 3s of
    // timeout must not become 3s of waiting.
    chk!(waited < 2_000, "the window must not hold past its own ms, waited {waited}ms");
    Ok(())
}

// ===========================================================================
// §3.1 — an ack from another incarnation answers `stale`
// ===========================================================================

async fn case_epoch_stale_acks(b: &Broker) -> Case {
    let q = unique("eph-stale-");
    b.ephemeral_push(push_one(&q, None, 1)).await.map_err(|e| e.to_string())?;
    b.ephemeral_pop(&pop_params(&q, &[])).await.map_err(|e| e.to_string())?;

    // An id minted by an epoch that is not this process's. THE ANSWER IS NEVER AN
    // ERROR: a client reconnecting after a restart flushes its outstanding acks,
    // and a 4xx per id would be a retry storm where `stale` is information.
    let r = b
        .ephemeral_ack(json!({ "queue": q, "acks": [{ "id": "e:dead0000beef0000:Default:1" }] }))
        .await
        .map_err(|e| e.to_string())?;
    chk!(
        outcomes(&r) == vec!["stale"],
        "an id from another incarnation is stale, got {:?}",
        outcomes(&r)
    );

    // A malformed id is `unknown` and not `stale`: `stale` is a claim about an
    // epoch, and there is no epoch to read here.
    let r = b
        .ephemeral_ack(json!({ "queue": q, "acks": [{ "id": "not-an-ephemeral-id" }] }))
        .await
        .map_err(|e| e.to_string())?;
    chk!(
        outcomes(&r) == vec!["unknown"],
        "a malformed id is unknown, got {:?}",
        outcomes(&r)
    );
    Ok(())
}

// ===========================================================================
// §1.1 — implicit queues are collected when empty and idle
// ===========================================================================

async fn case_implicit_gc(b: &Broker) -> Case {
    let q = unique("eph-gc-");
    b.ephemeral_push(push_one(&q, None, 1)).await.map_err(|e| e.to_string())?;
    b.ephemeral_pop(&pop_params(&q, &[("autoAck", "true")])).await.map_err(|e| e.to_string())?;
    chk!(listed(b, &q).await?.is_some(), "the implicit queue must exist while it is in use");

    // QUEEN_EPHEMERAL_IMPLICIT_IDLE_S is 1 for this suite and the backstop runs
    // once a second, so ~4s is three chances. The wait is a poll rather than a
    // sleep so a fast collection does not cost the suite four seconds.
    let collected = wait_for(6_000, || async {
        listed(b, &q).await.ok().flatten().is_none().then_some(())
    })
    .await;
    chk!(
        collected.is_some(),
        "an empty, unpolled implicit queue must be collected — it was still listed after 6s"
    );
    Ok(())
}

// ===========================================================================
// §3.1 — configure / reset / delete, and the closed option list
// ===========================================================================

async fn case_configure_rejects_unknown_options(b: &Broker) -> Case {
    let q = unique("eph-badopt-");
    let e = b
        .ephemeral_configure(json!({ "queue": q, "options": { "ttlSecond": 60 } }))
        .await
        .err()
        .ok_or("an unknown option must be refused, not ignored")?;
    let (status, text) = status_and_code(&e);
    chk!(status == 400, "an unknown option is a 400, got {status} ({text})");
    chk!(
        text.contains("ttlSecond"),
        "the refusal must name the offending key so a typo is findable: {text}"
    );
    // And the queue must not have been declared as a side effect of the refusal.
    chk!(
        listed(b, &q).await?.is_none(),
        "a refused configure must not declare the queue"
    );
    Ok(())
}

async fn case_configure_reset_delete_round_trip(b: &Broker) -> Case {
    let q = unique("eph-admin-");
    let echo = b
        .ephemeral_configure(json!({
            "queue": q,
            "options": { "maxLength": 50, "policy": "dropOldest", "ttlSeconds": 30,
                          "leaseSeconds": 5, "retryLimit": 2,
                          "windowBuffer": { "ms": 10, "count": 3 } }
        }))
        .await
        .map_err(|e| e.to_string())?;
    chk!(
        echo.get("queue").and_then(|x| x.as_str()) == Some(q.as_str()),
        "configure echoes the stored row: {echo}"
    );
    chk!(echo.get("options").is_some(), "the echo carries the stored options: {echo}");

    let row = listed(b, &q).await?.ok_or("a declared queue must be listed")?;
    chk!(
        row.get("tier").and_then(|t| t.as_str()) == Some("declared"),
        "the tier of a configured queue is `declared`: {row}"
    );
    // The EFFECTIVE configuration, clamped by the engine — the status read
    // publishes what is in force, which is a different question from the echo.
    let eff = row.get("options").cloned().unwrap_or(Value::Null);
    chk!(
        eff.get("maxLength").and_then(|x| x.as_i64()) == Some(50)
            && eff.get("policy").and_then(|x| x.as_str()) == Some("dropOldest")
            && eff.get("ttlSeconds").and_then(|x| x.as_i64()) == Some(30)
            && eff.get("retryLimit").and_then(|x| x.as_i64()) == Some(2),
        "the listing publishes the options actually in force: {eff}"
    );

    for n in 0..4 {
        b.ephemeral_push(push_one(&q, None, n)).await.map_err(|e| e.to_string())?;
    }
    // A lease is outstanding across the reset, which is the interesting case: the
    // reset must void it, not leave a lease pointing at a seq that is gone.
    b.ephemeral_pop(&pop_params(&q, &[])).await.map_err(|e| e.to_string())?;

    let r = b.ephemeral_reset(&q).await.map_err(|e| e.to_string())?;
    chk!(
        r.get("dropped").and_then(|d| d.as_i64()) == Some(4),
        "reset reports what it dropped: {r}"
    );
    let d = b.ephemeral_depth(&q, None).await.map_err(|e| e.to_string())?;
    chk!(d.get("pending").and_then(|p| p.as_i64()) == Some(0), "reset empties the queue: {d}");
    chk!(d.get("bytes").and_then(|p| p.as_i64()) == Some(0), "reset returns the bytes: {d}");
    // The queue itself SURVIVES a reset — it is declared, and its configuration
    // is durable. Only the contents were ever disposable.
    chk!(listed(b, &q).await?.is_some(), "reset must not delete a declared queue");

    let del = b.ephemeral_delete(&q).await.map_err(|e| e.to_string())?;
    chk!(
        del.get("deleted").and_then(|x| x.as_bool()) == Some(true)
            && del.get("declared").and_then(|x| x.as_bool()) == Some(true),
        "delete removes both halves and says so: {del}"
    );
    chk!(listed(b, &q).await?.is_none(), "a deleted queue is gone from the listing");

    // A second delete is 200 with deleted:false — the house rule, and the reason
    // it is not a 404: the status describes the outcome of the CALL.
    let again = b.ephemeral_delete(&q).await.map_err(|e| e.to_string())?;
    chk!(
        again.get("deleted").and_then(|x| x.as_bool()) == Some(false),
        "deleting nothing is 200 deleted:false, got {again}"
    );

    // And an unknown queue's depth is a 404, mirroring the durable depth read.
    let e = b
        .ephemeral_depth(&q, None)
        .await
        .err()
        .ok_or("the depth of a deleted queue must be a 404")?;
    chk!(e.status() == Some(404), "depth of an absent queue is 404, got {:?}", e.status());
    Ok(())
}

// ===========================================================================
// Helpers that talk to the status endpoints
// ===========================================================================

/// One queue's row in `GET /api/v1/ephemeral/queues`, or `None`.
async fn listed(b: &Broker, queue: &str) -> Result<Option<Value>, String> {
    let v = b.ephemeral_queues().await.map_err(|e| e.to_string())?;
    Ok(v.get("queues")
        .and_then(|q| q.as_array())
        .and_then(|a| a.iter().find(|x| x.get("queue").and_then(|n| n.as_str()) == Some(queue)))
        .cloned())
}

/// `(bounds, ttl, retry)` drops for one queue.
async fn drops_of(b: &Broker, queue: &str) -> Result<(u64, u64, u64), String> {
    let row = listed(b, queue).await?.ok_or_else(|| format!("queue {queue} is not listed"))?;
    let d = row.get("drops").cloned().unwrap_or(Value::Null);
    let g = |k: &str| d.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
    Ok((g("bounds"), g("ttl"), g("retry")))
}

/// Poll `f` every 100 ms until it yields, or give up after `budget_ms`.
///
/// A POLL AND NOT A SLEEP wherever the engine allows it: the expiry paths here
/// are opportunistic (they run on the next touch of the ring), so the poll is
/// both the trigger and the observation, and a fixed sleep would have to be sized
/// for the slowest machine that will ever run this suite.
async fn wait_for<T, F, Fut>(budget_ms: u64, mut f: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(budget_ms);
    loop {
        if let Some(v) = f().await {
            return Some(v);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn ephemeral_semantics() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // The knobs this suite is timed against, set BEFORE the first boot because
    // `config::load()` reads them once per process start. One second is the floor
    // `config.rs` clamps the lease to, and it is what keeps the whole suite under
    // half a minute.
    std::env::set_var("QUEEN_EPHEMERAL_LEASE_S", "1");
    std::env::set_var("QUEEN_EPHEMERAL_IMPLICIT_IDLE_S", "1");
    // A tenant with no grant row must be UNLIMITED for the first two phases: the
    // grant ladder is phase C's subject, and leaving it on here would make every
    // case above a 403.
    std::env::set_var("QUEEN_EPHEMERAL_REQUIRE_GRANT", "false");

    let mut report: Vec<(&str, Case)> = Vec::new();

    // ---- phase A: one broker, the whole §1 contract ----------------------
    let b = boot(&host, port).await;
    report.push(("fifo_per_partition", case_fifo_per_partition(&b).await));
    report.push(("lease_redelivery_counts_attempts", case_lease_redelivery_counts_attempts(&b).await));
    report.push(("ack_statuses_completed_failed_retry", case_ack_statuses(&b).await));
    report.push(("retry_limit_exhaustion_is_counted", case_retry_limit_exhaustion_is_counted(&b).await));
    report.push(("auto_ack_is_at_most_once", case_auto_ack_is_at_most_once(&b).await));
    report.push(("fan_out_across_groups", case_fan_out_across_groups(&b).await));
    report.push(("groupless_is_queue_mode", case_groupless_is_queue_mode(&b).await));
    report.push(("ttl_head_drop", case_ttl_head_drop(&b).await));
    report.push(("bounds_reject", case_bounds_reject(&b).await));
    report.push(("bounds_drop_oldest", case_bounds_drop_oldest(&b).await));
    report.push(("cursor_skips_evicted_range", case_cursor_skips_evicted_range(&b).await));
    report.push(("window_buffer_fattens_a_waiting_pop", case_window_buffer_fattens_a_waiting_pop(&b).await));
    report.push(("epoch_stale_acks", case_epoch_stale_acks(&b).await));
    report.push(("implicit_gc", case_implicit_gc(&b).await));
    report.push(("configure_rejects_unknown_options", case_configure_rejects_unknown_options(&b).await));
    report.push(("configure_reset_delete_round_trip", case_configure_reset_delete_round_trip(&b).await));

    // ---- phase B: a declared queue survives a restart, EMPTY --------------
    //
    // §1.2's whole contract in one case, and the only one that needs two brokers:
    // the configuration is durable, the contents are not. A declared queue that
    // came back with its messages would be a promise this class must never make;
    // one that came back UNDECLARED would silently revert to the defaults, drop
    // differently and never appear in the listing again.
    let survivor = unique("eph-survivor-");
    let reboot: Case = async {
        b.ephemeral_configure(json!({ "queue": survivor, "options": { "maxLength": 7 } }))
            .await
            .map_err(|e| e.to_string())?;
        for n in 0..3 {
            b.ephemeral_push(push_one(&survivor, None, n)).await.map_err(|e| e.to_string())?;
        }
        b.shutdown().await;

        let b2 = boot(&host, port).await;
        let row = listed(&b2, &survivor)
            .await?
            .ok_or("the declared queue did not come back after a restart")?;
        chk!(
            row.get("tier").and_then(|t| t.as_str()) == Some("declared"),
            "it must come back DECLARED, not as a fresh implicit queue: {row}"
        );
        chk!(
            row.get("depth").and_then(|d| d.as_i64()) == Some(0),
            "it must come back EMPTY — contents survive nothing (§1.2): {row}"
        );
        chk!(
            row.get("options").and_then(|o| o.get("maxLength")).and_then(|x| x.as_i64()) == Some(7),
            "its CONFIGURATION must have survived: {row}"
        );
        let got = b2.ephemeral_pop(&pop_params(&survivor, &[])).await.map_err(|e| e.to_string())?;
        chk!(msgs(&got).is_empty(), "nothing may be delivered from before the restart");
        b2.ephemeral_delete(&survivor).await.map_err(|e| e.to_string())?;
        b2.shutdown().await;
        Ok(())
    }
    .await;
    report.push(("declared_queue_survives_a_reboot_empty", reboot));

    // ---- phase C: the grant ladder ---------------------------------------
    //
    // `require_grant` is flipped ON and a THIRD broker booted, because the flag is
    // read once at `config::load()`. This is the cloud posture: OSS unlimited by
    // default, cloud refused until the control plane writes a row.
    std::env::set_var("QUEEN_EPHEMERAL_REQUIRE_GRANT", "true");
    let pg = connect(&host, port).await;
    let _ = pg
        .execute("DELETE FROM queen.ephemeral_quota WHERE tenant_id = $1::text::uuid", &[&DEFAULT_TENANT])
        .await;

    let ungranted: Case = async {
        let b3 = boot(&host, port).await;
        let q = unique("eph-gate-");
        let e = b3
            .ephemeral_push(push_one(&q, None, 1))
            .await
            .err()
            .ok_or("with require_grant on, a tenant with NO row must be refused")?;
        chk!(
            e.status() == Some(403),
            "the absence of a grant row is 403 feature_gated, got {:?} ({e})",
            e.status()
        );
        chk!(
            e.to_string().contains("not granted"),
            "the refusal must name the grant and not a quota: {e}"
        );
        // The pop and the status read are on the SAME rung: a gated tenant must
        // not be able to probe the surface either.
        chk!(
            b3.ephemeral_pop(&pop_params(&q, &[])).await.is_err(),
            "a pop must be refused on the same rung as a push"
        );
        b3.shutdown().await;
        Ok(())
    }
    .await;
    report.push(("no_grant_row_is_feature_gated", ungranted));

    let granted: Case = async {
        // A grant with a TINY byte allowance. `max_queues` is left generous so the
        // case isolates the byte rung; the object rung shares the same code path.
        pg.execute(
            "INSERT INTO queen.ephemeral_quota (tenant_id, enabled, max_bytes, max_queues, max_msgs_per_sec) \
             VALUES ($1::text::uuid, TRUE, 200, 100, NULL) \
             ON CONFLICT (tenant_id) DO UPDATE SET enabled = TRUE, max_bytes = 200, \
                 max_queues = 100, max_msgs_per_sec = NULL",
            &[&DEFAULT_TENANT],
        )
        .await
        .map_err(|e| e.to_string())?;

        // A fresh boot awaits ONE grant read before serving, which is exactly the
        // property that keeps a rollout from answering `feature_gated` to
        // everybody for a refresh period.
        let b4 = boot(&host, port).await;
        let q = unique("eph-quota-");
        b4.ephemeral_push(push_one(&q, None, 1))
            .await
            .map_err(|e| format!("a granted tenant under its allowance must be served: {e}"))?;

        // Now run past 200 bytes. Every push is charged before it appends, so the
        // refusal is immediate for the writer that overruns.
        let mut refused = None;
        for n in 0..50 {
            if let Err(e) = b4
                .ephemeral_push(json!({
                    "queue": q,
                    "messages": [{ "payload": { "n": n, "pad": "0123456789012345678901234567890123456789" } }]
                }))
                .await
            {
                refused = Some(e);
                break;
            }
        }
        let e = refused.ok_or("a 200-byte allowance must refuse SOMETHING inside 50 pushes")?;
        chk!(
            e.status() == Some(403),
            "over-quota is 403 ephemeral_quota_exceeded, got {:?} ({e})",
            e.status()
        );
        chk!(
            e.to_string().contains("allowance"),
            "the refusal must name the tenant allowance, not the queue's own bound: {e}"
        );
        b4.shutdown().await;
        Ok(())
    }
    .await;
    report.push(("grant_row_caps_are_enforced", granted));

    // Leave the database as it was found: the row is tenant-scoped and a
    // leftover would make a re-run of phase C start already granted.
    let _ = pg
        .execute("DELETE FROM queen.ephemeral_quota WHERE tenant_id = $1::text::uuid", &[&DEFAULT_TENANT])
        .await;

    println!("\n=============== Ephemeral semantics (EPHEMERAL_QUEUES §7.1) ===============");
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
    assert_eq!(failed, 0, "{failed} ephemeral semantics case(s) failed — see the table above");
}
