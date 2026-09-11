//! DLQ MOVE SEMANTICS — the contract of `queen.log_dlq_move_v1` (016_messages)
//! and of the two replay routes built on it, against a real Postgres.
//!
//! WHY THIS EXISTS. The replay route it replaces had four independent defects
//! and the dashboard dropped its button because of them (8d357fa4): a fresh
//! transaction id per attempt (so every double click appended another copy), a
//! read without `FOR UPDATE` followed by a delete in a second statement (so two
//! concurrent callers both pushed), a reportable "pushed but still
//! dead-lettered" state, and an address — (partition_id, transaction_id) — that
//! can carry ONE ROW PER CONSUMER GROUP while the cleanup deleted every one of
//! them. Each case below is one of those defects, written as the property that
//! now holds.
//!
//! WHY AT THIS LEVEL, AND WHY BOTH LEVELS. The move is one SQL transaction, so
//! the concurrency, tenancy and multi-group cases call the SP directly on the
//! same connection kind the broker uses — going through HTTP would prove them
//! for one of the four surfaces (HTTP, the embedded facade, the SDKs, a future
//! redrive) and nothing about the others. But two of the properties are the
//! BROKER's half and are invisible from SQL: the frame is packed and encrypted
//! by the broker, and the landing is announced by it
//! (`handlers::announce_landed`) — the omission behind the 1.0.3-through-1.5.1
//! timer bug. Those two cases go through `embedded::Broker`, which dispatches to
//! the very handler the router does.
//!
//! WHAT THE SYNTHETIC SEGMENTS ARE. The SQL-level cases hand the SP a 16-byte
//! hash and an opaque blob: the SP never decodes a blob (§3 — SQL cannot see
//! inside a segment), so a real one would test nothing extra. They are moved
//! into a per-case SINK queue that nothing ever pops, and each call carries a
//! UNIQUE hash because the destination's dedup window is the schema default
//! (3600s) and two identical hashes would be a `duplicate` on purpose — except
//! in the two cases that WANT one: the concurrency case, where both callers
//! deliberately carry the same fingerprint exactly as two brokers packing the
//! same row would, and the duplicate case, which is about that verdict.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`,
//! same convention as `kv_semantics` and `timer_fire_delivery`:
//!
//! ```bash
//! docker run --rm -d --name queen-w3-pg -e POSTGRES_PASSWORD=postgres -p 5474:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5474 cargo test --test dlq_move_semantics -- --ignored --nocapture
//! ```
//!
//! ONE test function on purpose (one Broker is booted to apply the real,
//! `include_str!`-embedded schema, and the encryption key is process-global
//! env). The cases are still reported one by one: each returns
//! `Result<(), String>` and the runner prints a PASS/FAIL line per case before
//! failing, so a red run names every broken rule instead of only the first.

use queen::protocol as qp;
use queen::{Broker, BrokerConfig};
use serde_json::Value;
use tokio_postgres::Client;

/// `config::DEFAULT_TENANT` — what every request resolves to with tenancy off.
const DEFAULT_TENANT: &str = "00000000-0000-0000-0000-000000000001";
/// Any other valid uuid: the foreign-tenant case needs a tenant that owns
/// nothing, not one that owns something else.
const OTHER_TENANT: &str = "00000000-0000-0000-0000-0000000000ff";

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

/// A 16-byte xxh3_128 stand-in. Unique per call for the reason the header
/// gives; `distinct` lets a caller pin two calls to the SAME fingerprint.
fn synthetic_hash(distinct: u128) -> Vec<u8> {
    distinct.to_be_bytes().to_vec()
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

/// `tokio_postgres::Error`'s Display is "db error" and nothing else, which turns
/// a red suite into identical lines. Unwrap the SQLSTATE and message.
fn pg_err(e: tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{} {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

/// One `log_dlq_move_v1` call, the broker's binding idiom: uuids as text with an
/// explicit cast, both segment blobs as bytea.
async fn move_row(
    c: &Client,
    tenant: &str,
    dlq_id: &str,
    queue: &str,
    partition: &str,
    hashes: &[u8],
) -> Result<Value, String> {
    let blob: &[u8] = b"opaque-to-sql";
    let row = c
        .query_one(
            "SELECT queen.log_dlq_move_v1($1::text::uuid, $2::text::uuid, $3, $4, \
             $5::bytea, $6::bytea)::text",
            &[&tenant, &dlq_id, &queue, &partition, &hashes, &blob],
        )
        .await
        .map_err(pg_err)?;
    let txt: String = row.get(0);
    Ok(serde_json::from_str(&txt).unwrap_or(Value::Null))
}

fn result_of(v: &Value) -> String {
    v.get("result")
        .and_then(|x| x.as_str())
        .unwrap_or("<none>")
        .to_string()
}

async fn dlq_row_count(c: &Client, dlq_id: &str) -> Result<i64, String> {
    c.query_one(
        "SELECT count(*) FROM queen.log_dlq WHERE id = $1::text::uuid",
        &[&dlq_id],
    )
    .await
    .map(|r| r.get::<_, i64>(0))
    .map_err(pg_err)
}

/// The payload column exactly as the ack path wrote it — no decryption, unlike
/// `GET /api/v1/dlq`, which decrypts on read.
async fn stored_dlq_payload(c: &Client, dlq_id: &str) -> Result<Value, String> {
    let txt: String = c
        .query_one(
            "SELECT COALESCE(payload, 'null'::jsonb)::text FROM queen.log_dlq \
             WHERE id = $1::text::uuid",
            &[&dlq_id],
        )
        .await
        .map_err(pg_err)?
        .get(0);
    Ok(serde_json::from_str(&txt).unwrap_or(Value::Null))
}

/// The partition's allocator watermark — how many frames the destination has
/// taken. `-1` for a partition that does not exist yet.
async fn last_offset(c: &Client, queue: &str, partition: &str) -> Result<i64, String> {
    let rows = c
        .query(
            "SELECT p.last_offset FROM queen.log_partitions p \
             JOIN queen.queues q ON q.id = p.queue_id \
             WHERE q.name = $1 AND p.name = $2 AND q.tenant_id = $3::text::uuid",
            &[&queue, &partition, &DEFAULT_TENANT],
        )
        .await
        .map_err(pg_err)?;
    Ok(rows.first().map(|r| r.get::<_, i64>(0)).unwrap_or(-1))
}

fn group_params(group: &str, wait_ms: Option<u64>) -> qp::PopParams {
    qp::PopParams {
        batch: Some(10),
        consumer_group: Some(group.to_string()),
        subscription_mode: Some(qp::SubscriptionMode::All),
        wait: wait_ms.map(|_| true),
        timeout_millis: wait_ms,
        ..Default::default()
    }
}

/// Push one message, claim it, and dead-letter it through the ACK PATH — the
/// way the broker produces a `queen.log_dlq` row in production (a forced `dlq`
/// ack under a live lease: 005_log_ack returns the poison offset, the broker
/// snapshots the frame and calls `log_dlq_head_v1`). Returns the DLQ row.
async fn dead_letter_one(
    broker: &Broker,
    queue: &str,
    group: &str,
    payload: Value,
) -> Result<qp::DlqMessage, String> {
    broker
        .push(vec![qp::PushItem::new(queue.to_string(), payload)])
        .await
        .map_err(|e| format!("push: {e}"))?;
    let claimed = broker
        .pop(queue, &group_params(group, None))
        .await
        .map_err(|e| format!("pop: {e}"))?;
    let m = claimed
        .messages
        .first()
        .ok_or_else(|| format!("{queue}/{group}: nothing to claim"))?;
    let acked = broker
        .ack(&qp::AckRequest {
            transaction_id: m.transaction_id.clone(),
            partition_id: m.partition_id.clone(),
            status: qp::AckStatus::Dlq,
            consumer_group: Some(group.to_string()),
            lease_id: Some(m.lease_id.clone()),
            error: Some("poison".to_string()),
        })
        .await
        .map_err(|e| format!("dlq ack: {e}"))?;
    if !acked.success || !acked.dlq {
        return Err(format!("the ack did not dead-letter: {acked:?}"));
    }
    let listed = broker
        .dlq(&qp::DlqParams {
            queue: Some(queue.to_string()),
            consumer_group: Some(group.to_string()),
            ..Default::default()
        })
        .await
        .map_err(|e| format!("dlq list: {e}"))?;
    listed
        .messages
        .into_iter()
        .next()
        .ok_or_else(|| format!("{queue}/{group}: no dead-letter row after the ack"))
}

// ===========================================================================
// THE BROKER'S HALF. A replay through the handler must (a) put the payload back
// in the log under the deterministic transaction id, (b) remove the row, and
// (c) ANNOUNCE the landing — the parked pop below can only be served by the
// announce, because the re-poll floor is inflated to 6s (see the runner).
// ===========================================================================
async fn case_replay_wakes_a_parked_pop(broker: &Broker, c: &Client) -> Case {
    let queue = unique("dlqmove.replay");
    let group = "movers";
    let payload = serde_json::json!({"order": 4471, "amount": 41});

    let row = dead_letter_one(broker, &queue, group, payload.clone()).await?;
    let dlq_id = row.id.clone().ok_or("the DLQ listing carried no row id")?;
    let pid = row.partition_id.clone().ok_or("no partitionId")?;
    let txn = row.transaction_id.clone().ok_or("no transactionId")?;

    // Park a consumer BEFORE the move, on the group whose cursor the
    // dead-lettering already advanced past the poison frame: the only thing that
    // can make this queue pending for it again is the moved frame.
    let waiter = {
        let broker = broker.clone();
        let queue = queue.clone();
        let group = group.to_string();
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            let got = broker.pop(&queue, &group_params(&group, Some(8_000))).await;
            (started.elapsed(), got)
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    let replay = broker
        .retry_message(&pid, &txn)
        .await
        .map_err(|e| format!("retry_message: {e}"))?;
    chk!(
        replay.get("success") == Some(&serde_json::json!(true))
            && replay.get("result") == Some(&serde_json::json!("moved")),
        "the replay must report a move: {replay}"
    );
    chk!(
        replay.get("dlqRowRemoved") == Some(&serde_json::json!(true)),
        "a committed move always removes the row: {replay}"
    );
    let replayed_txn = replay
        .get("replayedAs")
        .and_then(|r| r.get("transaction_id"))
        .and_then(|x| x.as_str())
        .unwrap_or_default()
        .to_string();
    chk!(
        replayed_txn == format!("dlq:{dlq_id}"),
        "the replayed frame must carry the deterministic id dlq:{dlq_id}, got {replayed_txn:?}"
    );
    chk!(
        dlq_row_count(c, &dlq_id).await? == 0,
        "the dead-letter row must be gone in the same transaction"
    );

    let (elapsed, got) = waiter.await.map_err(|e| format!("waiter join: {e}"))?;
    let got = got.map_err(|e| format!("parked pop: {e}"))?;
    chk!(
        got.messages.len() == 1,
        "the parked pop must be served the moved frame, got {got:?}"
    );
    chk!(
        elapsed < std::time::Duration::from_secs(2),
        "the move must ANNOUNCE the landing: with the re-poll floor at 6s only \
         handlers::announce_landed can serve this pop, and it took {elapsed:?}"
    );
    let m = &got.messages[0];
    chk!(
        m.data == payload,
        "the payload must survive the round trip: {:?} != {payload}",
        m.data
    );
    chk!(
        m.transaction_id == replayed_txn,
        "the delivered frame must carry the id the route reported: {} != {replayed_txn}",
        m.transaction_id
    );
    Ok(())
}

// ===========================================================================
// Defect 2, the one a double click hits: the row is claimed under a lock, so two
// concurrent movers produce ONE message and the loser is told `gone` — not a
// second copy, and not an error either.
//
// THE OVERLAP IS FORCED, not hoped for. Two spawned tasks that each open their
// own connection usually serialise by themselves, and a second mover that
// starts after the first COMMITTED legitimately answers `gone` — the same word
// the lock produces — so a version of this case that only raced two tasks
// passed with `FOR UPDATE OF d` deleted from the SP. Here the first mover sits
// inside an OPEN transaction while the second one runs, which makes the two
// outcomes different words: with the row lock the second blocks on the DLQ row
// and finds nothing after the commit (`gone`); without it, it walks past the
// row, blocks on the destination partition instead, and answers `duplicate`
// once the first commits (both carry the same fingerprint, exactly as two
// brokers packing the same row would).
// ===========================================================================
async fn case_two_concurrent_movers_produce_one_message(
    broker: &Broker,
    c: &Client,
    host: &str,
    port: u16,
) -> Case {
    let queue = unique("dlqmove.race");
    let sink = unique("dlqmove.racesink");
    let row = dead_letter_one(broker, &queue, "movers", serde_json::json!({"n": 1})).await?;
    let dlq_id = row.id.clone().ok_or("no row id")?;

    // The same fingerprint on both callers, deliberately: two brokers packing
    // the same row produce the same deterministic txn and therefore the same
    // hash. Only the row lock decides the winner.
    let hash = synthetic_hash(0x5ace_0000_0000_0000_0000_0000_0000_0001);

    // The winner, holding the row lock in an uncommitted transaction.
    let first_conn = connect(host, port).await;
    first_conn.batch_execute("BEGIN").await.map_err(pg_err)?;
    let first = move_row(&first_conn, DEFAULT_TENANT, &dlq_id, &sink, "Default", &hash).await?;
    chk!(
        result_of(&first) == "moved",
        "the first mover holds the row and moves it: {first}"
    );

    let loser = {
        let (dlq_id, sink, hash) = (dlq_id.clone(), sink.clone(), hash.clone());
        let host = host.to_string();
        tokio::spawn(async move {
            let c = connect(&host, port).await;
            move_row(&c, DEFAULT_TENANT, &dlq_id, &sink, "Default", &hash).await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(750)).await;
    if loser.is_finished() {
        let answered = loser.await.map_err(|e| format!("join: {e}"))?;
        return Err(format!(
            "the second mover must BLOCK while the first transaction holds the \
             dead-letter row; it answered {answered:?} instead"
        ));
    }

    first_conn.batch_execute("COMMIT").await.map_err(pg_err)?;
    let second = loser.await.map_err(|e| format!("join: {e}"))??;
    chk!(
        result_of(&second) == "gone",
        "the loser must find the row gone. `duplicate` here means it got PAST \
         the row (no FOR UPDATE) and pushed against the destination's dedup \
         window instead: {second}"
    );
    chk!(
        last_offset(c, &sink, "Default").await? == 0,
        "the destination must hold exactly ONE frame after two concurrent moves"
    );
    chk!(
        dlq_row_count(c, &dlq_id).await? == 0,
        "the winner's transaction removes the row"
    );
    Ok(())
}

// ===========================================================================
// The same property in sequence, which is what a retried HTTP request does:
// moving an id twice is `gone`, never a second copy.
// ===========================================================================
async fn case_a_second_move_is_gone(broker: &Broker, c: &Client) -> Case {
    let queue = unique("dlqmove.twice");
    let sink = unique("dlqmove.twicesink");
    let row = dead_letter_one(broker, &queue, "movers", serde_json::json!({"n": 2})).await?;
    let dlq_id = row.id.clone().ok_or("no row id")?;

    let first = move_row(
        c,
        DEFAULT_TENANT,
        &dlq_id,
        &sink,
        "Default",
        &synthetic_hash(0x0001_0000_0000_0000_0000_0000_0000_0002),
    )
    .await?;
    chk!(result_of(&first) == "moved", "the first move: {first}");
    chk!(
        first.get("offset") == Some(&serde_json::json!(0)),
        "the first frame of a fresh partition is offset 0: {first}"
    );

    let second = move_row(
        c,
        DEFAULT_TENANT,
        &dlq_id,
        &sink,
        "Default",
        &synthetic_hash(0x0002_0000_0000_0000_0000_0000_0000_0002),
    )
    .await?;
    chk!(
        result_of(&second) == "gone",
        "a second move of the same row must be gone, not a second copy: {second}"
    );
    chk!(
        last_offset(c, &sink, "Default").await? == 0,
        "the destination must still hold exactly one frame"
    );
    Ok(())
}

// ===========================================================================
// Defect 4, and the reason the route is addressed by ROW ID. One message
// dead-lettered under two consumer groups is two rows; moving one must leave the
// other exactly where it was.
// ===========================================================================
async fn case_moving_one_group_leaves_the_other(broker: &Broker, c: &Client) -> Case {
    let queue = unique("dlqmove.groups");
    let sink = unique("dlqmove.groupsink");
    let payload = serde_json::json!({"shared": true});

    // Two groups, same message: the second group's cursor is its own, so it
    // claims the same frame and dead-letters it again.
    let a = dead_letter_one(broker, &queue, "group-a", payload.clone()).await?;
    let b = {
        let claimed = broker
            .pop(&queue, &group_params("group-b", None))
            .await
            .map_err(|e| format!("pop b: {e}"))?;
        let m = claimed
            .messages
            .first()
            .ok_or("group-b claimed nothing: the frame is still in the log")?;
        let acked = broker
            .ack(&qp::AckRequest {
                transaction_id: m.transaction_id.clone(),
                partition_id: m.partition_id.clone(),
                status: qp::AckStatus::Dlq,
                consumer_group: Some("group-b".to_string()),
                lease_id: Some(m.lease_id.clone()),
                error: Some("poison for b".to_string()),
            })
            .await
            .map_err(|e| format!("dlq ack b: {e}"))?;
        chk!(acked.success && acked.dlq, "group-b ack: {acked:?}");
        broker
            .dlq(&qp::DlqParams {
                queue: Some(queue.clone()),
                consumer_group: Some("group-b".to_string()),
                ..Default::default()
            })
            .await
            .map_err(|e| format!("dlq list b: {e}"))?
            .messages
            .into_iter()
            .next()
            .ok_or("no dead-letter row for group-b")?
    };

    let id_a = a.id.clone().ok_or("no row id for a")?;
    let id_b = b.id.clone().ok_or("no row id for b")?;
    chk!(
        id_a != id_b && a.transaction_id == b.transaction_id,
        "the fixture must be two rows at ONE address: {a:?} / {b:?}"
    );

    let moved = move_row(
        c,
        DEFAULT_TENANT,
        &id_a,
        &sink,
        "Default",
        &synthetic_hash(0x0003_0000_0000_0000_0000_0000_0000_0003),
    )
    .await?;
    chk!(result_of(&moved) == "moved", "moving group-a's row: {moved}");
    chk!(
        moved.get("consumerGroup") == Some(&serde_json::json!("group-a")),
        "the verdict names the group whose record was moved: {moved}"
    );
    chk!(
        dlq_row_count(c, &id_a).await? == 0 && dlq_row_count(c, &id_b).await? == 1,
        "moving one group's record must leave the other group's record alone"
    );
    Ok(())
}

// ===========================================================================
// Tenancy: a row of another tenant is `gone` — the same answer as a row that
// never existed, and it must not be moved or even reported as existing.
// ===========================================================================
async fn case_a_foreign_tenant_sees_gone(broker: &Broker, c: &Client) -> Case {
    let queue = unique("dlqmove.tenant");
    let sink = unique("dlqmove.tenantsink");
    let row = dead_letter_one(broker, &queue, "movers", serde_json::json!({"n": 3})).await?;
    let dlq_id = row.id.clone().ok_or("no row id")?;

    let foreign = move_row(
        c,
        OTHER_TENANT,
        &dlq_id,
        &sink,
        "Default",
        &synthetic_hash(0x0004_0000_0000_0000_0000_0000_0000_0004),
    )
    .await?;
    chk!(
        result_of(&foreign) == "gone",
        "another tenant's row must be gone: {foreign}"
    );
    chk!(
        dlq_row_count(c, &dlq_id).await? == 1,
        "the row must survive a foreign move"
    );
    chk!(
        last_offset(c, &sink, "Default").await? == -1,
        "a refused move must not even provision its destination"
    );

    // And the owner still moves it, so `gone` was the tenant boundary and not a
    // broken fixture.
    let owned = move_row(
        c,
        DEFAULT_TENANT,
        &dlq_id,
        &sink,
        "Default",
        &synthetic_hash(0x0005_0000_0000_0000_0000_0000_0000_0005),
    )
    .await?;
    chk!(result_of(&owned) == "moved", "the owner's move: {owned}");
    Ok(())
}

// ===========================================================================
// The move primitive proper: the destination is named, not implied, and a
// destination that does not exist yet is provisioned by log_push_one_v1 exactly
// as a first-contact producer push provisions one.
// ===========================================================================
async fn case_a_move_provisions_a_new_destination(broker: &Broker, c: &Client) -> Case {
    let queue = unique("dlqmove.origin");
    let sink = unique("dlqmove.elsewhere");
    let row = dead_letter_one(broker, &queue, "movers", serde_json::json!({"n": 4})).await?;
    let dlq_id = row.id.clone().ok_or("no row id")?;

    chk!(
        last_offset(c, &sink, "retry-lane").await? == -1,
        "the fixture wants a destination that does not exist yet"
    );
    let moved = move_row(
        c,
        DEFAULT_TENANT,
        &dlq_id,
        &sink,
        "retry-lane",
        &synthetic_hash(0x0006_0000_0000_0000_0000_0000_0000_0006),
    )
    .await?;
    chk!(result_of(&moved) == "moved", "the move: {moved}");
    chk!(
        moved.get("queue") == Some(&serde_json::json!(sink.clone()))
            && moved.get("partition") == Some(&serde_json::json!("retry-lane")),
        "the verdict names the DESTINATION, not the origin: {moved}"
    );
    chk!(
        last_offset(c, &sink, "retry-lane").await? == 0,
        "the destination queue and partition must have been provisioned"
    );
    chk!(
        dlq_row_count(c, &dlq_id).await? == 0,
        "the source row goes wherever the message went"
    );
    Ok(())
}

// ===========================================================================
// The `duplicate` verdict: the destination's dedup window already holds a frame
// under this move's transaction id, so NOTHING is written — and nothing is
// removed either.
//
// WHY THE ROW STAYS, which is the whole point of this case. The dedup identity
// is the txn hash and nothing else, and a move's txn is `dlq:<row id>` — an id
// the DLQ listing hands to every reader. So a `duplicate` does NOT prove the
// frame in the window is this row's snapshot: a producer credential can put a
// decoy there under that id. Deleting the dead-letter row on that verdict would
// destroy the record while writing nothing, and the consumer group's cursor is
// already past the original offset. The move therefore removes a row only when
// it moved it.
// ===========================================================================
async fn case_a_duplicate_writes_nothing_and_keeps_the_row(broker: &Broker, c: &Client) -> Case {
    let queue = unique("dlqmove.dup");
    let sink = unique("dlqmove.dupsink");

    let first = dead_letter_one(broker, &queue, "movers", serde_json::json!({"n": "first"})).await?;
    let second =
        dead_letter_one(broker, &queue, "movers", serde_json::json!({"n": "second"})).await?;
    let id_first = first.id.clone().ok_or("no row id for the first")?;
    let id_second = second.id.clone().ok_or("no row id for the second")?;
    chk!(
        id_first != id_second,
        "the fixture wants two DIFFERENT dead-letter rows: {id_first} / {id_second}"
    );

    // ONE fingerprint for both moves — the only case here that does that on
    // purpose besides the race: it is what makes the second push a duplicate
    // inside the sink's dedup window (the schema default, 3600s).
    let hash = synthetic_hash(0x0d0d_0000_0000_0000_0000_0000_0000_0008);
    let moved = move_row(c, DEFAULT_TENANT, &id_first, &sink, "Default", &hash).await?;
    chk!(result_of(&moved) == "moved", "the first move: {moved}");
    chk!(
        moved.get("offset") == Some(&serde_json::json!(0)),
        "the first frame of a fresh partition is offset 0: {moved}"
    );

    let dup = move_row(c, DEFAULT_TENANT, &id_second, &sink, "Default", &hash).await?;
    chk!(
        result_of(&dup) == "duplicate",
        "the same fingerprint inside the window must answer duplicate: {dup}"
    );
    chk!(
        dup.get("offset") == Some(&serde_json::json!(0)),
        "a duplicate reports the PRE-EXISTING occurrence's offset, not null: {dup}"
    );
    chk!(
        last_offset(c, &sink, "Default").await? == 0,
        "a duplicate allocates nothing: the destination must still hold one frame"
    );
    chk!(
        dlq_row_count(c, &id_second).await? == 1,
        "a move that wrote nothing must remove nothing — the dead-letter row it \
         did not move has to still be there"
    );
    chk!(
        dlq_row_count(c, &id_first).await? == 0,
        "the row that WAS moved is gone, in the same transaction as its frame"
    );
    Ok(())
}

// ===========================================================================
// THE ROUTE THE DASHBOARD CALLS. Everything above addresses the SP or the
// address-keyed retry route; `POST /api/v1/dlq/:id/replay` is the third
// surface, and the only one whose address names exactly one record. Through
// `embedded::Broker`, which dispatches to the same handler the router does.
//
// Two properties at once, because they share a fixture: the overridden half is
// the ONLY half that moves (a "move this to queue X" must not silently
// re-partition the message), and the moved frame is a real, poppable message at
// the destination.
// ===========================================================================
async fn case_the_id_route_moves_only_the_half_it_was_given(
    broker: &Broker,
    c: &Client,
) -> Case {
    let queue = unique("dlqmove.byid");
    let sink = unique("dlqmove.byidsink");
    let payload = serde_json::json!({"addressed": "by row id"});
    let row = dead_letter_one(broker, &queue, "movers", payload.clone()).await?;
    let dlq_id = row.id.clone().ok_or("no row id")?;
    let source_partition = row.partition.clone().ok_or("the listing carried no partition")?;

    let answer = broker
        .dlq_replay(&dlq_id, Some(&sink), None)
        .await
        .map_err(|e| format!("dlq_replay: {e}"))?;
    chk!(
        answer.get("result") == Some(&serde_json::json!("moved")),
        "the replay: {answer}"
    );
    chk!(
        answer.get("queue") == Some(&serde_json::json!(sink.clone())),
        "the override names the destination QUEUE: {answer}"
    );
    chk!(
        answer.get("partition") == Some(&serde_json::json!(source_partition.clone())),
        "an omitted half keeps the source row's partition ({source_partition}): {answer}"
    );
    chk!(
        answer.get("dlqRowRemoved") == Some(&serde_json::json!(true)),
        "a committed move removes the row: {answer}"
    );
    chk!(
        answer
            .get("replayedAs")
            .and_then(|r| r.get("transaction_id"))
            .and_then(|x| x.as_str())
            == Some(format!("dlq:{dlq_id}").as_str()),
        "the id route mints the same deterministic txn: {answer}"
    );
    chk!(
        dlq_row_count(c, &dlq_id).await? == 0,
        "the dead-letter row must be gone"
    );
    chk!(
        last_offset(c, &sink, &source_partition).await? == 0,
        "the frame must be in the destination's OWN partition, provisioned by the move"
    );

    let got = broker
        .pop(&sink, &group_params("readers", None))
        .await
        .map_err(|e| format!("pop: {e}"))?;
    chk!(
        got.messages.len() == 1 && got.messages[0].data == payload,
        "the moved frame must be a poppable message carrying the snapshot: {got:?}"
    );

    // A row id is not an address: replaying the same id again finds nothing.
    let again = broker.dlq_replay(&dlq_id, Some(&sink), None).await;
    chk!(
        again.as_ref().err().and_then(|e| e.status()) == Some(404),
        "a second replay of the same row id must be a 404 gone: {again:?}"
    );
    Ok(())
}

// ===========================================================================
// An id that cannot name a row is the same answer as a row that is no longer
// there: 404 `gone`, never a 500 out of the `::text::uuid` cast (which the
// dashboard would render as "the replay failed" instead of "already replayed or
// purged"). Postgres is the parser — the second spelling below passes a
// 32-nibble-with-dashes-anywhere rule and is refused by the database.
// ===========================================================================
async fn case_an_id_that_names_no_row_is_gone(broker: &Broker) -> Case {
    for id in [
        "not-a-uuid",
        "0-198f2c14d3a7c109f2b6a1e5d0c7b83",
        "0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b8",
        // Well-formed, and simply not a row.
        "0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83",
    ] {
        let answer = broker.dlq_replay(id, None, None).await;
        chk!(
            answer.as_ref().err().and_then(|e| e.status()) == Some(404),
            "{id:?} must answer 404 gone, got {answer:?}"
        );
    }
    Ok(())
}

// ===========================================================================
// PUSH MAINTENANCE REFUSES A MOVE. The switch's guarantee is that nothing
// reaches queen.log_segments while it is on, and a move would reach it through
// log_push_one_v1 — a new queue and partition included. It cannot be spooled
// either (the spool carries frames, not the deletion of a dead-letter row: that
// is the "pushed but still dead-lettered" state this primitive exists to make
// impossible), so the answer is 503 and the row stays where it is.
// ===========================================================================
async fn case_push_maintenance_refuses_a_move(broker: &Broker, c: &Client) -> Case {
    let queue = unique("dlqmove.maint");
    // Dead-letter FIRST: with maintenance on, the push itself would be spooled.
    let row = dead_letter_one(broker, &queue, "movers", serde_json::json!({"n": 6})).await?;
    let dlq_id = row.id.clone().ok_or("no row id")?;

    broker
        .set_push_maintenance(true)
        .await
        .map_err(|e| format!("maintenance on: {e}"))?;
    let refused = broker.dlq_replay(&dlq_id, None, None).await;
    let status = refused.as_ref().err().and_then(|e| e.status());
    // Restore before any assertion can leave the switch on for the cases below.
    broker
        .set_push_maintenance(false)
        .await
        .map_err(|e| format!("maintenance off: {e}"))?;
    chk!(
        status == Some(503),
        "a move under push maintenance must be refused with 503, got {refused:?}"
    );
    chk!(
        dlq_row_count(c, &dlq_id).await? == 1,
        "a refused move leaves the dead-letter row exactly where it was"
    );

    // And the same replay works the moment the switch is off, which is what
    // makes the refusal cost nothing.
    let moved = broker
        .dlq_replay(&dlq_id, None, None)
        .await
        .map_err(|e| format!("dlq_replay after maintenance: {e}"))?;
    chk!(
        moved.get("result") == Some(&serde_json::json!("moved")),
        "the replay after maintenance: {moved}"
    );
    chk!(
        dlq_row_count(c, &dlq_id).await? == 0,
        "and then the row is gone"
    );
    Ok(())
}

// ===========================================================================
// An empty destination name would provision a queue nobody can address. Both
// guards are the SP's, in 003_log_push's style: loud, not silently mis-filed.
// ===========================================================================
async fn case_the_guards_are_loud(broker: &Broker, c: &Client) -> Case {
    let queue = unique("dlqmove.guards");
    let row = dead_letter_one(broker, &queue, "movers", serde_json::json!({"n": 5})).await?;
    let dlq_id = row.id.clone().ok_or("no row id")?;

    let short = move_row(c, DEFAULT_TENANT, &dlq_id, "sink", "Default", b"short").await;
    chk!(
        short.as_ref().err().is_some_and(|e| e.contains("QMOVE bad segment")),
        "a hash blob that is not 16 bytes must RAISE: {short:?}"
    );
    let unnamed = move_row(
        c,
        DEFAULT_TENANT,
        &dlq_id,
        "",
        "Default",
        &synthetic_hash(0x0007_0000_0000_0000_0000_0000_0000_0007),
    )
    .await;
    chk!(
        unnamed.as_ref().err().is_some_and(|e| e.contains("QMOVE unnamed destination")),
        "an empty destination queue must RAISE: {unnamed:?}"
    );
    chk!(
        dlq_row_count(c, &dlq_id).await? == 1,
        "a raised guard rolls its transaction back: the row must still be there"
    );
    Ok(())
}

// ===========================================================================
// Encryption is decided by the DESTINATION queue, and the snapshot is stored
// VERBATIM — so a move must decrypt the envelope and re-encrypt at pack time,
// or the replayed message is a double-encrypted envelope nobody can read. The
// pop is the proof: it returns plaintext.
// ===========================================================================
async fn case_an_encrypted_snapshot_round_trips(broker: &Broker, c: &Client) -> Case {
    let queue = unique("dlqmove.secret");
    let group = "movers";
    let payload = serde_json::json!({"card": "4111-1111-1111-1111"});

    // Configure FIRST: the queue's encryption flag must exist before the first
    // push, both because the push path reads it to encrypt and because the
    // broker memoizes it per (tenant, queue) for the process's lifetime.
    broker
        .configure(
            &qp::ConfigureRequest::new(queue.clone()).options(qp::QueueOptions {
                encryption_enabled: Some(true),
                ..Default::default()
            }),
        )
        .await
        .map_err(|e| format!("configure: {e}"))?;

    let row = dead_letter_one(broker, &queue, group, payload.clone()).await?;
    let dlq_id = row.id.clone().ok_or("no row id")?;
    let pid = row.partition_id.clone().ok_or("no partitionId")?;
    let txn = row.transaction_id.clone().ok_or("no transactionId")?;
    // Read the snapshot as STORED, not as the DLQ route renders it: the route
    // decrypts on read (handlers::decrypt_dlq_payloads) so the console is not
    // full of ciphertext, which would hide the very thing this case is about. If
    // the stored payload is not an envelope, the rest of the case proves nothing.
    let stored = stored_dlq_payload(c, &dlq_id).await?;
    chk!(
        stored.get("encrypted").is_some() && stored.get("authTag").is_some(),
        "the snapshot of an encrypted queue must be stored as an envelope, got {stored}"
    );

    let replay = broker
        .retry_message(&pid, &txn)
        .await
        .map_err(|e| format!("retry_message: {e}"))?;
    chk!(
        replay.get("result") == Some(&serde_json::json!("moved")),
        "the replay: {replay}"
    );
    chk!(dlq_row_count(c, &dlq_id).await? == 0, "the row must be gone");

    let got = broker
        .pop(&queue, &group_params(group, None))
        .await
        .map_err(|e| format!("pop: {e}"))?;
    chk!(
        got.messages.len() == 1,
        "the replayed message must be poppable: {got:?}"
    );
    chk!(
        got.messages[0].data == payload,
        "a moved envelope must come back as PLAINTEXT, not as a double-encrypted \
         envelope: {:?}",
        got.messages[0].data
    );
    Ok(())
}

// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn dlq_move_semantics() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // Inflate the parked pop's re-poll floor above the 2s bound the announce
    // case asserts, so that assertion can ONLY pass through the in-process
    // notifier: a move that forgot handlers::announce_landed would sit there
    // until the first 6s re-poll. Same trick, and same reasoning, as
    // embedded_smoke's long-poll case. Safe: this binary runs one test and every
    // other pop here is wait=false.
    std::env::set_var("POP_WAIT_INITIAL_INTERVAL_MS", "6000");
    std::env::set_var("POP_WAIT_MAX_INTERVAL_MS", "6000");
    // A key must be configured before the broker boots (encryption.rs reads the
    // env once) or the encrypted case would silently test the plaintext path.
    std::env::set_var(
        "QUEEN_ENCRYPTION_KEY",
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );

    // Booting the real broker applies the include_str!-embedded schema, so this
    // also proves 016_messages.sql compiles from a clean boot AND that it is
    // registered in schema.rs's PROCEDURES list — a file that exists on disk but
    // is missing from that list is never applied, and every case below would
    // fail with 42883 instead of passing silently.
    let broker = Broker::start(
        BrokerConfig::new()
            .pg(host.clone(), port, "postgres", "postgres", "postgres")
            .pool_size(8),
    )
    .await
    .expect("broker start");

    let c = connect(&host, port).await;

    let mut report: Vec<(&str, Case)> = Vec::new();
    report.push((
        "replay_wakes_a_parked_pop",
        case_replay_wakes_a_parked_pop(&broker, &c).await,
    ));
    report.push((
        "two_concurrent_movers_produce_one_message",
        case_two_concurrent_movers_produce_one_message(&broker, &c, &host, port).await,
    ));
    report.push((
        "a_second_move_is_gone",
        case_a_second_move_is_gone(&broker, &c).await,
    ));
    report.push((
        "moving_one_group_leaves_the_other",
        case_moving_one_group_leaves_the_other(&broker, &c).await,
    ));
    report.push((
        "a_foreign_tenant_sees_gone",
        case_a_foreign_tenant_sees_gone(&broker, &c).await,
    ));
    report.push((
        "a_move_provisions_a_new_destination",
        case_a_move_provisions_a_new_destination(&broker, &c).await,
    ));
    report.push((
        "a_duplicate_writes_nothing_and_keeps_the_row",
        case_a_duplicate_writes_nothing_and_keeps_the_row(&broker, &c).await,
    ));
    report.push((
        "the_id_route_moves_only_the_half_it_was_given",
        case_the_id_route_moves_only_the_half_it_was_given(&broker, &c).await,
    ));
    report.push((
        "an_id_that_names_no_row_is_gone",
        case_an_id_that_names_no_row_is_gone(&broker).await,
    ));
    report.push((
        "push_maintenance_refuses_a_move",
        case_push_maintenance_refuses_a_move(&broker, &c).await,
    ));
    report.push(("the_guards_are_loud", case_the_guards_are_loud(&broker, &c).await));
    report.push((
        "an_encrypted_snapshot_round_trips",
        case_an_encrypted_snapshot_round_trips(&broker, &c).await,
    ));

    println!("\n=================== DLQ move semantics (log_dlq_move_v1) ===================");
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
    assert_eq!(failed, 0, "{failed} DLQ move case(s) failed — see the table above");
}
