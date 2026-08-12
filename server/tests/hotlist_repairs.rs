//! Contract test for the DURABLE hot-list repair markers (A3,
//! PLAN_HOTLIST_FOLLOWUP.md).
//!
//! A hot-list ring learns that OLD partitions became pending again — a backward seek,
//! a consumer-group delete — only from the mesh, and the mesh drops frames by design
//! when a peer is slow or down. `queen.hotlist_repairs` is the floor underneath: the
//! operation publishes a row in its OWN transaction and every broker polls it on its
//! reconcile cadence.
//!
//! The unit tests in `hotlist.rs` cover what a broker DOES with a marker
//! (`request_full_walk`, `mark_remote_group`). This covers the publication, which they
//! cannot reach: that a repair is published at all, that it is scoped to what actually
//! moved, that the table stays one row per (tenant, queue, group) however many times it
//! is written, and that a failed operation publishes nothing.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`:
//!
//! ```bash
//! docker run --rm -d --name queen-repairs-pg -e POSTGRES_PASSWORD=postgres -p 5467:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5467 cargo test --test hotlist_repairs -- --ignored --nocapture
//! ```

use queen::{Broker, BrokerConfig};

const TENANT: &str = "00000000-0000-0000-0000-000000000001";

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

/// Every repair published for `queue`, as (group, partition_name, reason).
async fn repairs(
    c: &tokio_postgres::Client,
    queue: &str,
) -> Vec<(String, Option<String>, String)> {
    c.query(
        "SELECT consumer_group, partition_name, reason FROM queen.hotlist_repairs \
         WHERE queue_name = $1 ORDER BY consumer_group",
        &[&queue],
    )
    .await
    .expect("repairs")
    .iter()
    .map(|r| (r.get(0), r.get(1), r.get(2)))
    .collect()
}

/// The broker's own read (`db::hotlist_repairs_all`): the whole table, unordered, with
/// the timestamp as an opaque change-detector rather than a position.
async fn reader_read(c: &tokio_postgres::Client) -> Vec<(String, String, Option<String>, String)> {
    c.query(
        "SELECT tenant_id::text, queue_name, partition_name, repair_at::text \
         FROM queen.hotlist_repairs",
        &[],
    )
    .await
    .expect("reader read")
    .iter()
    .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
    .collect()
}

/// A queue with `n` partitions, all holding data, all consumed by `group` — the shape
/// where a backward cursor move is the ONLY thing that can make them pending again.
async fn seed_queue(c: &tokio_postgres::Client, queue: &str, n: i32, group: &str) {
    c.execute(
        "INSERT INTO queen.queues(name) VALUES ($1) ON CONFLICT DO NOTHING",
        &[&queue],
    )
    .await
    .expect("queue");
    c.execute(
        "INSERT INTO queen.log_partitions(queue_id,name,last_offset,log_start)
         SELECT (SELECT id FROM queen.queues WHERE name=$1), 'p'||g, 10, 0
         FROM generate_series(1,$2::int) g",
        &[&queue, &n],
    )
    .await
    .expect("partitions");
    c.execute(
        "INSERT INTO queen.log_consumers(partition_id,consumer_group,committed)
         SELECT p.id, $2, 10 FROM queen.log_partitions p
         JOIN queen.queues q ON q.id = p.queue_id WHERE q.name = $1",
        &[&queue, &group],
    )
    .await
    .expect("consumers");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn a_cursor_move_publishes_a_repair_the_peers_can_read() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // Boot the real broker purely to apply the real schema — the SQL under test is
    // include_str!-embedded, so this is also what proves the file compiled in.
    let broker = Broker::start(
        BrokerConfig::new()
            .pg(host.clone(), port, "postgres", "postgres", "postgres")
            .pool_size(4),
    )
    .await
    .expect("broker start");

    let (c, conn) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres password=postgres dbname=postgres"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let queue = unique("repq");
    let other = unique("repq-other");
    let group = "replayers";
    seed_queue(&c, &queue, 4, group).await;
    seed_queue(&c, &other, 2, group).await;
    // A queue this group never consumed: the delete removes no cursor there, so nothing
    // about its pendingness changes and it must publish nothing.
    let untouched = unique("repq-untouched");
    seed_queue(&c, &untouched, 2, "someone-else").await;

    // -------------------------------------------------- a per-partition seek is scoped
    c.execute(
        "SELECT queen.log_seek_partition_v1($1,$2,$3,true,NULL,$4::text::uuid)",
        &[&group, &queue, &"p1", &TENANT],
    )
    .await
    .expect("seek p1");
    assert_eq!(
        repairs(&c, &queue).await,
        vec![(group.to_string(), Some("p1".to_string()), "seek-partition".to_string())],
        "one repair, naming the ONE partition that moved — a reader marks exactly it \
         instead of walking 9,563"
    );

    // Seeking the same partition again is the same repair, not a second row: the table
    // is bounded by (tenant, queue, group) and must stay that way under a loop.
    c.execute(
        "SELECT queen.log_seek_partition_v1($1,$2,$3,true,NULL,$4::text::uuid)",
        &[&group, &queue, &"p1", &TENANT],
    )
    .await
    .expect("seek p1 again");
    assert_eq!(
        repairs(&c, &queue).await,
        vec![(group.to_string(), Some("p1".to_string()), "seek-partition".to_string())],
        "an upsert, and the scope is unchanged"
    );

    // ------------------------------------------------ a second partition widens to NULL
    // The primary key leaves two choices: widen, or lose one of the two repairs.
    c.execute(
        "SELECT queen.log_seek_partition_v1($1,$2,$3,true,NULL,$4::text::uuid)",
        &[&group, &queue, &"p2", &TENANT],
    )
    .await
    .expect("seek p2");
    assert_eq!(
        repairs(&c, &queue).await,
        vec![(group.to_string(), None, "seek-partition".to_string())],
        "two partitions pending under one key ⇒ the reader owes the whole queue"
    );

    // ------------------------------------------------------- a failed seek publishes nothing
    let before = repairs(&c, &other).await;
    let txt: String = c
        .query_one(
            "SELECT (queen.log_seek_partition_v1($1,$2,$3,true,NULL,$4::text::uuid))::text",
            &[&group, &other, &"no-such-partition", &TENANT],
        )
        .await
        .expect("seek missing")
        .get(0);
    assert!(txt.contains("\"success\": false"), "the seek itself failed: {txt}");
    assert_eq!(
        repairs(&c, &other).await,
        before,
        "no cursor moved, so there is nothing to repair"
    );

    // ------------------------------------------------------------ a queue-wide seek
    c.execute(
        "SELECT queen.log_seek_consumer_group_v1($1,$2,true,NULL,$3::text::uuid)",
        &[&group, &other, &TENANT],
    )
    .await
    .expect("seek queue");
    assert_eq!(
        repairs(&c, &other).await,
        vec![(group.to_string(), None, "seek-queue".to_string())],
        "queue-wide: the pending set is whatever PG says, so the reader owes a walk"
    );

    // ------------------------------------------------- the group delete, one row per queue
    c.execute(
        "SELECT queen.log_delete_consumer_group_v1($1,true,$2::text::uuid)",
        &[&group, &TENANT],
    )
    .await
    .expect("delete group");
    for q in [&queue, &other] {
        assert_eq!(
            repairs(&c, q).await,
            vec![(group.to_string(), None, "consumer-group-delete".to_string())],
            "the delete makes every partition holding data pending again for {group}"
        );
    }
    assert!(
        repairs(&c, &untouched).await.is_empty(),
        "a queue the group never consumed had nothing to lose and needs no repair"
    );

    // ------------------------------------------------------------------ the reader's read
    // The reader takes the whole table and reacts to CHANGE, so what it must find here is
    // one row per repaired (tenant, queue, group) carrying the scope and a value it can
    // compare — never an ordering, which commit order would not respect anyway.
    let seen = reader_read(&c).await;
    let ours: Vec<&(String, String, Option<String>, String)> = seen
        .iter()
        .filter(|(_, q, _, _)| q == &queue || q == &other)
        .collect();
    assert_eq!(ours.len(), 2, "one row per repaired (tenant, queue, group)");
    assert!(
        ours.iter().all(|(t, _, p, at)| t == TENANT && p.is_none() && !at.is_empty()),
        "the reader gets the tenant that owns the ring, the scope, and a change token"
    );
    // Re-reading changes nothing: the same rows with the same tokens, which is exactly
    // what stops a still-pending repair from being re-applied on every pass.
    assert_eq!(reader_read(&c).await.len(), seen.len());

    // ------------------------------------------------------------------------- the prune
    // A broker offline longer than the window cold-starts every ring it holds, so the
    // window is generous rather than load-bearing — but it does have to bound the table.
    let stale = unique("repq-stale");
    c.execute(
        "INSERT INTO queen.hotlist_repairs(tenant_id,queue_name,consumer_group,repair_at,reason) \
         VALUES ($1::text::uuid,$2,'g',now() - interval '2 hours','test')",
        &[&TENANT, &stale],
    )
    .await
    .expect("stale row");
    c.execute(
        "SELECT queen.hotlist_repair_publish_v1($1::text::uuid,$2,'g',NULL,'test')",
        &[&TENANT, &queue],
    )
    .await
    .expect("publish");
    assert!(
        repairs(&c, &stale).await.is_empty(),
        "the publish prunes what no broker can still act on"
    );

    let _ = broker.shutdown().await;
}
