//! End-to-end SQL contract for group-scoped `deliveryAttempt`.
//!
//! The ordinary unit test in `handlers::data` pins metadata parsing and the
//! final HTTP renderer. This test boots the real embedded schema and exercises
//! every SQL assembly family, including the hot-list fast path, while forcing
//! lease expiry between claims. It is ignored without a throwaway Postgres:
//!
//! ```bash
//! docker run --rm -d --name queen-attempt-pg -e POSTGRES_PASSWORD=postgres -p 5481:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5481 cargo test --test delivery_attempt_semantics -- --ignored --nocapture
//! ```

use queen::protocol as qp;
use queen::{Broker, BrokerConfig};
use serde_json::Value;

const TENANT: &str = "00000000-0000-0000-0000-000000000001";

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

fn parse(row: &tokio_postgres::Row) -> Value {
    serde_json::from_str(&row.get::<_, String>(0)).expect("valid pop metadata")
}

fn partition_attempt(meta: &Value) -> i64 {
    meta["partitions"][0]["deliveryAttempt"]
        .as_i64()
        .unwrap_or_else(|| panic!("missing partition deliveryAttempt: {meta}"))
}

async fn expire_group(client: &tokio_postgres::Client, group: &str) {
    client
        .execute(
            "UPDATE queen.log_consumers \
             SET lease_expires_at = clock_timestamp() - interval '1 second' \
             WHERE consumer_group = $1",
            &[&group],
        )
        .await
        .expect("expire group lease");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn delivery_attempt_is_group_scoped_across_every_pop_path() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // Starting the real broker applies the include_str!-embedded procedure
    // file, so this also proves the edited PL/pgSQL compiles from a clean boot.
    let broker = Broker::start(
        BrokerConfig::new()
            .pg(host.clone(), port, "postgres", "postgres", "postgres")
            .pool_size(4),
    )
    .await
    .expect("broker start");

    let (client, connection) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres password=postgres dbname=postgres"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let namespace = unique("attempt-ns");
    let queue = format!("{namespace}.job");
    broker
        .push(vec![qp::PushItem::new(
            queue.clone(),
            serde_json::json!({"job": 1}),
        )])
        .await
        .expect("push");

    let partition = "Default".to_string();
    let tenant = TENANT.to_string();
    let group_a = unique("attempt-a");

    // Specific route: first delivery is 1.
    let worker_1 = unique("worker");
    let first = client
        .query_one(
            "SELECT queen.log_pop_specific_v1(\
                 $1,$2,$3,10,60,$4,false,'all','',$5::text::uuid,false)::text",
            &[&queue, &partition, &group_a, &worker_1, &tenant],
        )
        .await
        .expect("specific pop");
    let first = parse(&first);
    assert_eq!(first["deliveryAttempt"], 1, "{first}");

    // Binary wildcard route: the same cursor after lease expiry is delivery 2.
    expire_group(&client, &group_a).await;
    let worker_2 = unique("worker");
    let second = client
        .query_one(
            "SELECT meta::text FROM queen.log_pop_wildcard_bin_v1(\
                 $1,$2,10,60,$3,false,8,'all','',$4::text::uuid,false)",
            &[&queue, &group_a, &worker_2, &tenant],
        )
        .await
        .expect("wildcard binary pop");
    let second = parse(&second);
    assert_eq!(partition_attempt(&second), 2, "{second}");

    // Hot-list route: existing consumer rows take the batched fast path, whose
    // UPDATE ... RETURNING must expose the value it atomically incremented.
    expire_group(&client, &group_a).await;
    let worker_3 = unique("worker");
    let third = client
        .query_one(
            "SELECT meta::text FROM queen.log_pop_list_v1(\
                 $1,$2,ARRAY['Default']::text[],10,60,$3,false,8,\
                 'all','',false,$4::text::uuid,false)",
            &[&queue, &group_a, &worker_3, &tenant],
        )
        .await
        .expect("hot-list pop");
    let third = parse(&third);
    assert_eq!(partition_attempt(&third), 3, "{third}");
    let stored: i32 = client
        .query_one(
            "SELECT c.attempt_count FROM queen.log_consumers c \
             JOIN queen.log_partitions p ON p.id = c.partition_id \
             JOIN queen.queues q ON q.id = p.queue_id \
             WHERE q.name = $1 AND c.consumer_group = $2",
            &[&queue, &group_a],
        )
        .await
        .expect("stored attempt")
        .get(0);
    assert_eq!(stored, 3, "wire count must equal the group row");

    // autoAck has no lease/redelivery episode, even if this group has old
    // attempt telemetry from earlier leased claims.
    expire_group(&client, &group_a).await;
    let worker_4 = unique("worker");
    let auto = client
        .query_one(
            "SELECT queen.log_pop_specific_v1(\
                 $1,$2,$3,10,60,$4,true,'all','',$5::text::uuid,false)::text",
            &[&queue, &partition, &group_a, &worker_4, &tenant],
        )
        .await
        .expect("autoAck specific pop");
    let auto = parse(&auto);
    assert_eq!(auto["deliveryAttempt"], 1, "{auto}");

    // A different group starts at 1 although group A reached 3: the count is
    // never a message-global or partition-global property.
    let group_b = unique("attempt-b");
    let worker_b = unique("worker");
    let other_group = client
        .query_one(
            "SELECT queen.log_pop_wildcard_wire_v1(\
                 $1,$2,10,60,$3,false,8,'all','',$4::text::uuid,false)::text",
            &[&queue, &group_b, &worker_b, &tenant],
        )
        .await
        .expect("wildcard wire pop");
    let other_group = parse(&other_group);
    assert_eq!(partition_attempt(&other_group), 1, "{other_group}");

    // Discovery has its own assembly function and must carry the same field.
    let group_c = unique("attempt-c");
    let worker_c = unique("worker");
    let empty_task = String::new();
    let discovered = client
        .query_one(
            "SELECT queen.log_pop_discover_wire_v1(\
                 $1,$2,$3,10,60,$4,false,8,'all','',$5::text::uuid,false)::text",
            &[&namespace, &empty_task, &group_c, &worker_c, &tenant],
        )
        .await
        .expect("discovery pop");
    let discovered = parse(&discovered);
    assert_eq!(partition_attempt(&discovered), 1, "{discovered}");

    broker.delete_queue(&queue).await.expect("cleanup queue");
    assert_eq!(broker.shutdown().await, 0);
}
