mod support;
use queen_mq::{Config, Queen, SupervisionConfig};
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use support::{FakeBroker, Reply};

fn message() -> Reply {
    Reply::ok(json!({"messages":[{"id":"m","transactionId":"t","partitionId":"p","partition":"Default",
        "leaseId":"l","consumerGroup":"workers","createdAt":"2026-10-07T10:00:00Z","data":{"private":"payload"}}]}).to_string())
}
fn observations(broker: &FakeBroker) -> Vec<Value> {
    broker
        .hits()
        .iter()
        .filter(|h| h.route() == "/api/v1/kv")
        .map(|h| {
            let body = h.json();
            let ops = body["operations"].as_array().unwrap();
            assert_eq!(ops.len(), 2);
            assert_eq!(ops[0]["ns"], "queen-supervisor");
            assert_eq!(ops[0]["ttlSeconds"], 60);
            assert_eq!(ops[1]["ttlSeconds"], 60);
            assert_eq!(ops[0]["value"]["write"], ops[1]["value"]["write"]);
            let bytes =
                queen_protocol::timers::base64_decode(ops[1]["value"]["data"].as_str().unwrap())
                    .unwrap();
            assert_eq!(
                bytes.len(),
                ops[0]["value"]["bytes"].as_u64().unwrap() as usize
            );
            assert!(!String::from_utf8_lossy(&bytes).contains("private"));
            serde_json::from_slice(&bytes).unwrap()
        })
        .collect()
}

#[tokio::test]
async fn default_off_and_explicit_reporting_preserve_consumption() {
    for enabled in [false, true] {
        let broker = FakeBroker::start_with(|_, h| {
            if h.route() == "/api/v1/kv" {
                Reply::ok(r#"{"results":[{"applied":true},{"applied":true}]}"#)
            } else {
                message()
            }
        })
        .await;
        let client = Queen::connect(Config::new(broker.url())).unwrap();
        let config = enabled.then(|| SupervisionConfig::new("billing-production"));
        let result = client
            .queue("orders")
            .supervision(config)
            .auto_ack(false)
            .wait(false)
            .limit(1)
            .consume(|_| async { Ok::<_, String>(()) })
            .await
            .unwrap();
        assert_eq!(result.processed, 1);
        let docs = observations(&broker);
        if !enabled {
            assert!(docs.is_empty());
            continue;
        }
        let last = docs.last().unwrap();
        assert_eq!(last["state"], "stopped");
        assert_eq!(last["pool_status"][0]["running"], 0);
        assert_eq!(last["pool_status"][0]["completed"], 1);
        assert_eq!(last["pool_status"][0]["busy"], 0);
    }
}

#[tokio::test]
async fn failures_and_panics_are_observed_without_changing_error_semantics() {
    let broker = FakeBroker::start_with(|_, h| {
        if h.route() == "/api/v1/kv" {
            Reply::status(403)
        } else {
            message()
        }
    })
    .await;
    let client = Queen::connect(Config::new(broker.url())).unwrap();
    let builder = client
        .queue("orders")
        .supervision(Some(SupervisionConfig::new("billing")))
        .auto_ack(false)
        .wait(false)
        .limit(1);
    builder
        .consume(|_| async { Err::<(), _>("private error") })
        .await
        .unwrap();
    let first = observations(&broker).last().unwrap().clone();
    assert_eq!(first["pool_status"][0]["failed"], 1);
    let result = builder
        .consume(|_| async {
            panic!("handler panicked");
            #[allow(unreachable_code)]
            Ok::<(), String>(())
        })
        .await;
    assert!(result.is_err());
    let last = observations(&broker).last().unwrap().clone();
    assert_eq!(last["state"], "stopped");
    assert_eq!(last["pool_status"][0]["failed"], 1);
    assert_eq!(last["pool_status"][0]["running"], 0);
    assert_ne!(first["instance_id"], last["instance_id"]);
}

#[tokio::test]
async fn hanging_publication_has_a_total_deadline() {
    let broker = FakeBroker::start_with(|_, h| {
        if h.route() == "/api/v1/kv" {
            Reply::hang()
        } else {
            message()
        }
    })
    .await;
    let client = Queen::connect(Config::new(broker.url())).unwrap();
    let start = Instant::now();
    client
        .queue("orders")
        .supervision(Some(SupervisionConfig::new("billing")))
        .auto_ack(false)
        .wait(false)
        .limit(1)
        .consume_batch(|_| async { Ok::<(), String>(()) })
        .await
        .unwrap();
    assert!(start.elapsed() < Duration::from_secs(6));
    assert_eq!(
        observations(&broker).last().unwrap()["pool_status"][0]["completed"],
        1
    );
}

#[tokio::test]
async fn invalid_groups_fail_before_any_request() {
    let broker = FakeBroker::start(vec![message()]).await;
    let client = Queen::connect(Config::new(broker.url())).unwrap();
    for group in ["", "coordination", "a/b", "a\n"] {
        assert!(client
            .queue("orders")
            .supervision(Some(SupervisionConfig::new(group)))
            .consume(|_| async { Ok::<(), String>(()) })
            .await
            .is_err());
    }
    assert!(broker.hits().is_empty());
}
