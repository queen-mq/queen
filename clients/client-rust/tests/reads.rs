//! Reads by offset (`fetch`) and partition discovery (`changed_since`) against
//! a live broker.

mod common;

use std::time::{Duration, SystemTime};

use queen_mq::{FetchOptions, PushItem, PushStatus, Queen, Since};
use serde_json::json;

use common::*;

async fn push_each(q: &Queen, queue: &str, items: Vec<PushItem>) {
    for chunk in items.chunks(500) {
        let results = q.queue(queue).push_items(chunk.to_vec()).await.unwrap();
        assert!(
            results.iter().all(|r| r.status == PushStatus::Queued),
            "{results:?}"
        );
    }
}

#[tokio::test]
async fn fetch_reads_every_record_by_offset() {
    let q = broker!();
    let queue = unique("reads-fetch");

    // 1,200 partitions: more than one fetch call (1000 entries each).
    let partitions: Vec<String> = (0..1200).map(|i| format!("entity-{i}")).collect();
    let per_partition = 3i64;
    for n in 0..per_partition {
        let items = partitions
            .iter()
            .map(|p| {
                PushItem::new(&queue, json!({ "p": p, "n": n }))
                    .partition(p.clone())
                    .transaction_id(format!("{p}-{n}"))
            })
            .collect();
        push_each(&q, &queue, items).await;
    }

    let read = q.queue(&queue).fetch(partitions.clone()).await.unwrap();
    assert_eq!(read.len(), partitions.len());
    for p in &partitions {
        let part = &read[p];
        let got: Vec<(i64, serde_json::Value, String)> = part
            .records
            .iter()
            .map(|r| (r.offset, r.payload.clone(), r.transaction_id.clone()))
            .collect();
        let want: Vec<(i64, serde_json::Value, String)> = (0..per_partition)
            .map(|n| (n, json!({ "p": p, "n": n }), format!("{p}-{n}")))
            .collect();
        assert_eq!(got, want, "{p}");
        assert_eq!(
            (part.high_watermark, part.log_start_offset, part.next_offset),
            (per_partition, 0, per_partition)
        );
    }

    // A later read from next_offset answers only what came after.
    q.queue(&queue)
        .partition("entity-7")
        .push(json!({ "late": true }))
        .await
        .unwrap();
    let later = q
        .queue(&queue)
        .fetch_with(
            ["entity-7", "entity-8"],
            FetchOptions::new()
                .offset("entity-7", read["entity-7"].next_offset)
                .offset("entity-8", read["entity-8"].next_offset),
        )
        .await
        .unwrap();
    let late = &later["entity-7"].records;
    assert_eq!(late.len(), 1);
    assert_eq!(
        (late[0].offset, &late[0].payload),
        (per_partition, &json!({ "late": true }))
    );
    assert!(later["entity-8"].records.is_empty());

    // The reads took no lease and moved no cursor: a pop still gets the messages.
    let popped = q
        .queue(&queue)
        .partition("entity-9")
        .batch(10)
        .wait(false)
        .pop()
        .await
        .unwrap();
    assert_eq!(popped.len(), per_partition as usize);

    let missing = q.queue(unique("reads-missing")).fetch(["x"]).await.unwrap();
    assert!(missing["x"].records.is_empty());
}

#[tokio::test]
async fn changed_since_lists_the_written_partitions() {
    let q = broker!();
    let queue = unique("reads-changed");
    let before = SystemTime::now() - Duration::from_secs(60);

    let names: Vec<String> = (0..1100).map(|i| format!("c-{i}")).collect();
    let items = names
        .iter()
        .map(|p| PushItem::new(&queue, json!({ "p": p })).partition(p.clone()))
        .collect();
    push_each(&q, &queue, items).await;

    // 1,100 partitions: two pages.
    let first = q.queue(&queue).changed_since(before).await.unwrap();
    let mut listed: Vec<&str> = first.partitions.iter().map(|p| p.name.as_str()).collect();
    listed.sort_unstable();
    let mut want: Vec<&str> = names.iter().map(String::as_str).collect();
    want.sort_unstable();
    assert_eq!(listed, want);
    assert!(!first.safe_time.is_empty());
    let one = first.partitions.iter().find(|p| p.name == "c-5").unwrap();
    assert_eq!((one.last_offset, one.log_start), (0, 0));
    assert!(one.id.is_some() && !one.last_write_at.is_empty());

    // From the safe_time of that pass, a write to one partition lists it, and
    // nothing written before that safe_time comes back.
    q.queue(&queue)
        .partition("c-42")
        .push(json!({ "again": true }))
        .await
        .unwrap();
    let next = q
        .queue(&queue)
        .changed_since(first.safe_time.as_str())
        .await
        .unwrap();
    assert!(next
        .partitions
        .iter()
        .any(|p| p.name == "c-42" && p.last_offset == 1));
    assert!(next
        .partitions
        .iter()
        .all(|p| p.last_write_at >= first.safe_time));

    let all = q.queue(&queue).changed_since(Since::All).await.unwrap();
    assert_eq!(all.partitions.len(), names.len());
    let none = q
        .queue(unique("reads-missing"))
        .changed_since(before)
        .await
        .unwrap();
    assert!(none.partitions.is_empty());
}
