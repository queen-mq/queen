# OpenMessaging Benchmark on GCP, 2026-10-16/17: Queen (native driver, Kafka facade) vs Kafka 4.3

**Rig.** GCP europe-west1-b:

- 3 brokers, c4-standard-24-lssd: 24 vCPU, local SSD;
- 3 benchmark clients, c4-highcpu-16.

**Software.**

- OMB at 5b1fa70, with the master fix (288cfbc in `~/Work/openmessaging-benchmark`).
- Kafka 4.3 in KRaft mode, `drivers/kafka.yaml`: RF 3, `acks=all`, `linger.ms=1`, `batch.size=1048576`.
- Queen: builds of the `next-release` branch, kept in `/root/p3/bin/nrN` on the brokers:
  - nr0 is the branch at the start of the night;
  - nr2 adds the fetch fix and the produce timings;
  - nr6 adds every Kafka-facade fix: topic wake, pipelined and relayed produce, and the catalog's
    plain mutex.
- Native runs use `omb/driver-queen` (`drivers/queen-native-v2*.yaml`). Dedup is on everywhere.

**Method.**

- A fresh cluster before every point (`scripts/matrix.sh`).
- 1 KB messages and 4 producers / 4 consumers unless the workload says otherwise.
- Latencies in ms: p50/p99/p99.9.
- "e2e" is OMB's end-to-end latency, publish to consume.
- Every row is in `runs/matrix.txt` and `runs/<tag>.out`.

## 1. The Kafka facade, before and after the night's fixes (1 topic, 100 partitions)

| Rate | Build | Carried | pub p50/p99/p99.9 | e2e p50/p99/p99.9 |
|------|-------|---------|-------------------|-------------------|
| 500k | nr0 (before) | 501k | 8.6 / 26 / 35 | 46 / 625 / 792 |
| 500k | nr6 | 501k | 2.7 / 4.8 / 5.7 | 5 / 20 / 58 |
| 1M | nr4 (pipelined, before the mutex fix) | 945k | 62 / 331 / 477 | 76 / 365 / 508 |
| 1M | nr6 | **1,001k** | 5.0 / 11.5 / 17.6 | 14 / 87 / 151 |
| 1M | Kafka 4.3, same rig | 690k (not carried) | 5.9 / 1,427 / 2,159 | 20 / 1,445 / 2,172 |

What made the difference:

- produce pipelined per connection, in order (commit c7cfd09af);
- followers relaying produce to the leader's facade;
- the catalog's queue list behind a plain mutex instead of a fair tokio mutex (86453615c). The fair
  mutex's convoy had made every produce plan cost 7.4 ms on the leader.

What remains: the facade's e2e p99 at 500k is 20 ms, against 4 ms for Kafka and 7 ms for the native
driver (fetch side).

## 2. Native driver vs Kafka, fixed rate (family 1)

1 topic, 4 producers / 4 consumers, 1 KB.

| Rate | Queen 100 partitions (e2e) | Kafka 100 partitions (e2e) | Queen 10,000 partitions (e2e) | Kafka 10,000 partitions |
|------|---------------|---------------|------------------|---------------|
| 200k | 3 / 4 / 5 | 1 / 3 / 7 | 3 / 4 / 5 | cannot run (below) |
| 500k | 4 / 7 / 8 | 2 / 4 / 50 | 5 / 7 / 8 | cannot run |
| 1M | 18 / 38 / 48 | **690k carried**, 20 / 1,445 / 2,172 | 16 / 32 / 40 | cannot run |
| 2M | 1,285k carried, 67 / 191 / 206 | 1,161k carried, 44 / 781 / 1,126 | 1,330k carried, 147 / 204 / 218 | cannot run |

- Builds: Queen at 100 partitions is nr0 for 500k and 1M, nr2 for 200k and 2M; at 10,000 partitions,
  nr2.
- Brokers' CPU at 500k: Queen 11 cores, Kafka 5 cores (sum over the 3 brokers).
- Kafka with 10,000 partitions: Kafka 4.3 KRaft refuses OMB's single CreateTopics of 10,000 partitions
  (`BoundedListTooLongException`: more than 10,000 records in one controller operation). OMB retries
  until its 600 s timeout, so this is not a measurement of Kafka's speed.
- **Recheck, Kafka at 1M with 8 producers / 8 consumers: carried 1,001k**, pub 1.5 / 3.9 / 55,
  e2e 2 / 13 / 107 ms, brokers 10.8 cores, Pub Delay p50 31 us. So Kafka's 690k with 4 producers was
  a client limit: at 1M, Kafka's p50 and p99 beat Queen's (18 / 38), and Queen's p99.9 is lower (48
  against 107).
- **Recheck with 16 producers / 16 consumers: carried 1,002k**, pub 1.3 / 3.1 / 33.5, e2e 2 / 4 / 90 ms,
  brokers 14.5 cores.
- **Recheck with 4 producers / 4 consumers and 128 KB batches** (`drivers/kafka-b128k.yaml`, OMB's
  `kafka-latency.yaml` value): **carried 1,002k**, pub 1.1 / 7.7 / 93.1, e2e 2 / 9 / 96 ms, brokers
  11.9 cores. The cause was OMB's stock `kafka-throughput.yaml` batch size (1 MB), not the producer
  count.
- **Queen at 1M with 8 producers / 8 consumers:** e2e 18 / 39 / 51 ms, the same as with 4 (pub
  8.0 / 22.2 / 31.0, brokers 15.9 cores). Queen's latency at 1M is set by the broker, not the clients.
- **Kafka at 1M is not a broker limit, so this row is not a fair win.** Its 3 brokers used 3.7
  cores, but OMB's "Pub Delay" (how late the producers sent against the schedule) was p50 12 s and
  p95 60 s, against Queen's 31 us. The 4 Kafka producers were blocked in `send()`.
  - `drivers/kafka.yaml` is OMB's stock `kafka-throughput.yaml`: `batch.size` 1 MB, with the default
    32 MB `buffer.memory` per producer.
  - Public runs reach 1 GB/s with Kafka on comparable machines, with more producers and 128 KB
    batches (Vanlightly 2023: Kafka 3.4, 288 partitions, 50 producers, `batch.size=131072`).
  - Rechecks queued in `runs/points-final.txt`: Kafka at 1M with 8p/8c, with 16p/16c, and with 4p/4c
    but 128 KB batches (`drivers/kafka-b128k.yaml`); Queen at 1M with 8p/8c.
  - Family 4's Kafka rows use the same producer config.

## 3. Max rate (family 3), messages per second produced / consumed

| Workload | Queen (pop autopilot) | Queen `popBatch` 1,000 | Queen `popBatch` 10,000 | Kafka |
|----------|-------------------|------------------|-------------------|-------|
| 1 topic, 1 partition, 1p/1c, 100 B | 466k / 18k | 475k / 19k | 481k / **182k** | 1,978k / 1,978k |
| 1 topic, 1 partition, 1p/1c, 1 KB | 363k / 65k | 362k / 70k | 355k / **293k** | 420k / 394k |
| 1 topic, 1 partition, 1p/1c, 64 KB | 11k / 6k | 11k / 6k | 11k / 6k | 7k / 7k |
| 1 topic, 100 partitions, 100p/100c, 100 B | 4,226k / 4,227k | | | 6,923k / 6,922k |
| 1 topic, 100 partitions, 100p/100c, 1 KB | 1,591k / 1,440k | | | 1,311k / 1,311k |
| 1 topic, 100 partitions, 100p/100c, 64 KB | 24k / 24k | | | 21k / 21k |
| 10 topics, 1 partition each, 1 KB | 1,154k / 215k | | | 1,305k / 1,305k |

- With one partition, Queen's consumer falls behind. A partition is leased to one batch at a time, so
  one consumer does one pop, deliver and ack cycle at a time.
- `popBatch` 10,000 makes those batches 10x bigger, and consumption rises 4.5x at 1 KB and 10x at
  100 B. It still does not reach Kafka. `popBatch` 1,000 is no better than the autopilot.
- Where the consumer falls behind, the e2e column of `runs/matrix.txt` measures the backlog's age, not
  delivery.

## 4. Kafka facade across a leader change (nr6, `failover/`)

The test, `failover/run.sh`:

- `kfail` runs 4 idempotent franz-go producers: `acks=all`, 5 requests in flight, 1 KB records.
- They produce 200k msg/s for 90 s into 100 partitions (RF 3) through the facade, from one client.
- The raft leader is stopped at 30 s and started again at 50 s.
- Then every partition is read back. Each record carries its producer's sequence number, so the check
  counts acknowledged records that are missing (lost), records read twice (duplicated) and records
  out of their producer's order (reordered).

| Leader stopped by | Acknowledged | Lost | Duplicated | Reordered | Throughput dip |
|-------------------|-------------:|-----:|-----------:|----------:|----------------|
| `kill -9` | 17,784,150 | 0 | 38 (one batch, one partition) | 0 | 49k/s for one 5 s sample, then catch-up at 267k/s |
| graceful stop (`qc.sh stop`) | 17,778,900 | 0 | 0 | 0 | none visible (194k/s in the 5 s of the stop) |

The duplicates are the documented bound of the facade's idempotence (`guides/kafka.mdx`, "Idempotent
producer"). The sequence windows live in the leader's memory, so after a leader change a client's
in-flight batches, at most 5 per producer and partition, can be written twice. Kafka keeps that state
in its log, so its idempotent producer is designed to write none twice here. Kafka was not run
through this test.

## 5. 1M msg/s on 10 topics, 100 B (family 4)

| Partitions per topic | Queen produced / consumed | Queen pub p50/p99/p99.9 | Kafka produced / consumed | Kafka e2e p50/p99/p99.9 |
|---------------------:|---------------------------|--------------------|---------------------------|---------------------|
| 1 | 1,002k / **473k** (backlog grows) | 7.2 / 24.8 / 32.1 | 1,001k / 1,001k | 1 / 2 / 4 |
| 16 | 1,001k / 1,002k, e2e 4 / 7 / 9 (brokers 20.0 cores) | 1.9 / 2.8 / 3.4 | 1,001k / 1,001k (brokers 8.4 cores) | 1 / **450** / 500 |

- With one partition per topic, Queen's consumers hit the same per-partition limit as in section 3.
  With `popBatch` 10,000 they keep up (1,001k / 1,001k), but e2e becomes 246 / 1,059 / 1,332 ms: a
  batch of 10,000 is about 100 ms of one partition's traffic (pub 6.7 / 23.3 / 30.7, brokers 6.5
  cores).
- With 16 partitions per topic, both carry 1M. Queen's tail is lower (e2e p99 7 ms against Kafka's
  450 ms), but it uses 2.4x the broker CPU.

## 6. Backlog (family 5): 100 GB built with the consumers stopped, then drained

- The producers run at 100k msg/s throughout.
- OMB stops the consumers until 100 GB of 1 KB messages are waiting, then restarts them.
- The drain time is OMB's own "Completed backlog draining in" line.

| Workload | Queen build / drain | Kafka build / drain |
|----------|-------------------|-------------------|
| 1 topic, 16 partitions | 1,049 s / **32 s** (104.7M messages, ~3.5M msg/s, 3.4 GB/s) | rerun, retention off: drained in **143 s** |
| 1 topic, 1 partition | not run: with the default pops one partition consumes 65k msg/s, below the 100k msg/s producer, so it never drains (`popBatch` 10,000's 293k would; no slot was left to run it) | rerun, retention off: drained in **326 s** |

- **Kafka, first attempt not valid.** The harness ran Kafka with `log.retention.ms=600000` (10
  minutes). Building the 100 GB backlog takes 17.5 minutes, so Kafka deleted 41.4M of the 104.7M
  messages before the consumers resumed (server.log: "Deleting segment files"). The run was stopped at
  13:56Z.
- **Reruns** `kafka-backlog-1t-16p-ret` and `kafka-backlog-1t-1p-ret` used `log.retention.ms=-1`, as
  Queen keeps everything until it is consumed. Both built 105M messages and drained them, the 16
  partitions in 143 s and the one partition in 326 s, with 0.9 broker cores while the producers ran.
- **Queen's 16-partition run is valid:** 80 lease extensions, 0 refused, and pushed = delivered =
  acked.
- **Caveat for the comparison:** OMB's Kafka consumer is one `KafkaConsumer`, polling on one thread.
  The Queen driver keeps a pool of pops in flight, with several delivery threads.

## 7. Many topics (family 2): 500,000 msg/s of 1 KB, 4 producers / 4 consumers

Queen ran with `QUEEN_QLOG_SHARDS=16` and the driver's `-topics.yaml` (`maxPops` 12,000).

| Topics | Queen produced / consumed, e2e p50/p99/p99.9, broker cores | Kafka |
|-------:|-------------------------------------------------------------|-------|
| 100 | 501k / 496k: a topic or two fell behind (e2e p99 35 s), 21.9 cores; **with `popBatch` 10,000: 501k / 501k, 29 / 481 / 714 ms, 21.4 cores** | 502k / 502k, 5 / 101 / 292 ms, 38.1 cores |
| 1,000 | 501k / 501k, 365 / 477 / 822 ms, 37.1 cores; with `popBatch` 10,000: 444 / 527 / 940 ms, 25.9 cores | broke 10 s into the warm-up: a producer worker running about 1,333 KafkaProducers answered OMB with HTTP 500 (a client limit) |
| 10,000 | not carried: 59k, brokers at 47 of 72 cores, pushes waiting 40 s | not run |
| 100,000 | not carried: 25k, 1.46M messages in failed pushes | not run |

## 8. At 2M msg/s, why neither carried it (family 1)

- **Queen** (1,285k on 100 partitions, 1,330k on 10,000) ran at 16 to 17 of 72 broker cores and 625
  to 645 MB/s a node. It stopped at the 4 producers' push lanes: the publish p50 of 57 to 136 ms is
  time waiting in the lane's buffer.
- **Kafka** (1,161k) was at the RAID's ~1.3 GB/s a node.

## 9. Pending

- The no-regression A/B of the released 2.4.0 against 2.5.0 with qload, `../2026-10-17-ab-24-25/`.
