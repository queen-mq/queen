# driver-queen: an OMB driver for Queen's native HTTP protocol

An [OpenMessaging Benchmark](https://github.com/openmessaging/benchmark) driver that talks to Queen
over its own HTTP protocol: binary push (`QPB1`), framed pop (`QPF1`) with pop autopilot, and
asynchronous batch acks. Built against OMB `5b1fa70` and tested against Queen `2.4.0`.

| File | What it is |
|---|---|
| `driver-queen/` | The OMB module: `pom.xml`, `src/main/java/io/openmessaging/benchmark/driver/queen/`, the sample `queen.yaml` |
| `omb-5b1fa70-driver-queen.patch` | The pom edits, against a pristine `5b1fa70`: the root-pom module line, plus the framework's dependencies on `driver-queen`, `netty-all` and `log4j-core` |
| `omb-5b1fa70-trimmed-build.patch` | The build that was tested: the same edits, with the framework also trimmed to `driver-kafka` + `driver-queen` (the 13 other drivers dropped, for a fast build) |

## Graft and build

```sh
git clone https://github.com/openmessaging/benchmark omb && cd omb && git checkout 5b1fa70
cp -R <this dir>/driver-queen .
git apply <this dir>/omb-5b1fa70-trimmed-build.patch   # or omb-5b1fa70-driver-queen.patch to keep every driver
docker run --rm -v "$PWD":/omb -v omb-m2:/root/.m2 -w /omb maven:3.9.9-eclipse-temurin-17 \
  mvn -q -B -pl package -am -DskipTests -Dspotless.check.skip=true -Dspotless.apply.skip=true \
  -Dlicense.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Djacoco.skip=true package
```

The result is `package/target/openmessaging-benchmark-0.0.1-SNAPSHOT-bin.tar.gz`. Its `lib/` gains
`io.openmessaging.benchmark-driver-queen-0.0.1-SNAPSHOT.jar`, `io.airlift-aircompressor-0.27.jar`
(pure-Java zstd) and `org.apache.logging.log4j-log4j-core-2.20.0.jar`, and the tarball also holds
`driver-queen/queen.yaml`. The trimmed patch was built that way from a fresh `5b1fa70` checkout. The
full patch was only checked with `git apply --check`, because building it downloads every driver.

The edits by hand, if a patch does not apply:

1. Root `pom.xml`: add `<module>driver-queen</module>` to `<modules>`, after `driver-kafka`.
2. `benchmark-framework/pom.xml`: add a dependency on `${project.groupId}:driver-queen:${project.version}`,
   after `driver-kafka`.
3. `benchmark-framework/pom.xml`: add `io.netty:netty-all` and `org.apache.logging.log4j:log4j-core`.
   Both versions are managed by the root pom. The framework uses Netty itself, and
   log4j-core is the log backend. Without log4j-core OMB prints nothing, not even its
   `Pub rate ... | E2E` lines. Pristine OMB gets both through other drivers, so a trimmed build
   needs them named.

To run it, put the brokers in `urls`, then `bin/benchmark --drivers driver-queen/queen.yaml <workload.yaml>`,
or use the distributed workers exactly as for Kafka.

## Configuration (`queen.yaml`)

Every key is optional. Unknown keys are ignored.

| Key | Default | Meaning |
|---|---|---|
| `urls` | `[http://localhost:6632]` | Broker base URLs. Each producer and each poller picks the next one, round-robin from a random start, so several workers do not all begin at `urls[0]`. Followers forward to the leader. |
| `token` | none | Sent as `Authorization: Bearer <token>`. |
| `dedupWindowSeconds` | `60` | The queue's dedup window. Dedup stays on: a `0` here is logged as a warning. |
| `queueOptions` | `{}` | Further `/api/v1/configure` options, passed as they are (for example retention). `dedupWindowSeconds` wins over a copy in here. |
| `topicPrefix` | `omb` | `getTopicNamePrefix()` is this plus `-` and 6 random characters, one per driver instance: the key of the run's KV entry (see below). |
| `partitionsPerTopic` | `0` | Fallback partition count for a producer whose worker did not create the topic and cannot read the count from KV (see below). `0` = fail. |
| `setupConcurrency` | `64` | `configure` and KV calls in flight while topics, producers and consumers are created. |
| `lingerMs` | `1` | Kafka `linger.ms`. `0` = send as soon as a slot is free. |
| `batchMaxBytes` | `1048576` | Kafka `batch.size`. A batch this full is sent at once. |
| `producerLanes` | `0` | Accumulators ("lanes") the worker's producers share. `0` = one per producer, at most one per core. |
| `maxInFlight` | `5` | Pushes in flight per lane. |
| `bufferMaxBytes` | `67108864` | Kafka `buffer.memory`, per lane: above it `sendAsync` blocks... |
| `maxBlockMs` | `60000` | ...for at most this long, then fails the message. |
| `compression` | `none` | `none` = binary push. `zstd` = client-framed groups (flag 8), compressed here; the broker stores them as they are. |
| `compressionThreads` | `0` (half the cores) | Threads that compress batches when `compression: zstd`. |
| `pollers` | `2` | Pops in flight per consumer. A subscription gets `pollers` x its consumers on this worker, at most one per partition of the topic, scaled down to its share of `maxPops`. |
| `popBatch` | `0` | `0`: the broker's pop autopilot sizes each pop and every message gets its ack item. `N > 0`: every pop asks `batch=N` (at most 10,000) without the autopilot, and each claim is acked with one item, its last message. |
| `maxPops` | `4096` | Pops in flight on this worker, all subscriptions together. Each is one HTTP connection. |
| `popTimeoutMs` | `1000` | The long poll's `timeout` while every subscription has its pops. |
| `rotationWaitMs` | `200` | The long poll's `timeout` when the subscriptions want more pops than `maxPops`: the pops then rotate over them. |
| `deliveryThreads` | `0` (cores) | Threads that decode pop answers and call OMB. |
| `leaseSeconds` | `0` | The pop's `leaseSeconds`. `0` = the queue's `leaseTime` (300 s on a queue made by `configure`). |
| `renewLeases` | `true` | Extend the lease of a batch not acked a third of its lease after its pop, until its ack is answered. |
| `ackMaxInFlight` | `16` | Ack requests in flight per consumer group on this worker. Acks that find them all taken wait and share the next request. |
| `ackBatchMax` | `8192` | Ack items in one request (whole pops). |
| `subscriptionMode` | `all` | Where a new consumer group starts (`all` or `new`). |
| `verify` | `false` | The consumer counts deliveries per (partition id, offset), then logs at close how many messages came twice and how many offsets are missing between the first and last delivered. A correctness check; it costs a little per message. |
| `ioThreads` | `0` (client default, 2 x cores) | Netty event-loop threads. |
| `requestTimeoutMs` | `30000` | Timeout of a push or an ack. A pop gets `popTimeoutMs` on top. |
| `connectTimeoutMs` | `10000` | |

## What it sends

The HTTP client is AsyncHttpClient 2.12.3, which the framework already ships (Netty, HTTP/1.1
keep-alive). Automatic retries are off: a failed request is never sent twice. Response bodies are
copied once, straight into a reusable array.

- **Topic**: a Queen queue. `createTopic(topic, P)` sends `POST /api/v1/configure`
  `{"queue":T,"options":{...queueOptions,"dedupWindowSeconds":60}}`; everything else stays at the
  queue defaults. `createTopics` runs `setupConcurrency` of them at a time (OMB's default would start
  them all at once).

  Queen partitions are names, so a producer has to know P to name `"0" .. "P-1"`. Kafka's producer
  learns P from metadata. In a distributed run, OMB creates the topics on the first worker only, all
  with one P and named `<prefix>-%07d-<7 random characters>`. So `createTopics` also writes ONE KV
  entry for the run, `PUT /api/v1/kv/omb/<prefix>` = `{"value":{"partitions":P},"ttlSeconds":2592000}`,
  and the other workers read it once (the prefix is the name less its last 16 characters). One
  entry per topic would not do: KV takes 100 writes and 200 reads per second per tenant
  (`QUEEN_KV_WRITE_RATE`, `QUEEN_KV_READ_RATE`), 1,000 s of writes at 100,000 topics. A topic made
  alone by `createTopic` still gets its own `omb/<topic>` entry. A reader retries for up to 10 s,
  then falls back to `partitionsPerTopic`.
- **Producer**: the worker's producers share lanes: an OMB producer is a small handle on its topic
  and its lane, and a push carries every topic and partition its lane holds (one group per queue
  and partition). Producer i of OMB's list gets lane i mod lanes, and OMB gives producer i to its
  thread i mod cores, so with one lane per core each thread has its own lane. A lane is an
  accumulator like Kafka's. A batch becomes sendable when it reaches `batchMaxBytes` (it closes at
  once) or when `lingerMs` has passed since its first message. A sendable batch goes out when fewer
  than `maxInFlight` pushes of the lane are in flight, and until then keeps taking messages, so a
  slow broker gets bigger batches.
  - Without a key, a producer writes one partition per batch of its lane, and the next batch the next partition (sticky).
  - With a key, the partition is a stable hash of it.
  - The wire form is `"QPB1" | u8 2 | u32 groups | per group: u16 queue | u16 partition | u32 n | n x (u32 len | payload)`.
    Flag 2 makes the answer only the counts. The broker mints the ids.
  - With `compression: zstd` it is flags `8|2`. Each group is one zstd frame of client frames:
    `u32 len | u8 0 | uuidv7 | u16 36 | the id's text | payload`.
  - A message's future completes when its push is answered. An HTTP or transport error, or a count
    that does not match, completes it exceptionally, and OMB counts it.
- **Message**: one JSON document that carries the send time, because OMB's end-to-end latency is
  receive time minus the publish timestamp, and Kafka hands over the producer's send time.
  - `{"t":<System.currentTimeMillis() at sendAsync>,"v":"<payload>"}` when the payload is printable
    ASCII without `"` or `\`. OMB's payload files are hex text.
  - `{"t":...,"b":"<base64>"}` otherwise, which includes `useRandomizedPayloads`.
  - The consumer reads `t` in place and hands OMB the original bytes: a view for `v`, a base64
    decode into a reusable buffer for `b`. A document it does not recognise goes through Jackson,
    and without a `t` it gets the time the broker stored it.
- **Consumer**: an OMB subscription is a Queen consumer group, named by the subscription's index:
  OMB's `sub-000-<random>` becomes `sub-000`. OMB randomizes the name because a Kafka group id is
  cluster-wide; a Queen group is scoped by its queue, so the index alone keeps a topic's
  subscriptions apart, and one ack request (whose body names one group) can carry the acks of every
  queue. A consumer owns no thread and no connection: the worker has one pool of pops.
  1. A subscription (queue, group) with consumers on this worker wants `pollers` x those consumers
     pops in flight, at most one per partition (a partition is leased to one pop at a time: more
     only park), scaled down to its share of `maxPops`.
  2. While the wants fit in `maxPops`, every subscription keeps its pops parked on the broker:
     `GET /api/v1/pop/queue/{queue}?wait=true&timeout=1000&consumerGroup={group}&autoAck=false&subscriptionMode=all&frames=true&autopilot=true`
     (`&leaseSeconds=` when set). No `batch` and no `partitions`, so the broker's autopilot sizes the pop.
     Past `maxPops`, the pops rotate over the subscriptions in FIFO order with `timeout=rotationWaitMs`.
  3. The answer goes to a delivery thread (never a Netty thread: OMB blocks its callback while it
     pauses the consumers). Claims, then records (zstd ones are decompressed), then frames; only
     offsets inside the claim's `start..=end` count. A JSON answer is read too. Every message goes
     to OMB, then the pop is over and the subscription goes to the back of the line.
  4. The acks go to the group's acker: `POST /api/v1/ack/batch`, one `completed` item per message
     (`transactionId`, `partitionId` as text, `leaseId`), plus `"consumerGroup"` and
     `"answer":"counts"`. A pop's items are sent at once while fewer than `ackMaxInFlight` requests
     of the group are in flight; otherwise they wait and share the next request.
  5. A batch not acked a third of its lease after its pop gets `POST /api/v1/lease/{leaseId}/extend`
     `{"seconds":<lease>}`, every third of the lease, until its ack is answered. OMB's backlog
     workloads hold the consumers for as long as the backlog takes to build, by blocking the
     callback; without the extension the held batches came back after their lease (see below).

  204, or an empty body, means nothing was there. The autopilot's `waitMs` only concerns non-waiting
  pops, and these always wait.

  Acking only the last message of each claim would settle the claim as well (concepts/consuming:
  "the earlier messages of the batch are done with it"). But the broker's autopilot reads the ack
  items per lease as the consumer's drain rate. Measured on 10-16: with one item per claim it shrank
  the batches from 1000 to about 240 and consumed 3x slower. So the driver acks every message.
- When the driver closes it logs its own totals, for example
  `Queen driver closed: pushes ..., messages pushed N (failed 0); pops ...; messages delivered N; acks ..., messages acked N (refused 0)`.
  These are a cross-check of OMB's numbers.

## Checked locally (10-16)

The setup was one Queen 2.4.0 node (`ghcr.io/queen-mq/queen:2.4.0-arm64`) and OMB inside
`maven:3.9.9-eclipse-temurin-17`, on one Docker network on the Mac. Each workload was 1 topic, 100
partitions, 1 KB, 1 minute of warm-up and 1 minute of test, with `verify: true`.

| Workload | Publish msg/s | Consume msg/s | Errors | Publish p50 / p99 (ms) | End-to-end p50 / p99 (ms) | Best 10-s window, publish / e2e p99 (ms) | Driver totals (whole run: probe, warm-up, test) |
|---|---|---|---|---|---|---|---|
| 10k msg/s, 1 producer, 1 consumer, `payload-1Kb.data` | 10,166 | 10,158 | 0 | 2.6 / 448 | 5 / 488 | 22 / 36 | 1,241,324 pushed = delivered = acked; 0 delivered twice, 0 holes |
| 50k msg/s, 1 producer, 1 consumer, `payload-1Kb.data` | 50,817 | 50,816 | 0 | 2.1 / 629 | 3 / 1,063 | 6 / 10 | 6,207,291 pushed = delivered = acked; 0 delivered twice, 0 holes |
| 50k msg/s, 4 producers, 4 consumers, randomized payloads (ratio 0.5, pool 1000) | 50,486 | 50,351 | 0 | 2.9 / 545 | 6 / 711 | 36 / 50 | 6,177,132 pushed = delivered = acked; 0 delivered twice in each consumer, and the four consumers' distinct counts add up to the 6,177,132 |

Rates are OMB's averages over the six 10-s windows of the test minute. Latencies are OMB's
aggregates over that minute. The 4-consumer run predates the shared `verify` tracker: each
consumer was checked on its own.

The tails come from the Docker Desktop disk, not the driver. The broker's own
`queen_raft_log_fsync_seconds{quantile="1"}` reached 0.66 s during the 10k run and 0.77 s during
the 50k run, and 0.93 s by the end. The client sees those stalls per message: every message waiting behind a stalled push
counts. In 10-s windows without a stall, publish p99 was 6 ms and end-to-end p99 10 to 11 ms at
50k msg/s. For comparison, the Kafka driver against the Kafka facade on the same Mac earlier on
10-16 gave end-to-end p50 112 ms at 5k msg/s.

Burst checks used a small harness through the driver API (not part of the module): 1M messages
through one producer and one consumer.

- One producer pushed 1M x 1 KB in 1.1-1.3 s (800-890k msg/s) to the single node, in about 1,000
  pushes of 1 MiB.
- Every message came back, exactly once.
- The plain, zstd (flag 8), keyed and random-payload (base64) forms all round-trip byte for byte.
- A second driver instance learned the partition count from KV.

## Many topics, backlogs, the other families (10-17)

Functional runs on two GCP c4-highcpu-16 VMs (one Queen 2.4.0 node, the OMB workers on the other:
the first worker of `--workers` produces, the second consumes), scaled workloads in
`test/workloads/`, `verify: true`. Scripts: `test/vm.sh`, `test/run.sh`, `test/batch.sh`; results
in `test/runs/` (not committed).

### Topic count: one queue per topic, shared logs on the broker, one pop pool per worker

- **Mapping**: a topic stays a queue, so a topic keeps its own dedup window, subscriptions and
  cursors. The broker of these points runs with `QUEEN_QLOG_SHARDS=16` (set when the data
  directory is created): every queue's records go to one of 16 shared logs, so a write fsyncs at
  most 16 files however many queues it touches.
- **Not discovery pops** (`GET /api/v1/pop?namespace=..`): each one scans every queue of the tenant
  (`server/src/rsm/consume/pop.rs`, `pop_discover`: `scan_queues(&c.tenant, usize::MAX, ..)`, then
  a group per matching queue), it does not park (`c.wait && kind != Kind::Discover`) but is re-run
  on any push of the tenant, and it cannot be limited to the topics whose consumers are on this
  worker (a namespace is one value per queue, set at `configure`, before OMB assigns consumers to
  workers). So it would deliver topic B's messages to a worker without a consumer of B.
- **Connections and threads**: a consumer owns no thread; the worker's pops are one pool of at most
  `maxPops` HTTP connections; a single-partition topic needs one pop per subscription. The
  producers share lanes (16 on a 16-core worker), so a push carries many topics.

| Topics (1 partition, 4p 4c, 20k msg/s, 2 min) | Broker data on | Pops | Pub p50 / p99 ms | E2E p50 / p99 / p99.9 ms | Broker cores, RSS, threads, fds | Worker threads (producer / consumer) | TCP to broker | Duplicates, holes |
|---|---|---|---|---|---|---|---|---|
| 1,000 | boot disk | 1 parked per subscription | 95 / 244 | 240 / 542 / 797 | -, 2.0 GB, 227, 1,217 | 121 / 115 | 1,096 | 0, 0 |
| 1,000 | tmpfs | 1 parked per subscription | 1.4 / 2.3 | 2 / 4 / 132 | 7.9, 1.9 GB, 180, 1,213 | 112 / 115 | 1,096 | 0, 0 |
| 10,000 (`maxPops` 12000) | tmpfs | 1 parked per subscription | 1.7 / 3.1 | 3 / 5 / 14 | 9.0, 2.8 GB, 200, 10,212 | 114 / 123 | 10,096 | 0, 0 |
| 10,000 (`maxPops` 4096) | tmpfs | doorbell (10 fetches of 1,000) | 2.0 / 4.3 | 12 / 20 / 28 | 10.8, 3.0 GB, 324, 4,318 | 124 / 124 | 4,202 | 0, 0 |
| 100,000 (`maxPops` 12000, the build before the doorbell = `doorbell: off`) | tmpfs | rotation, 200 ms waits | 103 / 223 | 867 / 10,176 / 10,471 | 13.5, 5.6 GB, 331, 12,214 | 113 / 126 | 12,096 | 0, 0 |
| 100,000 (`maxPops` 12000) | tmpfs | doorbell (98 fetches of 1,024) | 336 / 768 | 697 / 7,660 / 16,053 | 14.3, 7.8 GB, 365, 12,312 | 125 / 129 | 12,194 | 0, 0 |

- The boot disk is the VM's 50 GB Hyperdisk: raft log fsync p50 17 ms, p99 134 ms there
  (`queen_raft_log_fsync_seconds`). The brokers of the GCP matrix have local SSDs; tmpfs takes the
  disk out of these functional runs.
- The old consumer (8 threads per consumer, each its own long poll) needed 4 x 8 = 32 threads per
  topic: 32,000 at 1,000 topics. Now 113-129 threads per worker at any topic count.
- Up to `maxPops` subscriptions per worker, each keeps one pop parked on the broker: the cheap
  regime. The limit is connections: about 25,000 per loader to one broker address (the ephemeral
  port range).
- Past `maxPops` (`doorbell: auto`), a single-partition subscription waits in a doorbell: a
  long-poll `POST /api/v1/fetch` over up to 1,024 partitions at the offsets the subscription has
  not seen, with a top-level `maxBytes` of 1, so the answer is every entry's `highWatermark` and at
  most one record. A subscription whose partition moved past what it saw gets a pop without a wait
  (94% of them found messages, against 27% for the rotation); the others stay in the next fetch.
  Without it (`doorbell: off`, or a topic with several partitions) the pops rotate over the
  subscriptions with `rotationWaitMs`.
- 100,000 topics is past what this one 16-vCPU broker carries at 20k msg/s either way: 13.5-14.3
  cores, so both latencies are the broker's queue, and both runs are still exact. A 15-s profile
  (`test/runs/final-100k-bell/broker-perf-top40.txt`) has the fetch path's partition watches
  (`Notifier::gate`, `PartitionWatch` drops, `fetch_wait`), the JSON rendering of the fetch
  answers (serde_json `Value`), store reads and the allocator on top. A pop that waits on many
  queues and answers the ones with data, or HTTP/2 for 100,000 parked pops on a few connections,
  is a broker change.
- Discovery pops cannot do it either (above).

### Backlog: OMB holds the consumers by blocking the callback

OMB's `pauseConsumers` makes the callback sleep while the backlog builds (`LocalWorker`:
`while (consumersArePaused) Thread.sleep(1000)`), for as long as the backlog takes: about 17
minutes for `backlog-1-topic-16-partitions-1kb` (100 GB at 100 MB/s). The batches the consumers
hold, and the pop answers waiting for a delivery thread, outlive their lease (300 s on a
configured queue), and come back as duplicates after the resume. The driver now extends every
batch's lease from the moment its answer arrives (read from the answer's head on the Netty thread,
before it waits for a delivery thread) until its ack is answered.

| Run (3 GB backlog at 50k msg/s, pause 63 s, `leaseSeconds: 20`) | Lease extensions | Acks refused | Delivered again | Holes |
|---|---|---|---|---|
| 1 topic, 16 partitions, 1p 1c, `renewLeases: false` (the old behaviour) | 0 | 462 ("invalid or expired lease") | 462 | 0 |
| same, `renewLeases: true` | 72 | 0 | 0 | 0 |
| 1 topic, 100 partitions, 4p 4c (32 pops), extensions taken at decode only | 144 | 222 | 222 | 0 |
| same, extensions from the answer's arrival (the final driver) | 288 | 0 | 0 | 0 |

Each run pushed, delivered and acked the same 6.59 million messages, and drained the 3 GB at
about 350k msg/s.

### The other families

All with `verify: true`; "exact" = pushed = delivered = acked on the driver's own counters, 0
delivered twice and 0 holes. Max-rate runs had the broker on the boot disk (above), the others tmpfs.

| Workload (scaled) | Pub msg/s | Cons msg/s | Pub err | Pub p50 / p99 ms | E2E p50 / p99 ms | Result |
|---|---|---|---|---|---|---|
| 1 topic, 100 partitions, 4p 4c, 100k msg/s, 1 min, boot disk | 99,944 | 99,926 | 0 | 6.2 / 9.8 | 15 / 32 | exact, 6.1 M |
| 1 topic, 10,000 partitions, 4p 4c, 100k msg/s, boot disk | 99,899 | 99,869 | 0 | 6.4 / 38 | 15 / 52 | exact, 6.1 M, all 10,000 partitions |
| keyed (`KEY_ROUND_ROBIN`), 1 topic, 100 partitions, 4p 4c, 50k | 49,976 | 49,975 | 0 | 1.3 / 2.0 | 2 / 4 | exact, 12.9 groups per push |
| keyed (`RANDOM_NANO`), same | 49,980 | 49,979 | 0 | 1.3 / 2.0 | 2 / 4 | exact |
| 100 topics, 1 partition, 3 subscriptions, 30k | 29,995 | 89,855 | 0 | 1.8 / 3.1 | 6 / 694 | exact on all 300 (topic, subscription) pairs; see the tail below |
| max rate, 1 topic, 1 partition, 1p 1c, 100 B (`producerRate` 10M) | 619,686 | 21,073 | 0 | 638 / 1,179 | backlog | exact; one partition is one lease at a time: about 21 pops/s of 1,000 |
| max rate, same, 1 KB | 456,008 | 68,219 | 0 | 101 / 154 | backlog | exact |
| max rate, same, 64 KB | 5,129 | 3,303 | 0 | 153 / 191 | backlog | exact |
| max rate, 1 topic, 100 partitions, 100p 100c, 1 KB | 609,471 | 608,903 | 0 | 1,264 / 1,319 | 1,326 / 1,399 | exact, 37.6 M |
| max rate, 1 topic, 10 partitions, 10p 10c, 64 KB | 6,966 | 6,958 | 0 | 1,108 / 1,148 | 1,190 / 1,246 | exact |
| `producerRate: 0` (OMB's rate controller), 1 topic, 100 partitions, 1p 1c, 1 KB, 2 min | 276,199 | 276,180 | 0 | 6.0 / 10.6 | 14 / 20 | exact, 33.5 M |

At max rate the lanes' 64 MiB buffers stay full: `sendAsync` blocks (never past `maxBlockMs`:
0 messages failed on a full buffer), and the publish latency is the time in that buffer.

## For 1-2 M msg/s on 3 loaders

- **`pollers`**: raise it to 4-8 for the high points (pops per consumer; a subscription gets at most one per partition). With autopilot the pops seen here held at most
  1000 messages from 1 partition. On the burst, 4 pollers drained faster than 2 (mean end-to-end
  408 ms vs 670 ms). At 2M msg/s with 4 consumers, 2 pollers each would have to cycle pop, decode
  and ack in about 4 ms. With 10,000 partitions each pop holds even fewer messages, so give it more
  pollers.
- **Randomized payloads** (`useRandomizedPayloads: true`, as in `../workloads/`) go as base64:
  1,368 bytes per 1 KB message on the wire, against Kafka's 1,024. The broker's level-1 zstd on
  disk recovers much of that, but the HTTP bytes stay. The hex `payloadFile` workloads send 1,024
  bytes plus a 26-byte JSON envelope. `compression: zstd` cuts the push bytes and moves the
  compression CPU to the loaders.
- **Ack bodies** are about 110 bytes per message of JSON (one item per message, see above): about
  220 MB/s across the brokers at 2M msg/s. That is the protocol, not this driver.
- **Per-lane ceiling**: `sendAsync` takes its lane's lock and copies about 1.4 KB, and the batch goes out
  with no further copy. One producer reached 800-890k msg/s on the Mac, above the 500k each of 4
  producers carries at 2M.
- **Heap**: each lane holds at most `bufferMaxBytes` (64 MiB). Batch buffers and pop answers
  are reused, so per-message allocation is small.
