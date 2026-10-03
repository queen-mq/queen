# mqload: kload (Kafka) and pload (Pulsar)

Open-loop load generators for the Kafka vs Pulsar vs Queen benchmark. `../SPEC.md` is the contract (workload §2,
output §3, flags §4, client tuning §5). Everything that defines the workload lives in `internal/core` and is shared
by both loaders, so their semantics are identical by construction:

- goload's open-loop pacer, ported unchanged in its schedule math (W workers, per-worker random phase, wall-clock
  catch-up, maxCatchUp 4096, ramp F⁻¹ schedule). It paces **units**; a non-blocking in-flight semaphore
  (`-max-inflight` units) sheds.
- goload's `olHist` histograms, copied verbatim, so percentiles land in the same buckets as Queen's goload.
  `-out` JSON holds the sparse buckets, so loaders can be merged exactly.
- goload's `jsonEvent` payload with word seed 7, copied verbatim. A pool of 64 × max-unit events is pre-marshaled and
  spliced as `{"ts":<scheduled µs>,"src":<loader-index>,…`. Warm-up messages carry no `ts` and are never counted
  as load. Consumers parse `ts`/`src` by scanning bytes.
- Pickers: rr (topic then entity, start offset `loader_index*space/loaders`), rotate/scatter (`-active`; goload's
  `partitionIndex`/`scatterMultiplier` verbatim), zipf (rejection-inversion, any s ≥ 0, ranks permuted), and topic
  weights rr|zipf.
- Start barrier, consumer topic rule, `-proc-us`, drain, reporter lines and the JSON result.

## Build

```
./build.sh                                     # tests, then bin/{linux-amd64,linux-arm64,darwin-arm64}/{kload,pload}
SKIP_TESTS=1 TARGETS=linux/amd64 CMDS=pload ./build.sh   # one binary only (never rebuild a kload that runs on the VMs)
```

The binaries are static (`CGO_ENABLED=0`, `-trimpath`). Each is written to a temp name and then `mv`'d, so a running
binary is never overwritten in place. `go.mod` says `go 1.26.0` (toolchain go1.26.8) because franz-go v1.22.x
requires it. With the installed Go 1.25.7, `GOTOOLCHAIN=auto` uses go1.26.8 from the module cache. The droplets need
no Go. build.sh sets `GOWORK=off`: `/Users/alice/Work/queen/go.work` would otherwise capture the module.

Dependencies: franz-go v1.22.1 (+ kadm v1.19.0, kmsg v1.14.0) and apache/pulsar-client-go v0.21.0.

## One process, start to end

1. **Setup.** Topics are created (idempotent) or waited for, optionally warmed, and consumers are started and
   settled: Kafka waits until the groups are stable group-wide; Pulsar waits until every consumer is subscribed,
   plus `-settle`. Producers are created (Kafka: connections to every broker and a producer ID).
2. `READY <unix ms>` is printed. The process then waits for `-start-file` (polled every 100 ms; its content is the
   start instant in unix ms, and an empty or partly written file keeps it polling) or `-start-at`, or starts at once
   when neither is given. A start instant already in the past prints `WARN late start by <ms> ms` and starts now.
   Consumers run from READY on.
3. **Producing** runs for `-duration` (ramp included), at `-rate` msg/s for THIS process. Every `-report` from the
   start instant, one window line is printed (SPEC §3). The window that reaches the end of producing waits for the
   pacer's last wake, so no offered unit spills into the drain window.
4. Producers stop. Units still in flight complete, and consumers keep going for `-drain`.
5. Consumers and producers are closed, then the process prints `[info] …` lines, `[final] …` and `load_cpu=…%`, and
   writes `-out`. SIGINT/SIGTERM stop producing early and still print the final lines; a second signal kills.

Exact lines, parsed by `report/report.py` and the 09-29 `grid_report.py` regexes (verified in TESTS.md (f)):

```
[HH:MM:SS] offered=%9.0f/s achieved=%9.0f/s shed=%9.0f/s inflight=%6d | p50=%7.2f p99=%8.2f p999=%8.2f ms | push=%d pop=%d lag=%d | errs push=%d pop=%d empty=%d gor=%d | ack=%9.0f/s ackErr=%d ackAvg=%.2fms | e2e p50=%.2f p99=%.2f p999=%.2f n=%d | e2e_local p50=%.2f p99=%.2f n=%d
[final] offered=%d achieved=%d shed=%d (msgs: offered=%d achieved=%d shed=%d) pushErr=%d | pushed=%d popped=%d lag=%d | popErr=%d empty=%d | overall p50=%.2f p99=%.2f p999=%.2f ms | acked=%d ackErr=%d ackLag=%d ackAvg=%.2fms | e2e p50=%.2f p99=%.2f p999=%.2f ms
load_cpu=%.1f%%
```

Field definitions:

- Window rates are msg/s over the actual window length. `achieved` is messages acked by the broker.
- `p50/p99/p999` is produce latency, measured from the scheduled instant to the broker ack of the unit's last message,
  for units whose messages all succeeded.
- `push`, `pop` and `lag` are cumulative load messages, where lag = push − pop. Warm-up messages are not counted.
  With several processes, only the sum over processes balances.
- `errs push` counts failed units. `pop` counts consume errors. `empty` counts empty polls (Kafka).
- `ack=` is acked load msg/s: Kafka offsets committed; Pulsar `Ack()` handed to the client's ack grouping.
  `ackAvg` is the cumulative mean ack-call latency.
- e2e latency is consumer receive (before ack) minus scheduled send. `e2e_local` counts only messages whose `src` is
  in `-local-src`.
- `load_cpu` is getrusage CPU over the process lifetime.
- `[info] steady window …` reports this process's CPU over the windows between the end of the ramp and the end of
  producing, as cores per 100k produced msg/s (produce and consume both run in the process). It also goes into the
  JSON `steady` object.

## Flags

### Common (internal/core; same in kload and pload)

Durations accept a Go duration or a bare number of seconds (`-duration 70` = `-duration 70s`).

| flag | default | meaning |
|---|---|---|
| `-rate` | 0 | offered msg/s for THIS process, open loop (0 = no producers) |
| `-ramp` | 10s | linear ramp from 0 (goload's F⁻¹ schedule) |
| `-duration` | 70s | whole producing time, ramp included |
| `-report` | 10s | window line every this long, from the start instant |
| `-drain` | 0 | consumers keep consuming this long after the producers stop |
| `-start-file` | | start barrier file: polled every 100 ms, content = start instant (unix ms) |
| `-start-at` | 0 | start instant (unix ms) |
| `-max-inflight` | 5000 | in-flight units; a unit scheduled while the cap is full is offered+shed, never sent |
| `-mode` | batch | `batch`: a unit = `-batch` msgs of one entity back to back. `keyed`: a unit = `-batch`..`-batch-max` msgs of one entity, and the client batches across entities |
| `-batch` | 100 (batch) / 1 (keyed) | messages per unit |
| `-batch-max` | 0 | if > `-batch`: unit size uniform in [batch, batch-max] |
| `-topics` | 1 | T topics: `-topic` if T=1, else `<topic>-t<i>` |
| `-topic` | bench | topic name or prefix |
| `-partitions` | 200 | partitions per topic (Pulsar: 0 = non-partitioned) |
| `-entities` | 0 | entities per topic. 0 = entity e IS partition e (explicit partition). >0 = key `e<id>` and the system's key partitioner |
| `-dist` | rr | rr \| rotate \| scatter \| zipf |
| `-active` | 0 | rotate/scatter: distinct entities per second (goload `-active-partitions`) |
| `-active-policy` | rotate | with `-dist rr -active N`: rotate \| scatter |
| `-zipf-s` | 1.1 | zipf exponent over entities |
| `-topic-dist` | rr | rr \| zipf over topics |
| `-topic-zipf-s` | 1.0 | zipf exponent over topics |
| `-payload` | 256 | jsonEvent target bytes. goload's fixed fields are ~280 B, so events are ~290 B and ~320 B stamped, same bytes as Queen's runs |
| `-consumers` | 4 | consumers in this process |
| `-cons-offset` | 0 | global index of this process's first consumer |
| `-cons-total` | 0 (= consumers) | consumers across all processes |
| `-poll` | 1000 | Kafka PollRecords max; Pulsar: messages drained from the receive channel per loop |
| `-proc-us` | 0 | simulated processing µs per message, accumulated and slept in ≥ 1 ms chunks |
| `-ack` | async | Kafka commit per poll: async \| sync \| none. Pulsar: Ack per message (none = no acks) |
| `-ack-inflight` | 256 | in-flight async commits per process; full = block, never shed |
| `-loader-index` / `-loaders` | 0 / 1 | this process's index (the `src` and the rr offset) / number of processes |
| `-local-src` | own index | loader indices on THIS host, e.g. `0-2` or `0,3,6`, for `e2e_local` |
| `-create` | true | create topics (idempotent). Pass `-create=false` to the N load processes |
| `-create-only` | false | create (or wait for) the topics, optionally `-warm`, then exit |
| `-warm` | false | one message without `ts` into every partition (Kafka: acks=all, then readiness re-checked; Pulsar: also consumed and acked) |
| `-out` | | JSON: final numbers, histograms, windows, config, system info |
| `-tag` | | run tag |
| `-seed` | 0 (time based) | RNG seed; the process seed is seed+loader_index |
| `-pprof` | | serve net/http/pprof on this address |

Environment variables:

- `GOGC` defaults to 400 when unset, as goload ran on the loaders. `GOMEMLIMIT` is honored by the Go runtime; use it
  on memory-capped hosts.
- `MQLOAD_SLOW_MS=<ms>` prints the first 50 units at or over that produce latency to stderr.

### kload (franz-go v1.22.1)

| flag | default | meaning |
|---|---|---|
| `-brokers` | 127.0.0.1:9092 | seed brokers (all 3 private IPs) |
| `-rf` / `-min-isr` | 3 / 2 | replication factor / min.insync.replicas of created topics |
| `-topic-config` | retention.ms=600000,segment.bytes=268435456 | configs of created topics |
| `-producers` | 4 | franz-go producer clients per process; unit u → client u % producers |
| `-linger` | 5ms | ProducerLinger |
| `-compression` | lz4 | none\|gzip\|snappy\|lz4\|zstd |
| `-inflight-per-broker` | 5 | idempotent producers always run 5 per broker in franz-go (Kafka's max), and the option cannot be set with idempotency. A value other than 5 only prints a WARN |
| `-batch-max-bytes` | 1048576 | ProducerBatchMaxBytes |
| `-fetch-max-wait` / `-fetch-min-bytes` / `-fetch-max-partition-bytes` | 500ms / 1 / 1048576 | fetch settings |
| `-group-protocol` | consumer | `consumer` = KIP-848 (ServerSideBalancer, broker "uniform" assignor); `classic` = cooperative-sticky |
| `-session-timeout` | 45s | classic session timeout. KIP-848 uses the broker's `group.consumer.session.timeout.ms` |
| `-stable-wait` / `-group-timeout` | 3s / 180s | group stability judgement (see below) |
| `-create-chunk` | 5000 | partitions per CreateTopics/CreatePartitions request |
| `-topic-timeout` / `-topic-settle` | 300s / 0 | readiness gate timeout and hold time |
| `-metadata-max-age` | 5m | franz-go MetadataMaxAge |
| `-delivery-timeout` | 120s | RecordDeliveryTimeout (Java delivery.timeout.ms) |
| `-client-log` | none | franz-go log level to stderr |

**Producer.** acks=all, idempotent, linger 5 ms, lz4, 1 MiB batches, `TryProduce`. `MaxBufferedRecords` is set to
max-inflight × max unit + 1024 per client, so the client can never block the pacer; only the pacer's semaphore sheds.
Before READY, each client opens produce connections to every broker and loads its producer ID. For E=0, records use
the `ManualPartitioner`. For E>0, they use `StickyKeyPartitioner(nil)`: murmur2 & 0x7fffffff % n, Java's exact keyed
placement, tested against Kafka's golden vectors.

**Consumer.** One franz-go client per consumer. Groups are `<topic>-g-t<i>` when cons-total ≥ T, else
`<topic>-g-c<ci>`. A new group starts at the log start, and warm records have no `ts`, so they are not counted.
After `PollRecords(-poll)`, the loop takes e2e per record, runs `-proc-us`, then commits the polled offsets
(last+1, leader epoch) asynchronously under the process-wide `-ack-inflight` semaphore. The commit is taken from
`CommitOffsets`, which serializes commits per client and never cancels a prior one. Revoked partitions are
committed synchronously in OnPartitionsRevoked.

**Group stability.** A group is stable when, as seen group-wide through DescribeGroups (classic) or
ConsumerGroupDescribe (KIP-848: state Stable, epochs settled, every member's assignment equal to its target):

- the expected member count across all processes has joined,
- every partition is assigned,
- no local assignment change has happened for `-stable-wait`.

A local-only view is not enough: under KIP-848 a member can hold every partition while the target has already moved
some of them.

**Readiness gate.** Every partition has a leader and a full ISR. When this process creates topics or warms them,
every replica log must also be on disk (DescribeLogDirs).

### pload (apache/pulsar-client-go v0.21.0)

| flag | default | meaning |
|---|---|---|
| `-url` | pulsar://127.0.0.1:6650 | service URL with every broker: `pulsar://ip1:6650,ip2:6650,ip3:6650` |
| `-admin` | http://127.0.0.1:8080 | admin REST base URL |
| `-tenant` / `-namespace` / `-bundles` | bench / ns / 48 | created if missing. allowedClusters = the first cluster of GET /admin/v2/clusters |
| `-persistence` | (empty = broker default) | namespace E,Qw,Qa, e.g. `3,3,2` |
| `-sub` / `-sub-type` | sub / failover | subscription and type: failover \| shared \| key_shared \| exclusive |
| `-consumer-slices` | true | failover/exclusive on partitioned topics: each consumer subscribes only to its partitions (see below) |
| `-producer-shard` | true | E=0: process li produces only to partitions p % loaders == li, through lazy per-partition producers (see below) |
| `-producer-eager` | false | with `-producer-shard`: create the shard's producers before READY |
| `-compression` | lz4 | none \| lz4 \| zlib \| zstd |
| `-batch-delay` / `-batch-max-msgs` / `-batch-max-bytes` | 5ms / 1000 / 131072 | producer batching |
| `-max-pending` | 0 (auto) | MaxPendingMessages per partition producer. Auto = max-inflight × max unit × 2, capped by `-pending-budget-mb` |
| `-pending-budget-mb` | 64 | cap on the pending queues the Go client preallocates (24 B/slot per partition producer) |
| `-key-batching` | on for key_shared | KeyBasedBatchBuilder, required for Key_Shared ordering with batching |
| `-receiver-queue` | 1000 | ReceiverQueueSize |
| `-ack-group-time` | 100ms | ack grouping MaxTime (MaxSize 1000). Batch-index acks are on |
| `-conns-per-broker` | 1 | MaxConnectionsPerBroker |
| `-io-threads` | 0 | accepted for parity with Java; the Go client has no IO thread pool (no effect) |
| `-op-timeout` / `-conn-timeout` / `-send-timeout` | 30s / 10s / 30s | client and producer timeouts |
| `-topic-timeout` / `-admin-conc` | 300s / 32 | admin setup timeout / REST requests in flight |
| `-settle` | 2s | wait after subscribing, before READY. Failover moves the active consumer 1 s after a join, and Key_Shared re-splits hash ranges |
| `-client-log` | warn | pulsar client log level |

**Setup (REST).** pload creates the tenant, the namespace (`-bundles`), and the topics: `PUT …/partitions`, or a
non-partitioned topic when `-partitions 0`. **The subscription is created before any producer exists** (`PUT
…/subscription/<sub>`, earliest) and then verified on every partition with `GET …-partition-N/subscriptions`; a
partition that missed it gets it directly. Processes with `-create=false` wait until every topic has its partitions
and the subscription.

`-warm` sends one message without `ts` to every partition (explicit router), then consumes and acks those messages
on the subscription.

**Producers.** SendAsync never blocks: `DisableBlockIfQueueFull`, MaxPendingMessages sized above the in-flight share
of a partition, and a memory limit that is accounting only and set above the in-flight cap. Latency is taken at the
callback of the unit's last message.

- **E=0 with `-producer-shard`** (default): the picker walks this process's shard, and each partition has its own
  producer on `<topic>-partition-<p>`. A producer is created on first use in the background: units wait in the slot,
  never in the pacer, and the unit's latency includes the creation. Creation runs at most `-admin-conc` at a time;
  a failed creation is retried after 2 s.
  - Across all processes this is one producer per partition, with the same per-partition rate and the same units.
  - rotate/scatter use `ceil(active/loaders)` per process, so `-active N` stays N partitions per second in total.
  - The final `[info] producers created:` line and the JSON `producers_created` report how many were made.
- **E=0 with `-producer-shard=false`**: one partitioned producer per topic, and a MessageRouter returns the unit's
  partition. The index rides in a struct around the ProducerMessage, so routing needs no lookup.
- **E>0**: one partitioned producer per topic with key `e<id>` and the default key-hash routing. KeyBasedBatchBuilder
  is used for key_shared.

**Consumers.** One consumer per index, with `Name c<ci>` (failover orders consumers by name), receiver queue 1000,
ack grouping 100 ms, batch-index acks, initial position earliest, and Ack per message. The receive loop drains up to
`-poll` messages from the channel, samples e2e, runs `-proc-us`, then acks.

- **failover and exclusive** on partitioned topics with `-consumer-slices` (default): consumer ci subscribes only to
  its partitions of its topics, as one multi-topic consumer on explicit `<topic>-partition-<p>` topics.
  - `core.ConsumerPartitions`: the readers of topic t are ranked (rank = ci/T when cons-total ≥ T), and partition p
    belongs to rank p % readers. With T=1 this is `p % cons-total == ci`; with cons-total < T each consumer owns its
    whole topics.
  - Every partition has exactly one consumer in total, the shape of a Kafka group. Consumers beyond the partition
    count get nothing and are reported as idle.
- **shared and key_shared** subscribe whole topics, since they must share partitions.
- The `[consume]` line prints the consumers, registrations here and in total, and idle consumers.

## Examples

Kafka, create + warm once (loader 1), then one of the 9 load processes (loader-index 4 of 9 on the loader
running 3..5, consumers 22 per process, start barrier):

```
kload -brokers 10.114.0.2:9092,10.114.0.3:9092,10.114.0.4:9092 -topic c-a1m -partitions 200 -create-only -warm
kload -brokers 10.114.0.2:9092,10.114.0.3:9092,10.114.0.4:9092 -topic c-a1m -partitions 200 -create=false \
      -rate 111111 -duration 70 -ramp 10 -report 10 -drain 10 -loader-index 4 -loaders 9 -local-src 3-5 \
      -consumers 22 -cons-offset 88 -cons-total 198 -start-file /root/bench/start -out /root/bench/runs/a1m-4.json
```

Pulsar, matrix D shape (Key_Shared, 48 partitions, 1M keys, batch mode), one load process after
`pload … -create-only -warm`:

```
pload -url pulsar://10.114.0.2:6650,10.114.0.3:6650,10.114.0.4:6650 -admin http://10.114.0.2:8080 \
      -topic d-keys -partitions 48 -entities 1000000 -sub-type key_shared -create=false \
      -rate 111111 -duration 70 -drain 10 -loader-index 0 -loaders 9 -local-src 0-2 \
      -consumers 33 -cons-offset 0 -cons-total 297 -start-file /root/bench/start -out /root/bench/runs/d1m-0.json
```

Kafka, "real shape" (matrix E: 10 skewed topics, zipf keys, keyed units of 1..20, 200 µs processing):

```
kload -brokers … -topic e-real -topics 10 -topic-dist zipf -partitions 100 -entities 1000000 -dist zipf \
      -mode keyed -batch 1 -batch-max 20 -proc-us 200 -rate 50000 -create=false -loader-index 0 -loaders 9 \
      -consumers 33 -cons-offset 0 -cons-total 297 -start-file /root/bench/start
```

## Notes for run scripts

- Create once (`-create-only -warm`, one process), then start the N processes with `-create=false`. A creating
  process runs the heavier readiness gate: replica logs on disk, and on Pulsar a subscription check per partition.
- Every process prints `READY <unix ms>` after its setup. Write the start instant atomically (write a temp file,
  then `mv`). `-local-src` should list the loader indices on the same host.
- Exit codes: 0 after a run, including one cut short by a signal; 1 with `FATAL …` on setup errors (topic not
  ready, group not stable within `-group-timeout`, admin errors).
- **Kafka KIP-848 settles at the broker's heartbeat cadence.** A group is stable only after its members reconcile,
  which is paced by `group.consumer.heartbeat.interval.ms` (default 5 s). A 2-member group took ~13 s locally,
  against ~4 s with classic.
- **Kafka keyed mode commits often.** Keyed units trickle continuously, so each consumer polls small batches (~16
  records at ~3.3k msg/s per consumer) and commits after every poll, roughly one OffsetCommit per 16 messages.
- **Pulsar has a per-partition cost in the Go client.** Every partition producer runs its own 5 ms batch-flush ticker
  and allocates a 512 KB LZ4 table. Idle, that is ~0.55 cores per 1000 partition producers per process, and each
  attached partition consumer costs ~0.06 cores per 1000. Measurements are in TESTS.md.
  - `-producer-shard` and `-consumer-slices` keep each partition at one producer and one consumer registration in
    total. A process still owns P/loaders partition producers: ~11k at P=100k with 9 loaders, several cores idle.
  - Key-based batching allocates one LZ4 table per key batch, so keyed/Key_Shared runs have a high allocation rate
    and RSS of ~0.5 GB at 20k msg/s with GOGC=400. Use `GOMEMLIMIT` on small hosts.
