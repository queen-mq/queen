# Kafka + Pulsar harness on Queen's 3-node rig — SPEC (the contract)

Goal: measure Kafka 4.3.1 and Pulsar 4.2.4, each tuned the way an expert operator would run it, on the SAME
droplets and with the SAME workload as Queen's 3-node grid (09-29), so the three can sit in one table.
This file is the contract between the parts. The README explains how to run it.

## 0. Fairness rules (do not break them)

1. Same boxes: 3 brokers (16 vCPU / 31 GB / local ext4 disk, private VPC), 3 loaders (16 vCPU / 31 GB), one
   system running at a time. Every start script refuses to start while another system listens on the node
   (Queen 6632/7400, Kafka 9092/9093, Pulsar 2181/3181/6650/8080).
2. Same workload: 9 load processes (3 per loader), each offering 1/9 of the rate, open loop, shedding at an
   in-flight cap (never blocking), latency measured from the SCHEDULED instant (coordinated-omission correct),
   the same ~256 B JSON event payload generator as goload, e2e = consumer receive (before ack) − scheduled send.
3. Same durability class, stated per system in every table:
   - Queen: 3 copies, ack after 2 fsyncs (raft quorum).
   - Pulsar: E=3 Qw=3 Qa=2, bookie journal fsync on (journalSyncData=true) → 3 copies, ack after 2 fsyncs. Same as Queen.
   - Kafka: RF=3, min.insync.replicas=2, acks=all, idempotent producer; NO fsync (Kafka's production model:
     durability by replication, page cache flushed by the OS). Profile `fsync` (log.flush.interval.messages=1)
     exists for a parity point, not for the main grid.
4. Same OS tuning on every broker for every system, INCLUDING Queen's re-run (common/os-tune.sh).
5. Each system on its maintainers' recommended runtime: Kafka on G1 (Kafka's KAFKA_JVM_PERFORMANCE_OPTS),
   Pulsar broker + bookie on generational ZGC (Pulsar 4.2's own default on JDK 21). JDK 21 for both.
6. Tuning = performance knobs only. Never trade away the durability class above or ordering guarantees.

## 1. Hosts

`hosts.env` (sourced by every script; never hardcode IPs or NIC names — the VMs' private NIC is eth1, the local
rehearsal containers' is eth0):

```
B_PUB=(209.x 167.x 201.x)            # brokers, public IP (ssh)
B_PRIV=(10.114.0.2 10.114.0.3 10.114.0.4)   # brokers, private IP (all traffic)
L_PUB=(... ... ...)                  # loaders, public
L_PRIV=(10.114.0.5 10.114.0.6 10.114.0.7)
SSH="ssh -o StrictHostKeyChecking=no -o BatchMode=yes"   # rehearsal overrides with a docker-exec shim
REMOTE_ROOT=/root/bench              # harness + data root on every host
PROFILE=vm                           # vm | local (local = tiny heaps/caches for the laptop rehearsal)
```
A node finds its own index by matching its addresses (`hostname -I`) against B_PRIV.

Layout on every host: `/root/bench/{kafka,pulsar,common,bin,hosts.env}`; data under `/root/bench/data/<system>`,
logs under `/root/bench/logs/<system>`. Loader binaries: `/root/bench/bin/{kload,pload}` (linux/amd64 on VMs).

## 2. Workload model (both loaders, identical semantics)

- T topics (`-topics`, names `<prefix>` if T=1 else `<prefix>-t<i>`), P broker partitions per topic
  (`-partitions`; Pulsar 0 = non-partitioned topic), E entities per topic (`-entities`; 0 = one entity per
  partition, entity e IS partition e). An entity is Queen's partition: the unit of ordering.
- Entity → partition: E=0 → explicit partition index. E>0 → message key `e<id>` and the system's own key
  partitioner (Kafka murmur2, Pulsar key hash) — ordering per key, as a Kafka/Pulsar user would do it.
- Producer modes (`-mode`):
  - `batch` (default, mirrors goload's push): each scheduled unit = `-batch` B messages (default 100) for ONE
    entity of ONE topic, produced back to back. In-flight cap and latency are per unit (all B acked).
  - `keyed` (the "real shape"): each scheduled unit = `-batch` messages (default 1; `-batch-max` > batch draws
    uniform [batch, batch-max]) for one entity; the client batches across entities by partition (linger).
- Picker (`-dist`): `rr` (default; topic then entity, like goload; counter starts at
  `loader_index * E_or_P / loaders` so the 9 processes never walk p0,p1,... in lockstep), `rotate`/`scatter` with
  `-active N` entities per second (goload's -active-partitions semantics, same partitionIndex math), `zipf`
  (`-zipf-s`, default 1.1, over entities; permuted so hot keys are spread over partitions).
  Topic weights: `-topic-dist rr|zipf` (`-topic-zipf-s`, default 1.0) — the "10 skewed queues" shape.
- Payload: goload's `jsonEvent` generator (copy it verbatim: same fields, same word list seed 7, ~`-payload`
  bytes, default 256), pool of 64×B pre-marshaled events rotated per unit. Every message is spliced as
  `{"ts":<scheduled unix µs>,"src":<loader-index>,<rest of the event>`. Warm-up messages carry no "ts".
- Consumers: `-consumers C` per process, global consumer index ci = `-cons-offset` + i, total `-cons-total`.
  Topic assignment rule (both systems): if cons-total ≥ T → consumer ci reads topic ci % T (several consumers per
  topic share it); else consumer ci reads topics {t : t % cons-total == ci}.
  - Kafka: one consumer group per topic when cons-total ≥ T (group `<prefix>-g-t<i>`), else one group per consumer
    (`<prefix>-g-c<ci>`) owning its topic slice. Each consumer = its own franz-go client (a group member).
  - Pulsar: subscription `sub` on each topic, type `-sub-type` failover|shared|key_shared|exclusive; a consumer
    with several topics is a multi-topic consumer.
- Consume loop (closed loop): receive up to `-poll` (Kafka PollRecords, default 1000) / from the receive channel
  (Pulsar), e2e sample per message with a "ts", optional `-proc-us` per message (accumulated, slept in ≥1 ms
  chunks), then ack: Kafka = commit the polled offsets (`-ack async|sync|none`, async in-flight cap
  `-ack-inflight` 256, never shed: block); Pulsar = `Ack(msg)` per message (client ack grouping), batch-index acks on.
- Shedding: a semaphore of `-max-inflight` units, tried NON-blockingly at each scheduled instant; full → the
  unit is counted offered+shed, never sent. Producer-side client buffers must be sized ABOVE the cap
  (so the client never blocks the pacer).
- Pacer: copy goload's open-loop pacer (W workers, wall-clock catch-up, maxCatchUp 4096, ramp F⁻¹ schedule,
  per-worker random phase). `-rate` msg/s for THIS process, `-ramp` (default 10s), `-duration` (whole
  producing time incl. ramp).
- Start: after setup (clients connected, consumers subscribed/group stable, producers created) print
  `READY <unix ms>`; then wait for `-start-file` (poll 100 ms; content = unix ms instant) or `-start-at` (unix ms).
  Consumers run from READY; producers from the start instant. Late start (instant already past) → start now and
  print `WARN late start by <ms> ms`.
- After `-duration`: producers stop; consumers keep going for `-drain` (default 0); then close and print final.

## 3. Loader output (exact, parsed by report/report.py and by the 09-29 grid_report.py regexes)

Every `-report` (default 10s) one line (msg/s rates over the window; latencies in ms over the window):
```
[HH:MM:SS] offered=%9.0f/s achieved=%9.0f/s shed=%9.0f/s inflight=%6d | p50=%7.2f p99=%8.2f p999=%8.2f ms | push=%d pop=%d lag=%d | errs push=%d pop=%d empty=%d gor=%d | ack=%9.0f/s ackErr=%d ackAvg=%.2fms | e2e p50=%.2f p99=%.2f p999=%.2f n=%d | e2e_local p50=%.2f p99=%.2f n=%d
```
(time UTC; p50/p99/p999 = produce latency, scheduled → last broker ack of the unit; push/pop cumulative
messages; e2e_local = only messages whose "src" is this process's loader host, i.e. clock-skew free.)
At the end:
```
[final] offered=%d achieved=%d shed=%d (msgs: offered=%d achieved=%d shed=%d) pushErr=%d | pushed=%d popped=%d lag=%d | popErr=%d empty=%d | overall p50=%.2f p99=%.2f p999=%.2f ms | acked=%d ackErr=%d ackLag=%d ackAvg=%.2fms | e2e p50=%.2f p99=%.2f p999=%.2f ms
load_cpu=%.1f%%
```
(offered/achieved/shed before "(msgs:" count UNITS; load_cpu = this process's CPU over its lifetime, getrusage.)
`-out <file>` also writes the final numbers + config as JSON.
Histograms: log-linear µs buckets (goload's olHist: same bucket layout so percentiles match).

## 4. Loader flags

Common (internal/core): `-rate -ramp -duration -report -start-file -start-at -max-inflight -mode -batch
-batch-max -topics -topic -partitions -entities -dist -active -active-policy -zipf-s -topic-dist -topic-zipf-s
-payload -consumers -cons-offset -cons-total -poll -proc-us -ack -ack-inflight -drain -loader-index -loaders
-create -create-only -warm -out -tag -seed`.

kload (franz-go v1.22.x): `-brokers` (all 3 private IPs), `-rf 3`, `-min-isr 2`, `-topic-config k=v,...`
(default `retention.ms=600000,segment.bytes=268435456`), `-producers 4` (franz-go clients per process; unit u →
client u % producers), `-linger 5ms` (Kafka 4.x default linger.ms), `-compression lz4`, `-inflight-per-broker 5`,
`-batch-max-bytes 1048576`, `-fetch-max-wait 500ms`, `-fetch-min-bytes 1`, `-fetch-max-partition-bytes 1048576`,
`-group-protocol consumer|classic` (default: consumer = KIP-848 if franz-go supports it and the local test
passes, else classic + cooperative-sticky), `-session-timeout 45s`, `-stable-wait`, `-group-timeout`,
`-create-chunk 5000`, `-topic-timeout`. Keep the working parts of the 09-23 kload (chunked create, readiness
gate, warm, group-stable judgement, drain).

pload (apache/pulsar-client-go v0.21.x): `-url pulsar://ip1:6650,ip2:6650,ip3:6650`, `-admin http://ip1:8080`,
`-tenant bench -namespace ns -bundles 48`, `-sub sub -sub-type failover`, `-compression lz4`,
`-batch-delay 5ms` (BatchingMaxPublishDelay), `-batch-max-msgs 1000`, `-batch-max-bytes 131072`,
`-max-pending` (MaxPendingMessages; default = max-inflight × batch × 2, never blocks the pacer),
`-key-batching` (default on when sub-type is key_shared: KeyBasedBatchBuilder — REQUIRED for Key_Shared
ordering with batching), `-receiver-queue 1000`, `-ack-group-time 100ms`, `-conns-per-broker 1`
(client MaxConnectionsPerBroker), `-io-threads`. Topic creation through the admin REST API: namespace with
`-bundles` bundles + persistence (3,3,2) + no retention; partitioned topics `PUT .../partitions`; the
subscription is created BEFORE any producer sends (`PUT .../subscription/sub`), so nothing is lost to a
subscription that did not exist yet. Warm = one message per partition (router to each index), consumed.

## 5. Cluster configs (expert tuning; render per node from hosts.env + PROFILE)

### Kafka 4.3.1, KRaft, 3 combined broker+controller nodes (kafka/mkconf.sh)
| setting | value | why |
|---|---|---|
| process.roles / quorum | broker,controller; static voters on the 3 private IPs | 3-node cluster, no extra controller boxes |
| num.network.threads | 8 | default 3 is sized for 4-core boxes; ~1k connections per broker |
| num.io.threads | 16 | = vCPU; request handlers (default 8) |
| num.replica.fetchers | 8 (was 4 until 09-30) | default 1 cannot follow ~200 MB/s per broker; at 4, 50k partitions gave acks=all p50 ~500 ms with idle CPU (long follower fetch rounds) |
| message.max.bytes | 16 MiB (09-30) | KIP-848 group metadata for 100k partitions exceeds 1 MiB (RecordTooLargeException on join); producer batches stay ≤ 1 MiB |
| background.threads | 16 | partition.metadata flushes at 100k partitions |
| num.recovery.threads.per.data.dir | 8 | restart/wipe speed at 100k partitions |
| socket.*.buffer.bytes, replica.socket.receive.buffer.bytes | -1 | kernel autotuning (os-tune raises the maxima) |
| socket.listen.backlog.size | 4096 | ~1k clients connect at once |
| max.incremental.fetch.session.cache.slots | 4000 | ≥ consumers + followers (default 1000) |
| default.replication.factor / min.insync.replicas | 3 / 2 | durability class |
| unclean.leader.election.enable / auto.create.topics.enable | false / false | |
| log.retention.ms / log.retention.check.interval.ms | 600000 / 60000 | 400 GB disk: bound soaks |
| log.segment.bytes | 268435456 | segments roll (and retention can delete) within a soak |
| compression.type | producer | never recompress on the broker |
| flush | none (default) · profile fsync: log.flush.interval.messages=1 | |
| heap (KAFKA_HEAP_OPTS) | 6g (≤10k partitions), 10g (≥50k) | rest of 31 GB = page cache, Kafka's read path |
| JVM | Kafka's G1 opts + -XX:+AlwaysPreTouch | pre-fault the heap, no first-touch stalls in the window |
| nofile / max_map_count | 1048576 / 4194304 | 2 mmaps + 1 fd per active segment |

Producer: acks=all, idempotent, linger 5 ms, lz4, 5 in flight per broker, 1 MiB batches.
Consumer: fetch.min.bytes 1 / fetch.max.wait 500 ms (latency; data flows continuously at 1M/s), async commit
per poll.

### Pulsar 4.2.4, 3 nodes × (ZooKeeper + bookie + broker) (pulsar/mkconf.sh)
Memory per node (31 GB): ZK 1 GB heap; bookie 3 GB heap + 6 GB direct (write cache 1.5 GB, read-ahead 1 GB)
+ RocksDB block cache 512 MB; broker 6 GB heap + 6 GB direct (managed-ledger cache 2 GB); ≈ 24 GB, ~7 GB page cache.
| setting | value | why |
|---|---|---|
| managedLedgerDefaultEnsembleSize/WriteQuorum/AckQuorum | 3/3/2 (default 2/2/2) | durability class = Queen's |
| journalSyncData | true (default) | fsync before ack |
| journal/ledger dirs | 1 journal + 1 ledger dir, same local disk | one disk per box; JOURNALS=2 knob for 2 journal dirs |
| journalPreAllocSizeMB | 128 (16) | fewer preallocation stalls at ~300 MB/s |
| dbStorage_writeCacheMaxSizeMb / readAheadCacheMaxSizeMb / rocksDB_blockCacheSize | 1536 / 1024 / 512 MB | explicit instead of % of direct |
| gcWaitTime | 900000 (default, kept after the 09-30 review) | a 1-min scan of every ledger is churn at 100k partitions; the soak fits the disk |
| managedLedgerMinLedgerRolloverTimeMinutes / Max | 10 / 240 (defaults, kept after the 09-30 review) | 2/5 min = ~333 rollovers/s of ZooKeeper churn at 100k partitions; a 15-min soak writes ~135 GB (lz4) per bookie: fits |
| managedLedgerCacheSizeMB | 2048 | tailing reads from broker cache |
| exposeTopicLevelMetricsInPrometheus | false (true) | 1.4 GB /metrics at 150k topics (08-13 trap) |
| brokerDeleteInactiveTopicsEnabled | false (true) | topics vanish after 60 s idle (08-13 trap) |
| allowAutoTopicCreation | false | topics created explicitly (partitioned) |
| defaultNumberOfNamespaceBundles / ns bundles | 48 (4) | 16 bundles per broker: topics spread evenly |
| loadBalancerSheddingEnabled / AutoBundleSplit / AutoUnloadSplitBundles | false (true) | no topic moves inside a run; placement checked after create |
| forceDeleteNamespaceAllowed / forceDeleteTenantAllowed | true | fast cleanup between points |
| ZK metricsProvider.httpPort | 8001 (8000) | 8000 = bookie http on the same box |
| GC | generational ZGC + AlwaysPreTouch (Pulsar default on JDK 21) | |
Client: producer batching (5 ms, 1000 msgs, 128 KB), lz4, key-based batching for Key_Shared, receiver queue 1000,
ack grouping 100 ms, batch-index acks.

### OS (common/os-tune.sh, every broker and loader, idempotent, prints before/after, skips what a container
cannot set)
vm.swappiness=1, vm.max_map_count=4194304, vm.dirty_background_ratio=5, vm.dirty_ratio=60, fs.file-max=4194304,
net.core.somaxconn=4096, net.ipv4.tcp_max_syn_backlog=8192, net.core.netdev_max_backlog=32768,
net.core.rmem_max/wmem_max=67108864, net.ipv4.tcp_rmem/tcp_wmem="4096 131072 67108864",
net.ipv4.tcp_slow_start_after_idle=0, loaders also net.ipv4.ip_local_port_range="10240 65535" and tcp_tw_reuse=1,
remount / with noatime. THP left at Ubuntu's madvise. No IRQ pinning (it made Queen worse on 09-29).

## 6. Scripts (bash, `set -u`, idempotent, every action logged with a UTC timestamp)

- `common/deploy.sh` (from the Mac): rsync harness + bin/ + hosts.env to all hosts; run os-tune; install.sh of the
  chosen system on brokers (tarball from `CACHE=` dir if present, else downloads.apache.org, sha512-verified).
- `common/sampler.sh start|stop <tag>` on each broker: every 2 s one `key=value` line: unix time, cpu_ticks
  (system), per-role CPU ticks + RSS (kafka | zk | bookie | broker, found by main class), MemFree/Cached/Dirty/
  AnonPages, majflt, pgscan_direct, allocstall, psi some (cpu/mem/io), disk_wsect/rsect/flush of the data disk,
  net rx/tx bytes of the NIC holding the private IP.
- `kafka/kc.sh` (node): start|stop|wipe|format|health|cpu|stats|threads|mark — port the 09-24 k3/kc.sh
  (keep stats/threads), IP from hosts.env. `kafka/cluster.sh` (Mac): reset (stop, wipe, format with a fresh
  cluster id, start, wait healthy: 3 voters + 3 unfenced brokers) | health | stop.
- `pulsar/pc.sh` (node): start|stop <zk|bookie|broker|all>, wipe, health, stats, threads, mark.
  `pulsar/cluster.sh` (Mac): reset (stop all, wipe, start ZK×3 → initialize-cluster-metadata once → bookies×3
  (wait all 3 writable via `bookkeeper shell listbookies -rw`) → brokers×3 (wait /admin/v2/brokers/health on
  all), tenant `bench` + namespace `bench/ns` with bundles and persistence 3/3/2) | health | balance (print topic
  and bundle ownership per broker after create; `-fix` unloads bundles from the heaviest broker until within
  ±10%) | stop.
- `<system>/run.sh <tag> <rate> [k=v ...]` (Mac): reset cluster, create topics + warm (loader 1), start samplers,
  start 9 loader processes (3 per loader, loader-index 0..8, cons-offset i×C), wait for 9 READY (timeout
  `READY_TIMEOUT`, default 300 s), write start instant (now+5 s) to every start file, mid-run thread snapshot,
  wait, collect into `runs/<tag>/` (loader logs, samples, broker logs + GC logs, stats, configs actually used,
  `run.env` with every parameter). Never edit a running script in place (write new, then mv).
- `<system>/grid.sh`: the matrix below as a list of run.sh calls; skips points whose `runs/<tag>/DONE` exists.
- `report/report.py runs/<tag>...`: one row per run: system, shape, offered, last-30-s pushed/consumed/shed,
  e2e p50/p99 (worst loader) and e2e_local p99, produce p99, broker cores (3 summed), RSS/node, disk MB/s,
  net MB/s, errors; `--loaders` prints the per-process breakdown.

## 7. Matrix (1M msg/s offered unless stated, 256 B, batch 100, 60 s + 10 s ramp)

A. Rate, 1 topic × 200 partitions: 300k, 600k, 1M, 1.5M, 2M.
B. Partitions, 1 topic at 1M: 200, 2k, 10k, 50k, 100k (Kafka warm; Pulsar partitioned topic, failover).
C. Topics at 1M: 10, 100, 1000 topics × (10k and 100k total partitions).
D. Keys at 1M: 1 topic, Pulsar 48 partitions Key_Shared / Kafka 1k partitions, entities 1M and 10M (batch mode).
E. Real shape (optional): 10 topics, topic-zipf 1.0, 1M keys per topic zipf 1.1, keyed mode batch 1..20,
   proc-us 200, ramp the rate until e2e p99 > 500 ms.
F. Soak: 15 min at the best B point (retention and GC settings above keep the disk bounded).
Consumers: 22 per process at ≤200 partitions (198 group members ≤ partitions), else 33 (297 total).

## 8. Local rehearsal (the laptop, before any VM exists)

Docker VM: 10 CPUs, 11.7 GB RAM; the Mac has ~29 GB free disk — keep every test small and clean up.
- Loader functional tests: `apache/kafka:4.3.1` (single node) and `apachepulsar/pulsar:4.2.4 bin/pulsar
  standalone`, rate ≤ 20k msg/s, ≤ 60 s.
- Cluster rehearsal: 3 (+1 loader) `benchvm:24.04` containers (Ubuntu 24.04 + JDK 21, see
  localtest/Dockerfile.benchvm) on a dedicated bridge network, PROFILE=local, tarballs mounted read-only from the
  shared cache (`CACHE`). Kafka uses subnet 10.201.0.0/24, Pulsar 10.202.0.0/24 (never 10.114.x). Keep each
  stack ≤ 4 GB of container memory; `docker rm -f` and `docker network rm` when done; no volumes left behind.
- SSH shim for rehearsal: `SSH="localtest/dexec.sh"` maps `<ip> <cmd>` to `docker exec` of the container with
  that IP, so run.sh/cluster.sh run unchanged.
