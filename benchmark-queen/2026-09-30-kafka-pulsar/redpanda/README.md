# Redpanda side of the harness: the Kafka matrix re-run on Redpanda

Redpanda 26.2.3 (the newest stable on 2026-10-01; git 3c9fc8dd, released 2026-09-17) from Redpanda's official apt repository,
3 nodes on the same droplets as the Kafka and Pulsar runs (16 vCPU / 31 GB / one local ext4 disk, VPC on eth1), driven by the
same 9 `kload` processes (3 per loader) with the same client settings, tuned the way Redpanda's production deployment guide
does it: production mode, `rpk redpanda tune all`, `rpk iotune`. `../SPEC.md` is the contract; this file is how the Redpanda
part works, every override and why, and the traps hit.

**Durability class (print it next to every Redpanda number):** RF 3 raft, `acks=all` acknowledged once a **majority (2 of 3)
has fsynced** the batch (`write_caching=false`, Redpanda's default), idempotent producers. This is Queen's and Pulsar's class
(3 copies, ack after 2 fsyncs), and **stronger than the Kafka runs** (RF 3, min.insync 2, acks=all, NO fsync: page cache).
Redpanda has no `min.insync.replicas` (it logs `not supported configuration min.insync.replicas ... will be ignored`): the raft
majority is the rule. `cluster.sh reset` proves it on a live partition every time (below).

## Files

| file | runs on | what |
|---|---|---|
| `install.sh` | node | the official repo's steps written out (signing key fingerprint pinned, one apt source line), `redpanda=26.2.3-1 redpanda-rpk=26.2.3-1 redpanda-tuner=26.2.3-1`, then **disables** `redpanda.service` + `redpanda-tuner.service` (the postinst enables them: a reboot would start Redpanda on :9092 and run the tuners under another system). Idempotent. `common/deploy.sh redpanda` calls it. |
| `mkconf.sh` | node | renders `conf/redpanda.yaml` with rpk itself (`rpk redpanda config bootstrap --self <VPC IP> --ips <3 VPC IPs>`, `rpk redpanda mode production`, `rpk redpanda config set ...`), `conf/.bootstrap.yaml` (cluster properties, read when the cluster forms), `conf/overrides.txt` (every override with default and reason), `conf/io-config.yaml` (the node's iotune result) |
| `rc.sh` | node | `render tune untune iotune check start stop wipe health stats threads busy transfers mark conf version ports` (header of the file) |
| `cluster.sh` | Mac | `reset` (stop, wipe, render, tune, iotune once, drop caches, start, healthy, overrides verified, durability proof), `proof`, `health`, `stop`, `start`, `stats`, `versions`, `tune`, `untune`, `iotune`, `check`, `ports` |
| `run.sh <tag> <rate> [K=V...]` | Mac | one point end to end → `runs/redpanda/<tag>/`; `kafka/run.sh` ported (same keys, defaults, layout) |
| `grid.sh` | Mac | exactly the 19 Kafka points that are DONE in `runs/kafka/`, in order; `DRY=1` lists, DONE points skipped |

Shared files changed for Redpanda: `common/deploy.sh` (ships `redpanda/`, step `redpanda`, `check` lists 9644/33145/8081/8082),
`common/sampler.sh` (role `redpanda` = the process `redpanda --redpanda-cfg ...`), `report/report.py` (role `redpanda`).

Layout on every broker (SPEC §1): config `/root/bench/redpanda/conf`, pid/mark/OS baseline/iotune result
`/root/bench/redpanda/state`, data `/root/bench/data/redpanda` (the ext4 root disk, as every system), log
`/root/bench/logs/redpanda/redpanda.log`. The package's own `/etc/redpanda` and `/var/lib/redpanda` stay unused.

## Run it

```sh
H=benchmark-queen/2026-09-30-kafka-pulsar           # everything below from $H, bash (not zsh)
(cd mqload && GOWORK=off ./build.sh)                  # kload; the repo's go.work must not capture mqload
common/deploy.sh harness tune redpanda clock check    # ship, os-tune, install Redpanda on the 3 brokers
redpanda/cluster.sh reset && redpanda/cluster.sh health   # first reset runs rpk iotune (10 min per node, in parallel, once)
redpanda/run.sh 1x200-300k 300000 TOPICS=1 PARTITIONS=200 CONSUMERS=22   # one point (the first of the grid)
DRY=1 redpanda/grid.sh                                # the 19 points; then:
redpanda/grid.sh                                      # resumable: DONE points are skipped, FAILED ones re-run
report/report.py runs/redpanda/* --loaders            # plus summary.txt's "shards busy" + "broker produce latency" lines
redpanda/cluster.sh stop && redpanda/cluster.sh untune   # BEFORE any other system runs on these brokers
```

Only one system may listen at a time: `rc.sh start` refuses while Queen (6632/7400), Kafka (9092/9093) or Pulsar
(2181/3181/6650/8080) listens (`FORCE=1` overrides) and when a stray redpanda process exists; Kafka's `kc.sh` and Pulsar's
`pc.sh` already refuse on 9092, which Redpanda holds. `run.sh` stops the cluster at the end of every point.

**`cluster.sh untune` matters.** The Redpanda tuners change OS state that the other systems were measured without (IRQ
affinity, RPS/RFS, disk scheduler, clocksource: SPEC §5 says no IRQ pinning). `rc.sh tune` saves every value before its first
write (`state/os-baseline.sh`, `.values`), `untune` writes them back and checks. The next `reset` tunes again.

## One point: what run.sh does differently from kafka/run.sh

Same tags, rates, partitions, topics, keys, consumers (22 per process at 1x200, else 33), durations (70 s incl. 10 s ramp),
start barrier, warm (≥ 2000 partitions), `-topic-settle 30s` at ≥ 50k, loader layout, `report.py` columns. Differences:

1. **Consumer groups: classic + cooperative-sticky** (`GROUP_PROTOCOL=classic` in run.env). Redpanda 26.2.3 answers
   `ConsumerGroupHeartbeat(68): UNSUPPORTED, ConsumerGroupDescribe(69): UNSUPPORTED` (Kafka's own
   `kafka-broker-api-versions.sh` against the cluster, 10-01; open request redpanda#29223). kload's classic mode is franz-go's
   `CooperativeStickyBalancer`, the best client-side assignor (incremental, no stop-the-world). The Kafka runs used KIP-848
   with the broker's uniform assignor: group formation differs, steady-state consumption does not.
2. **`-min-isr 0`**: kload does not send `min.insync.replicas` (Redpanda would ignore it with a log line).
3. **Leader balancer settle**: Redpanda's leader balancer moves leaders ~30 s after a topic appears (seen: 26 transfers in one
   burst at +30 s for 200 partitions). After all READY, run.sh waits until no node has started a transfer for 20 s and at least
   40 s passed since the topics were ready (cap 300 s) — `LEADER_SETTLE_WAITED_S`, `LEADER_TRANSFERS_BEFORE_WINDOW` in run.env;
   `transfers_since_mark` in stats-after.txt shows any transfer inside the window.
4. **Broker CPU, two numbers.** The sampler's `redpanda` role (report.py "brk cores"/"sys cores") counts /proc CPU, which for
   Seastar includes the reactors' polling (idle node: 0.57 cores in /proc, 0.06 cores of real work). `summary.txt` adds
   `shards busy`: `redpanda_cpu_busy_seconds_total` (all shards) between the start and the end of the steady window
   (`busy.txt`), and `threads-mid-n<k>.txt` shows both for 3 s mid-window. Compare systems on /proc cores; read Redpanda's
   headroom from shards busy. The same snapshots carry the broker-side produce latency histogram
   (`redpanda_kafka_request_latency_seconds`, log2 buckets): summary.txt prints p50/p99/p999 per node over the window.
5. **Page cache dropped** before every start (`DROP_CACHES=1`): Redpanda does O_DIRECT I/O and takes ~29 GiB itself; a page
   cache full of another system's files would be reclaimed during the window instead.
6. Collected per broker: `conf/redpanda.yaml`, `conf/bootstrap.yaml`, `conf/overrides.txt`, `conf/io-config.yaml`,
   `state/tune.txt`, `state/installed`, `redpanda.log.gz` (above `LOG_MAX_MB`=256: `.head.gz` 64 MiB + `.tail.gz` 128 MiB +
   `.digest` of every WARN/ERROR kind), `ls.txt`; per run: `cluster-config.json` (every cluster property in force, defaults
   included), `busy.txt`, `stats-before/after.txt`, `threads-mid-n<k>.txt`.

Client side, identical to the Kafka runs (kload defaults, SPEC §4): acks=all, idempotent, 4 producer clients per process,
linger 5 ms, lz4, 5 in flight per broker, 1 MiB batches, fetch min 1 B / max wait 500 ms / 1 MiB per partition, async commit
per poll, poll 1000, max-inflight 5000 units of 100 messages, GOGC 400 (GOMEMLIMIT=9GiB where the Kafka points had it).

## Every override and why

Rendered per node into `conf/overrides.txt` (and the cluster ones into `conf/.bootstrap.yaml`); defaults are Redpanda 26.2.3's
(`src/v/config/configuration.cc` v26.2.x and `rpk cluster config export --all` on a fresh cluster).

| scope | setting | value (default) | why |
|---|---|---|---|
| node | seed_servers, rpc_server, kafka_api, admin | the 3 VPC IPs / this node's VPC IP, ports 33145 / 9092 / 9644 (0.0.0.0) | `rpk redpanda config bootstrap --self --ips`: all traffic on eth1, never the public IP nor DO's 10.19.x anchor; plaintext as every system here |
| node | node_id | index + 1 (auto) | n1..n3 = node 1..3 like Kafka's node.id; every reset wipes all three, so ids never repeat on another box |
| node | empty_seed_starts_cluster | false (true) | Redpanda's production bootstrap: the 3 seeds form the cluster together |
| node | developer_mode | false (true in the packaged yaml) | `rpk redpanda mode production`: real fsyncs, full checks |
| node | data_directory | /root/bench/data/redpanda (/var/lib/redpanda/data) | SPEC layout; the droplet's one ext4 disk, same as every system (Redpanda recommends XFS: the start check warns, nothing else to do on this rig) |
| node | rpk.ballast_file_path | data dir (/var/lib/redpanda/data/ballast) | the 1 GB ballast lives and dies with the data |
| node | pandaproxy, schema_registry | removed (on) | unused HTTP services in a Kafka-protocol benchmark |
| start | `rpk redpanda start --check=true` | the unit's START_ARGS | production checks at start (warnings only here, below) |
| start | smp / memory | Seastar defaults: 16 shards, 29.06 GiB (2.03 GiB left to the OS) | one Redpanda per 16-core node, nothing else runs there |
| start | overprovisioned | false | dedicated box: pinned reactors that poll (see "Broker CPU, two numbers") |
| start | io-properties | conf/io-config.yaml from `rpk iotune` (none) | Seastar's I/O scheduler sized to the measured disk |
| process | user, limits | root via nohup, nofile 1048576, memlock unlimited, oom_score_adj -950 (unit: user redpanda, 800000, infinity, -950) | the harness layout under /root/bench; the unit's limits kept or raised. The unit's slice weights (IOWeight 1000, MemoryMin 2G) only matter against competing processes: there are none |
| cluster | default_topic_replications | 3 (1) | durability class (kload also asks RF 3) |
| cluster | internal_topic_replication_factor | 3 (3) | stated: __consumer_offsets in the same class |
| cluster | write_caching_default | false (false) | stated: the durability class (ack after a majority fsynced) |
| cluster | kafka_batch_max_bytes | 16 MiB (1 MiB) | = Kafka's message.max.bytes in the Kafka runs; producer batches stay ≤ 1 MiB |
| cluster | log_retention_ms | 600000 (7 days) | = Kafka's log.retention.ms (kload sets retention.ms=600000 per topic too) |
| cluster | rpc_server_listen_backlog | 4096 (unset: Seastar's 100) | ~1k connections arrive at READY; = somaxconn, as Kafka's socket.listen.backlog.size |
| cluster | topic_partitions_per_shard | 7000 (5000) | admission only: 100k partitions RF 3 = 100k replicas per node = 6250 per shard; 16 x 5000 = 80k per node cannot admit the 100k points |
| cluster | topic_partitions_memory_allocation_percent | **by PARTS**: 10 up to ~13k partitions, else the need rounded up to 5, **capped at 40** (10, max 80): 50k, 75k, 100k → 40 | admission (replicas per node ≤ memory × pct / topic_memory_per_partition; 10% admits ~14.9k) AND a real reservation carved out before the Kafka/RPC/chunk-cache budgets (`memory_groups.cc`; per shard at 10%: Kafka 533 / RPC 355 / chunk cache 267 MiB, at 40%: 341 / 227 / 170, at 75%: 116 / 77 / 58). The cap is measured: at 75% a 100k creation never converged (raft starved of RPC memory), at 40% it was ready in ~60 s. Small points keep 10: a global raise would cut every point's budgets |
| cluster | topic_memory_per_partition | 200 KiB (200 KiB); **lowered only where the 40% cap binds**: 75k → 144 KiB, 100k → 108 KiB | admission only, so the 40% reservation admits the point; the memory really taken is measured: ~155-180 KB per replica at rest (RSS 11.1 / 14.0 / 17.2 GB per node at 50k / 75k / 100k, of the 29.06 GiB Redpanda holds) |
| cluster | segment_fallocation_step | **by PARTS**: 32 MiB up to 1000 partitions, 16 MiB at 2k, 2 MiB at 10k, 512 KiB at 50k, 256 KiB at 100k (32 MiB) | every partition's segment is fallocated to the step at birth (measured: 200 partitions = 6.3 GB on disk): 10k x 32 MiB = 320 GB of 387 GB, 50k = 1.6 TB. Rule: largest power of two ≤ 32 GiB / PARTS |
| cluster | auto_create_topics_enabled | false (false) | stated |
| cluster | (kept) partition_autobalancing_mode continuous, core_balancing_continuous true, enable_leader_balancer true, max_concurrent_producer_ids 100000/shard, group_initial_rebalance_delay 3 s, fetch_read_strategy non_polling | defaults | Redpanda's production behavior; with RF 3 on 3 nodes nothing can move between nodes; 36 idempotent producers x partitions touched in 70 s stay < 100k per shard |
| opt-in | `RPROPS=k=v,...`, `START_FLAGS="--smp=15 ..."`, `FALLOC=`, `MEMPCT=`, `TUNE=0` | none | NOT part of the expert set; marked in the rendered files and run.env |

## Tuners (production mode + `rpk redpanda tune all`, recorded in `state/tune.txt` per node)

Enabled by production mode: aio_events, ballast_file, clocksource, cpu, disk_irq, disk_nomerges, disk_scheduler,
disk_write_cache, net, swappiness (not fstrim, coredump, transparent_hugepages). What they changed on these droplets (same on
all three; values before = the SPEC os-tune state):

| tuner | change |
|---|---|
| aio_events | fs.aio-max-nr 65536 → 10000137 |
| clocksource | kvm-clock → tsc |
| disk_scheduler / disk_nomerges | vda: mq-deadline → none, nomerges 0 → 2 |
| disk_irq (mode sq_split: vda is a non-NVMe virtio disk) | vda's config IRQ → CPU0; its request-queue IRQ (virtio4-req.0) **refused by the kernel** (stays ffff) |
| net (mode mq: 16 cores / 8 rx queues ≤ 4 per queue) | the 32 virtio-net IRQs of both NICs from the driver's CPU pairs to one CPU each (16 CPUs); eth1 RPS on all 16 CPUs (rps_cpus ffff), rps_flow_cnt 4096 per queue, net.core.rps_sock_flow_entries 0 → 32768 (RFS); somaxconn/syn backlog already 4096/8192 |
| swappiness | already 1 (os-tune) |
| cpu | nothing to do (no cpufreq governor in the VM, 1 thread per core; no reboot allowed) |
| ballast_file | 1 GB file in the data dir |
| disk_write_cache | not supported (GCP only) |

`rpk redpanda check` after tuning still warns (all warnings, none fatal): data directory on ext4 not XFS, 2005 MB free memory
per CPU < 2048 (31 GB / 16 vCPU), swap not enabled (the droplets have none). `rpk iotune` (10 min, vendor default) result per
node: `state/io-config.yaml` (collected per run as `conf/io-config.yaml`).

## Partition density: what the matrix needs and where the limits are

Every node holds every partition (RF 3 on 3 nodes). The three admission checks (`partition_allocator.cc`): cores
(replicas ≤ 48 shards × topic_partitions_per_shard), memory (≤ 3 × 29.06 GiB × pct / 200 KiB), fds (≤ 3 × 1048576 / 5).
Redpanda's sizing guide asks ~2 MB of memory per partition replica, i.e. ~15k replicas per node on 31 GB: the 50k and 100k
points are 3x and 7x beyond the vendor's sizing on this hardware. They are configured to start anyway (above), and what
happens is measured, not assumed. With the rules above all of 50k, 75k and 100k RF 3 create, elect, replicate and warm in
about 1-1.5 minutes (table below); the first rule tried (reservation by need, 75% at 100k) did not converge and is kept in the
table as the reason for the 40% cap.

## Measured on 10-01 (setup logs in `runs/redpanda-setup-1001/`)

**Smoke point** `redpanda/run.sh 1x200-300k 300000 TOPICS=1 PARTITIONS=200 CONSUMERS=22` (the grid's first call; final
code, DONE in 200 s; an earlier run with the same cluster settings, before the broker-latency line was added, is kept in
`runs/redpanda-setup-1001/smoke-v1-1x200-300k`: e2e p99 53, brk cores 23.4, shards busy 4.10), next to Kafka's:

```
run         sys       shape                                    offered  push/s  cons/s  shed/s  e2e p50  e2e p99  local p99  prod p99  brk cores  sys cores  rss GB/node  disk w MB/s/node  net rx/tx MB/s/node  errs  load cpu
1x200-300k  redpanda  1x200 e=0 batch b=100 rr c=22x9 classic   300000    300k    300k      0k        9       66         73        58       23.7       22.9          8.4               104                46/46     0       1.5
1x200-300k  kafka     1x200 e=0 batch b=100 rr c=22x9 default   300000    300k    300k      0k        8       10         10        10        4.0        3.6          6.5                39                46/46     0       1.9
```
summary.txt: 19,499,900 messages produced, 19,499,600 consumed (300 in flight at close), 0 errors;
`shards busy ... n1=1.90 n2=1.20 n3=1.02 sum=4.11 cores`;
`broker produce latency ... n1 p50<=2.0 p99<=65.5 p999<=131.1 ms | n2 p50<=2.0 p99<=8.2 p999<=32.8 ms | n3 p50<=2.0 p99<=8.2 p999<=32.8 ms`.
No leadership transfer inside the window (33 before it, settle waited 35 s).

How to read it:
- **CPU**: 23.7 cores on the three boxes by /proc (what report.py shows for every system) against 4.11 cores of real
  Redpanda work: Seastar's reactors poll. At 300k msg/s /proc overstates Redpanda's need ~5x; at higher rates the gap closes.
- **Tail latency is one node's**: the broker-side produce p99 is ≤ 8 ms on n2 and n3 and ≤ 65 ms on n1 (165.227.154.114), the
  node with CPU steal (0.21 cores, the others 0), the lowest iotune IOPS (92k write vs 122k / 164k), fewer and slower
  flushes (2.75k/s vs 3.9-4.2k/s), the highest psi io, and the controller role. Every acks=all produce waits for a
  majority fsync: Redpanda issues ~3-4k device flushes per second per node at 300k msg/s (Kafka: 9/s, page cache), on ext4
  (Redpanda tests on XFS; this rig has one ext4 disk). Client-side produce p50 is 8.6 ms of which ~5 ms is linger.

**Partition creation, run.sh's exact create step** (`kload -create-only -warm -topic-settle 30s`, cluster reset with the
point's PARTS so the per-point rules apply; `runs/redpanda-setup-1001/create-*.log`):

| 1 topic, RF 3 | 50 000 | 75 000 (55%, before the cap) | 100 000 at 75% (first rule) | **100 000 at 40%** (final rule, estimate 100 KiB) |
|---|---|---|---|---|
| reservation / per-shard Kafka, RPC, chunk cache | 40%: 341 / 227 / 170 MiB | 55%: 244 / 163 / 122 MiB | 75%: 116 / 77 / 58 MiB | 40%: 341 / 227 / 170 MiB |
| controller accepted | 4.8 s (10 requests of 5000) | 6.1 s (15) | ~7 s | 6.7 s (20) |
| leaders + full ISR + replica logs on disk | ~36 s | ~49 s | **never**: 40 344 leaderless, 2 606 under-replicated after 12.5 min (stopped) | ~60 s |
| warm (1 record per partition, acks=all) | 5.2 s | 5.3 s | - | 5.4 s |
| whole create step (incl. 2 × 30 s settle) | 104 s | 116 s | - | 127 s |
| RSS per node after create + warm | 11.1 GB | 14.0 GB | 20-23.5 GB and +0.5 GB/min | 17.2 GB |
| fds / data per node | 50.2k / 26.8 GB | 75.2k / 20.8 GB | | 100.2k / 27.4 GB |
| leaders per node | 16667/16666/16667 | 24999/25001/25000 | | 33333/33333/33334 |
| delete (accepted / dirs gone) | 1 s / 42 s | 1 s / 76 s | killed + wiped | 1 s / 97 s |
| redpanda.log per node | ~0.6 GB (with the delete) | ~0.9 GB | 3.8-6.9 GB in 12 min (~7 MB/s of raft warnings) | |

The non-converging 100k run (75%) logged "reconciliation seems stuck" (controller_backend, ERROR), raft recovery
`client_request_timeout`, "Too long queue accumulated for raft_recv", and millions of "received vote_request reply from the node
where ntp ... does not exist". Every creation burst logs Seastar "Too long queue accumulated for main (~1300 tasks)" and a few
"oversized allocation" notices (non-fatal); deletions of 75k/100k log "reconciliation seems stuck" at ERROR while they proceed.
At 50k-100k the leader balancer moves ~10-20k leaders during creation; run.sh's settle step waits for it to go quiet.

## Expected disk and RAM per point (per broker)

Data per message as Kafka (lz4 batches of 100, ~150 B per message per replica); plus the fallocated tail of every segment
(PARTS × step: 6.4 GB at 200 partitions, 20 GB at 10k, 25 GB at 50k and 100k) and the 1 GB ballast. `storage_min_free_bytes`
= 5 GiB: below that Redpanda rejects producers. The A 4M point writes ~40 GB per broker in 70 s: fits. RAM: Redpanda takes
29.06 GiB (its RSS grows into it); 2 GiB stay with the OS, the sampler and sshd.

## Traps hit (10-01)

- **The official repo moved** to `linux.pkg.redpanda.com` (a GCP Artifact Registry; the old dl.redpanda.com Cloudsmith token
  answers `TOKEN_NOT_ACTIVE`). Its signing key is GCP AR's `35BAA0B33E9EB396F59CA838C0BA5CE6DC6315A3`.
- **The package enables its units** (`redpanda`, `redpanda-tuner`) without starting them: a reboot would start Redpanda with
  the packaged config on 0.0.0.0:9092 and run the tuners. install.sh disables both.
- **`topic_partitions_memory_allocation_percent` is not an admission knob only** (see the table): a global 80% would have
  starved every small point's Kafka request memory, and 75% at 100k starved raft's RPC memory so badly that the creation never
  converged (40% converges). Values > 80 are rejected at bootstrap ("must be at most 80") and the bootstrap then imports 80.
- **Fallocation fills the disk at 10k+ partitions** with the default 32 MiB step (each new partition's segment is 32 MiB on
  ext4 at once).
- **rpk's tuning plan only lists what still differs**: the OS baseline must be taken per value before its first write, or a
  second pass would record tuned values as "before". rc.sh does that.
- **Seastar polls**: /proc CPU of an idle node is ~0.5 cores; use the shards-busy line to judge headroom.
- **`rpk` probes AWS IMDS** on every call (`WARN falling back to IMDSv1 ...`): harmless noise, filtered out of the logs.
- **A 100k-partition failure floods the log** (GBs in minutes): run.sh collects a head + tail + WARN/ERROR digest above
  `LOG_MAX_MB` (256) instead of the whole file.
- **`rpk` prints string properties quoted** (`write_caching_default` → `"false"`): the overrides check strips the quotes.
- **The kload binary differs from the Kafka runs' sha (6b7436e6…)** only by Go's VCS stamp (`vcs.revision` of the queen repo
  HEAD): mqload sources are unchanged since before the Kafka runs (file mtimes), franz-go v1.22.1 in both.

## Before the 19-point grid (for whoever runs it)

- **Duration**: ~3.5 min per point up to 10k partitions (smoke: 200 s), ~10 min at 50k, ~12-15 min at 100k (create step
  2 min, a 297-member classic group over 100k partitions, the settle, collect). All 19: about 2 hours.
- **First-ever territory** (creation is proven, the load is not): B 1x50000 and the four 100k-replica points (B 1x100000,
  C 10x10000, 100x1000, 1000x100). With the 40% reservation each shard keeps Kafka 341 / RPC 227 / chunk cache 170 MiB, and
  17 GB of the 29 GiB is partition state at 100k; a classic consumer group over 100k partitions has never been tried here.
  READY_TIMEOUT=900 / GROUP_TIMEOUT=600s / TOPIC_TIMEOUT=1800s as the Kafka points had. If a 100k point fails, its run dir
  keeps the evidence (head/tail/digest of a big log); the 75% rule's failure is the template of what it looks like.
- **1000-topic points** (C 1000x10, 1000x100): many CreateTopics; not timed here.
- **A 3M / 4M**: every produce is a majority fsync; expect the fsync path (and n1) to set the ceiling, not CPU.
- **Report both CPU numbers** (report.py's /proc cores and summary.txt's shards busy) and the durability class: Redpanda
  fsyncs before ack, Kafka did not. A Kafka-class parity point is one knob away: `RPROPS=write_caching_default=true` (ack
  after a majority received the batch, flush in the background: Redpanda's own relaxed mode), not part of the grid.
- **After the grid**: `redpanda/cluster.sh stop && redpanda/cluster.sh untune` before Queen, Kafka or Pulsar run here again.
