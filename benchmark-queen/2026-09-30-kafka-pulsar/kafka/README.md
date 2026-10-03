# Kafka side of the Kafka vs Pulsar vs Queen harness

Kafka 4.3.1, KRaft, 3 combined broker+controller nodes on the 16 vCPU / 31 GB / one local ext4 disk droplets, driven by
9 `kload` processes (3 per loader). `../SPEC.md` is the contract; this file is how the Kafka part works and why each
setting is what it is. Local proof: `REHEARSAL.md`.

Durability class (print it next to every Kafka number): RF 3, `min.insync.replicas=2`, `acks=all`, idempotent
producers, unclean election off, **no fsync** (Kafka's production model: durability by replication, the OS flushes the
page cache). `acks=all` waits for every in-sync replica (normally all 3), never fewer than 2. `KPROFILE=fsync`
(`log.flush.interval.messages=1`) exists for one parity point only.

## Files

| file | runs on | what |
|---|---|---|
| `install.sh` | node | JDK 21 headless via apt if missing, Kafka 4.3.1 from `$CACHE` or downloads.apache.org (archive.apache.org fallback), sha512 checked against the published value (and a pinned copy), unpacked to `$REMOTE_ROOT/kafka/dist`. Idempotent. `common/deploy.sh kafka` calls it. |
| `mkconf.sh [default\|fsync]` | node | renders `kafka/conf/server.properties` + `kafka/conf/jvm.env` for THIS node: index = position of one of `hostname -I` in `B_PRIV` (never a NIC name), `node.id` = index+1. Every SPEC §5 value with a one-line reason above it. |
| `kc.sh` | node | `format <cid> [profile]`, `start`, `stop`, `wipe`, `health`, `cpu`, `stats`, `threads [s]`, `mark`, `conf`, `version`, `features`, `uuid` (header of the file has the details) |
| `cluster.sh` | Mac | `reset [default\|fsync]` (stop, wipe, format all 3 with ONE fresh cluster id, start, wait until every node sees 3 voters + 3 unfenced brokers, print versions / feature levels / effective settings), `health`, `stop`, `start`, `stats`, `versions` |
| `run.sh <tag> <total_rate> [KEY=VAL...]` | Mac | one point end to end → `runs/kafka/<tag>/` (below) |
| `grid.sh` | Mac | the SPEC §7 matrix (A B C D F; E optional) as `run.sh` calls; skips points with a `DONE` |
| `smoke.sh [KEY=VAL...]` | Mac | the cluster proven with Kafka's own tools from loader 1: `kafka-topics` (RF 3, min.insync 2), `kafka-producer-perf-test` + `kafka-consumer-perf-test`, `FAILOVER=1` = one broker stopped and restarted mid-produce (ISR 3→2→3) |
| `rehearsal/` | Mac | `up.sh` / `down.sh` (4 benchvm containers on `kbench` 10.201.0.0/24), `deploy.sh` (common/deploy.sh on a staged copy with the rehearsal `hosts.env`), `hosts.env` |

Layout on every host (SPEC §1): scripts `/root/bench/kafka`, dist `/root/bench/kafka/dist`, rendered config
`/root/bench/kafka/conf`, pid + mark `/root/bench/kafka/state`, data `/root/bench/data/kafka`, logs
`/root/bench/logs/kafka` (server.log, controller.log, state-change.log, kafkaServer-gc.log*, kafkaServer.out), loader runs
`/root/bench/runs/kafka/<tag>`, samples `/root/bench/samples/kafka-<tag>.txt`.

## VM day

```sh
H=benchmark-queen/2026-09-30-kafka-pulsar            # everything below from $H
cp hosts.env.example hosts.env                         # fill in the 6 IPs; PROFILE=vm
(cd mqload && ./build.sh)                              # mqload/bin/linux-amd64/kload
common/deploy.sh harness tune kafka clock check        # ship, os-tune, install.sh on the 3 brokers
kafka/cluster.sh reset && kafka/cluster.sh health      # 3 voters + 3 unfenced, group.version=1 (KIP-848)
kafka/smoke.sh RATE=50000 SECS=30 FAILOVER=1           # Kafka's own tools, ISR shrink + recovery
kafka/run.sh 1x200-300k 300000 CONSUMERS=22            # one point; then the matrix:
kafka/grid.sh                                          # ≈ 1.5-2 h; re-run to resume (DONE points are skipped)
report/report.py runs/kafka/* --loaders
```
`POINTS="B"` runs one series, `DRY=1` lists the calls, `BEST_B=<partitions>` picks the F soak point (after B).
Only one system may listen at a time: `kc.sh start` refuses while Queen (6632/7400) or Pulsar (2181/3181/6650/8080)
listens on the node, and `run.sh` stops the cluster at the end of every point (data stays until the next reset).

## One point: `run.sh <tag> <total_rate> [KEY=VAL...]`

1. `cluster.sh reset` with `PARTS=TOPICS×PARTITIONS` (heap rule) → `cluster.log`, `cluster.id`
2. `common/sampler.sh start kafka-<tag>` on the 3 brokers and the loader hosts (2 s)
3. topics by `kload -create-only` on loader 1 (RF 3, min.insync 2, kload's default topic config
   `retention.ms=600000,segment.bytes=268435456`), `-warm` when TOPICS×PARTITIONS ≥ 2000, `-topic-settle 30s` at ≥ 50k;
   `kc.sh stats` → `stats-before.txt`
4. `PROCS_PER_LOADER` (3) kload processes per loader host: loader-index i of N, `-rate total/N`, `-consumers C
   -cons-offset i·C -cons-total N·C`, `-local-src` = the indices on that host (e2e_local), `-start-file`; detached
   (`nohup`, pid + exit code in files) so an ssh drop cannot kill a run; wait for N × `READY <ms>` (READY_TIMEOUT 300 s)
5. `kc.sh mark` on the brokers (instant + jstack thread names, BEFORE the window), start instant = loader-1 clock + 5 s
   into every start file; mid-window `kc.sh threads 3` (from /proc only: no safepoint in the window); wait for all N
6. collect: `loaders/l<k>/` (p<i>.log, p<i>.json, p<i>.sh = the exact command, rc, create.log), `samples/b<k>.txt
   l<k>.txt`, `brokers/n<k>/` (server.properties, jvm.env, server/controller/request logs + kafkaServer.out gzipped,
   GC logs plain), `stats-before.txt` / `stats-after.txt`, `threads-mid-n<k>.txt`, `run.env` (every parameter + derived
   values + cluster id + start instant), `summary.txt`; cluster stopped; `DONE` (or `FAILED` + reason: grid.sh re-runs it)

Defaults = the SPEC §7 shape: 1 topic × 200 partitions, batch mode, batch 100 (keyed: 1), 256 B, ramp 10 s, duration
70 s (60 s + ramp), report 10 s, max-inflight 5000 (Queen 09-29 grid), consumers 22 per process at 1 topic ≤ 200
partitions else 33, poll 1000, async commit, GOGC 400. Every kload knob of SPEC §4 is a KEY (header of `run.sh`);
empty = kload's SPEC default (4 producers, linger 5 ms, lz4, 1 MiB batches, fetch 1 B / 500 ms, KIP-848 groups).

## Every broker setting and why

From SPEC §5 (rendered by `mkconf.sh`, the reason is also the comment above each line in `server.properties`):

| setting | value | why |
|---|---|---|
| process.roles | broker,controller | 3-node cluster, no extra controller boxes |
| controller.quorum.voters | `1@b1:9093,2@b2:9093,3@b3:9093` (private IPs) | static quorum, formatted once with ONE cluster id (no `--standalone`) |
| listeners / advertised | PLAINTEXT `:9092` + CONTROLLER `:9093` on the private IP | all traffic on the VPC, plaintext like Queen and Pulsar here |
| num.network.threads | 8 (3) | default is sized for 4-core boxes; ~1k connections per broker |
| num.io.threads | 16 (8) | = vCPU; request handlers run acks=all produce + fetch. Combined mode: the controller gets its own pool of the same size (mostly idle), so jstack shows 32 handler and 16 network threads |
| num.replica.fetchers | 8 (1) | per source broker (16 per node = vCPU); 09-30: at 4, 50k partitions gave acks=all p50 ~500 ms with Kafka at 3 cores/node (long follower fetch rounds) |
| message.max.bytes | 16 MiB (1 MiB) | 09-30: KIP-848 group metadata for 100k partitions is one batch > 1 MiB → every join failed (RecordTooLargeException); producer batches stay ≤ 1 MiB |
| background.threads | 16 (10) | partition.metadata flushes, retention, checkpoints at 100k partitions |
| num.recovery.threads.per.data.dir | 8 (2) | log load/flush at start/stop with 100k partitions |
| socket.send/receive.buffer.bytes, replica.socket.receive.buffer.bytes | -1 (100 KiB/100 KiB/64 KiB) | kernel autotuning; os-tune raises the maxima to 64 MiB |
| socket.listen.backlog.size | 4096 (50) | ~1k clients connect at READY; = net.core.somaxconn |
| max.incremental.fetch.session.cache.slots | 4000 (1000) | ≥ consumer + follower fetch sessions |
| default.replication.factor / min.insync.replicas | 3 / 2 | durability class |
| unclean.leader.election.enable / auto.create.topics.enable | false / false | never lose acked data; topics only on purpose |
| log.retention.ms / log.retention.check.interval.ms | 600000 / 60000 | bound the disk in soaks |
| log.segment.bytes | 256 MiB (1 GiB) | segments roll (and can be deleted) within a soak |
| compression.type | producer | never recompress the producer's lz4 batches |
| flush | none; `KPROFILE=fsync`: log.flush.interval.messages=1 | Kafka's model; fsync = parity point only |
| heap | `-Xms=-Xmx` 6g (≤ 10k partitions), 10g (> 10k; SPEC says ≥ 50k, nothing in the matrix sits between), `HEAP=` overrides; PROFILE=local 384m | rest of 31 GB = page cache, Kafka's read path |
| JVM | Kafka's own `KAFKA_JVM_PERFORMANCE_OPTS` read from the installed `kafka-run-class.sh` (`-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -XX:MaxInlineLevel=15`) + `-XX:+AlwaysPreTouch` | G1 as Kafka ships it; the heap pre-faulted at start |
| GC log | on (kafka-server-start's `-loggc` → `kafkaServer-gc.log`, 10 × 100 MB) | pauses in `kc.sh stats` (count, max, sum) |
| nofile / vm.max_map_count | 1048576 / ≥ 4194304 | 1 fd + 2 mmaps per active segment; `kc.sh start` sets `ulimit -n`, refuses on PROFILE=vm below the map count (os-tune sets it) |

Added beyond the SPEC table, and why:

| setting | value | why |
|---|---|---|
| offsets.topic.replication.factor, transaction.state.log.replication.factor, share.coordinator.state.topic.replication.factor | 3 | internal topics in the same class. These ARE Kafka's defaults; stated because the shipped `config/server.properties` lowers them to 1 |
| transaction.state.log.min.isr, share.coordinator.state.topic.min.isr | 2 | same (defaults, stated) |
| log.cleaner.dedupe.buffer.size | 32 MiB, **PROFILE=local only** | the cleaner allocates it on the heap at start (128 MiB = a third of the 384m rehearsal heap); droplets keep the default |
| CLI tools (health, features, smoke) | `-Xmx256m`, SerialGC, C1 only, niced | a probe must not compete with the broker it measures |
| mid-window `kc.sh threads` | names from the jstack taken at `mark` | jstack is a safepoint; it happens 5 s before the window, not inside it |

Opt-in knobs (off by default, marked "NOT part of SPEC" in the rendered files and recorded in `run.env`):
`KPROPS=k=v,k=v` (any broker property) and `JVM_EXTRA=...` (flags after Kafka's own). What an operator might argue for:
- `JVM_EXTRA=-XX:+UseTransparentHugePages`: THP stays madvise (SPEC); with AlwaysPreTouch the 6-10 GB heap would be
  backed by huge pages once at start (fewer TLB misses, no runtime compaction because nothing is faulted later).
- `KPROPS=log.roll.ms=300000` for a soak at ≥ 10k partitions: 1M msg/s over 10k partitions is ~15 KB/s per partition,
  a 256 MiB segment never rolls in 15 min, so retention cannot delete anything (disk still fits, see below).
- `KPROPS=queued.max.requests=1000` if `RequestQueueTimeMs` shows up; ≤ 36 producer clients × 5 in flight per broker
  stay under the default 500, so it is not set.

## Expected disk and RAM per point (per broker)

Measured in the rehearsal: ~150 B on disk per message (143 MiB per broker for 990,000 messages, indexes and
__consumer_offsets included: the ~319 B stamped JSON in lz4 batches of 100, ≈ 2.1×). RF 3 on 3 nodes = every broker
stores every message once, so disk per broker ≈ messages × 150 B (the 10 s ramp counts half).

| point | messages per broker | disk per broker | note |
|---|---|---|---|
| A 300k / 600k / 1M / 1.5M / 2M, 70 s | 20M / 39M / 65M / 98M / 130M | 3 / 6 / 10 / 15 / 20 GB | fits in page cache: consumers never read the disk |
| B, C, D at 1M, 70 s | 65M | ~10 GB | index files are sparse: 100k partitions add little |
| F soak 15 min at 1M | 905M | ~135 GB | at 200 partitions a segment rolls every ~6-7 min, the first becomes deletable after ~17 min: nothing is deleted inside the soak; 400 GB disk |
| smoke / rehearsal | — | < 100 MB | |

Each `cluster.sh reset` wipes the data, so only one point is ever on disk. RAM per broker: heap 6 GB (≤ 10k partitions)
or 10 GB, + ~0.6-1 GB off-heap → RSS ~7 / ~11.5 GB; the remaining ~20 GB is page cache, which holds a whole 70 s point.
Loaders: franz-go keeps ~0.5 KB per client per partition (09-24, kfake): 37 clients × 100k partitions ≈ 2 GB per
process, ≈ 6 GB per loader at 100k, plus GOGC=400 headroom (`GOMEMLIMIT=` caps it if needed).

## Traps (09-24 notes and this rehearsal)

- **Cluster id must not start with `-`**: `kafka-storage.sh format -t -abc…` reads it as a flag. `cluster.sh` draws
  base64url ids until one does not (like Kafka's `Uuid.randomUuid()`); `kc.sh format` rejects bad ones.
- **server.log rotates** (log4j2 hourly `server.log.YYYY-MM-DD-HH`, and at 100k partitions the volume is large):
  `kc.sh stats` counts over every `server.log*` file, "since mark" by timestamp; `run.sh` collects all of them, gzipped.
- **scp drops exec bits**: 09-24 lost a whole sweep's CPU/disk numbers to `kc.sh: Permission denied`. Every script
  here is invoked as `bash <script>`; deploy.sh also chmods.
- **JDK 21 jstack prints `nid=` in decimal** (older JDKs: hex `0x…`); `kc.sh threads` accepts both.
- **Zombie brokers in containers**: with `sleep infinity` as PID 1 nothing reaps the exited JVM, `kill -0` still
  succeeds and a clean stop looked like a 60 s hang ending in SIGKILL. `rehearsal/up.sh` uses `--init` (reaps like
  systemd on a droplet) and `kc.sh` treats a zombie as gone. Clean stop: 0.6-5.6 s.
- **Health probe ERRORs at start**: `ControllerApis … DESCRIBE_CLUSTER` ERROR lines are `kc.sh health` asking before
  the quorum has a leader; harmless, and before `mark` (`errors_since_mark` excludes them).
- **`pgrep -f` over ssh matches its own shell** (the pattern is in `bash -c`'s command line): use `[x]yz` patterns.
- **`kafkaServer.out` duplicates server.log** (log4j2 root logs to STDOUT too, Kafka as shipped); collected gzipped.
- **KIP-848** needs the `group.version` feature ≥ 1: `cluster.sh reset` prints the finalized features (4.3.1: 1).
- **Big partition counts**: 09-24 needed ~4 min to create 100k partitions (+ ~40 s warm); `run.sh` passes
  `-topic-timeout 1800s`, `-topic-settle 30s` at ≥ 50k, grid.sh READY_TIMEOUT 900 s / GROUP_TIMEOUT 600s there.
  A classic-protocol group over 100k partitions writes ~0.5 MB of group metadata (< 1 MiB message.max.bytes; at 500k
  it overflowed on 09-24); KIP-848 stores per-member records.
- **vm.max_map_count** cannot be set inside a container (262144 there): `kc.sh` warns on PROFILE=local and refuses on
  PROFILE=vm below 4194304 (run `common/os-tune.sh broker`).
- **The first seconds after start** show a latency spike (metadata, JIT): the 10 s ramp absorbs it.
- **`common/sampler.sh` (not ours) records net/disk = 0**: `sample_loop` runs in `nohup bash -c "$(declare -f …)"`
  where `$IFACE`/`$DISK` are unset, so the python gets empty names. Needs a fix before VM day (pass them as arguments).
