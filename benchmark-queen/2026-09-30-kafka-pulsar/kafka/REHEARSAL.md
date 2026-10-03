# Kafka rehearsal, 2026-09-30 (laptop, before any VM)

Rig: 4 `benchvm:24.04` containers (Ubuntu 24.04 + OpenJDK 21.0.12.1) on network `kbench` 10.201.0.0/24: kb1 .2, kb2 .3,
kb3 .4 (brokers, `--memory 1100m --cpus 2`), kl1 .5 (loader, 800m); `--init`; tarball cache mounted read-only at
`/cache`; `PROFILE=local` (heap 384m). The Docker VM (10 CPUs, 11.7 GB) ran the Pulsar rehearsal (3 × 1.27 GB) and
mqload's single-node Kafka at the same time, so latencies here say nothing about Kafka; the point was the harness.
All commands from the harness root `H=benchmark-queen/2026-09-30-kafka-pulsar`, with
`HOSTS_ENV=kafka/rehearsal/hosts.env` (B_PUB=B_PRIV=10.201.0.2-4, L_PUB=L_PRIV=10.201.0.5, `SSH=common/dexec.sh`,
`REMOTE_ROOT=/root/bench`, `PROFILE=local`, `CACHE=/cache`).

## 1. Rig, deploy, install

```sh
kafka/rehearsal/up.sh
kafka/rehearsal/deploy.sh harness tune kafka     # common/deploy.sh on a staged copy whose hosts.env = the rehearsal one
```
- harness copied to all 4; `os-tune.sh`: the container refuses the non-namespaced sysctls (skipped, printed), e.g.
  `vm.max_map_count` stays 262144, `net.core.somaxconn = 4096 (already)`.
- `install.sh` on kb1-3: tarball from `/cache`, published sha512 `c7d7b231…21f4c54c` = computed, 135,852,628 bytes,
  unpacked to `/root/bench/kafka/dist`; a second run says "already in dist, nothing to do".

## 2. Cluster reset, health, settings

```sh
kafka/cluster.sh reset
kafka/cluster.sh stats; common/dexec.sh 10.201.0.2 'bash /root/bench/kafka/kc.sh threads 2'
```
- healthy in 10 s (6-27 s over 10 resets): every node reports `leader=1 voters=3 unfenced=3 fenced=0 maxlag=0`.
- `conf_hash=cb1d1e98` on all 3 nodes (rendered configs differ only in node.id + listener IPs); running JVM:
  `-Xms384m -Xmx384m -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+AlwaysPreTouch`,
  nofile 1048576; features `metadata.version=4.3-IV0 group.version=1` (KIP-848 on) `transaction.version=2`.
- the effective settings printed by reset are exactly the SPEC §5 table (+ internal topics RF 3 / min ISR 2, and the
  local-only `log.cleaner.dedupe.buffer.size=33554432`).
- stats: rss 533-539 MB, 135 threads, gc pauses parsed from `kafkaServer-gc.log` (15, max 40 ms). Two start-up ERROR
  lines on one node = `ControllerApis … DESCRIBE_CLUSTER` from the health probe before a leader existed (harmless).

## 3. Kafka's own tools (`smoke.sh`)

```sh
kafka/smoke.sh RATE=10000 SECS=30 PARTS=6
```
- `kafka-topics --create` 6 partitions RF 3, `min.insync.replicas=2`; describe: every ISR = 3 replicas.
- `kafka-producer-perf-test` (acks=all, idempotent, lz4, linger 5 ms, 1 MiB batches, 256 B): 300,000 records,
  9,994 rec/s, avg 9.46 ms, p50 4 / p95 9 / p99 251 / p99.9 305 ms (the first 5-s window holds the JVM/metadata
  warm-up: 644 ms max; later windows avg 4.3-4.5 ms, max ≤ 28 ms).
- `kafka-consumer-perf-test` in parallel: ~10,000 msg/s all along (299,037 by its last 5-s stat).

## 4. Broker stop/start: ISR shrink and recovery

First try: `kc.sh stop` on kb3 took 60 s and ended in SIGKILL although server.log said shutdown complete after 3 s.
Cause: PID 1 in the container was `sleep infinity`, which never reaps; the exited JVM stayed a zombie, and `kill -0`
succeeds on a zombie. Fixed on both ends: `kc.sh` treats a zombie as gone, `up.sh` runs containers with `--init`
(reaps like systemd on a droplet). Containers recreated, redeployed, then:

```sh
kafka/smoke.sh RATE=5000 SECS=45 PARTS=6 FAILOVER=1
```
- t=15 s: kb3 stopped cleanly in 2.9 s. ISR of all 6 partitions 3→2; the two partitions kb3 led moved to broker 1.
- producer: 225,000 sent, 0 failed (NOT_LEADER_OR_FOLLOWER warnings, retried by the idempotent producer), 4,997 rec/s,
  p50 7 / p95 123 / p99 186 / p99.9 227 ms; consumer: all 225,000.
- t=35 s: kb3 started again; 1 s after the produce ended every ISR was 3 again (`--under-replicated-partitions` empty).
- server.log: 28 + 22 `ISR updated to` lines on the two surviving brokers. A clean stop removes the replica through the
  controller, so there is no `Shrinking ISR` line; a crashed follower would log those.

## 5. Reset is idempotent

```sh
kafka/cluster.sh reset; kafka/cluster.sh reset     # + kafka-topics --list after each
```
- cluster ids `KaPbXdid8Y78hgvKYuXprw` then `BBMTDXfWQNVHmOJxQH9mzg`; the smoke topics were gone after each (empty
  list); healthy in 11 s and 15 s; clean stops 0.6-5.6 s.
- `cluster.sh start / health / versions / stop` on a formatted cluster: all fine. `kc.sh start` with a listener on
  :6650 (netcat standing in for Pulsar): `another system listens here (:6650): refusing to start`, rc 1.

## 6. run.sh end to end

Before `mqload/bin/linux-arm64/kload` existed I built a throwaway copy from mqload's source into my scratchpad
(`/root/privbin/kload` on kl1, `KLOAD=` override) to exercise the harness:

| tag | what | result |
|---|---|---|
| reh-priv-1x12-15k | 15k msg/s, 12 partitions, 2 consumers/process, 40 s | DONE in 90 s; 562,500 pushed, 562,400 popped, 0 shed, 0 errors; produce p99 29 ms, e2e p99 28 ms |
| reh-priv-fsync-keyed | `KPROFILE=fsync WARM=1 MODE=keyed BATCH=1 BATCH_MAX=5 ENTITIES=1000`, 10k msg/s | DONE; `log.flush.interval.messages=1` rendered, warm wrote 24 records in 2.3 s; the laptop's shared virtual disk cannot fsync every append: produce p99 7.2 s, 12 % shed (harness counted it correctly) |
| reh-priv-fail | `KX=-no-such-flag` | FAILED `topic create: DONE 2`, logs collected, cluster stopped, no kload left |

Then with the official binary (`mqload/bin/linux-arm64/kload`, sha256 `9533dde965bdc9ab…`, shipped by deploy.sh to
`/root/bench/bin/kload`), SPEC default shape (1 topic × 200 partitions, batch 100, 22 consumers per process, ramp 10 s):

```sh
HOSTS_ENV=kafka/rehearsal/hosts.env kafka/run.sh reh-1x200-18k 18000 DURATION=60s
report/report.py runs/kafka/reh-1x200-18k --loaders
```
- reset 27 s, create 6 s, 3 × READY 15 s later, start instant = kl1 clock + 5 s, DONE in 127 s.
- 990,000 offered = achieved = pushed = popped, 0 shed, 0 errors (push/pop/ack); produce p50 11 ms.
- one 10-s window carries a 1.4 s G1 mixed pause on kb1 (228M→165M of the 384m heap with the VM's CPUs shared), so the
  run's overall produce p99 is 2.0 s; report.py's last 30 s: 18k/s pushed, 18k/s consumed, e2e p50 14 / p99 322 ms.
- disk: 143 MiB per broker = ~150 B per message (lz4) → the README's disk table.
- mid-window threads (kb2): 0.97 cores = request handlers 0.32, replica fetchers 0.27, network 0.22.

```sh
HOSTS_ENV=kafka/rehearsal/hosts.env kafka/run.sh reh-1x2000-10k 10000 PARTITIONS=2000 CONSUMERS=4 DURATION=30s RAMP=5s
```
- `WARM=auto` → 1 at 2000 partitions: topic ready (leaders + full ISR + every replica log) after 12.1 s, warm wrote one
  record into each of the 2000 partitions in 5.4 s, READY again at once; 275,000 pushed, 274,900 popped, 0 shed,
  0 errors, produce p99 114 ms, e2e p99 146 ms; DONE in 94 s.

`runs/kafka/reh-1x200-18k/` (792 KB; all rehearsal runs were moved afterwards to `kafka/rehearsal/runs/` so that the VM
day's `runs/kafka/*` holds VM runs only):
```
DONE  run.env  run.log  summary.txt  cluster.id  cluster.log  cluster-stop.log  stats-before.txt  stats-after.txt
threads-mid-n1.txt  threads-mid-n2.txt  threads-mid-n3.txt
samples/     b1.txt b2.txt b3.txt l1.txt
loaders/l1/  create.sh create.log create.rc create.pid launch.sh p0.sh p0.log p0.json p0.rc p0.pid (p1, p2 the same)
brokers/nK/  server.properties jvm.env kafkaServer-gc.log server.log.gz controller.log.gz kafka-request.log.gz
             kafkaServer.out.gz ls.txt
```

## 7. Found, not mine

`common/sampler.sh` writes `net_rx=0 net_tx=0` and no `disk_*`: `sample_loop` runs inside
`nohup bash -c "$(declare -f sample_loop); sample_loop $IV"`, where `$IFACE` and `$DISK` are unset, so python gets
empty names (the header line, printed by the parent, shows `iface=eth0` correctly; `/sys/class/net/eth0/statistics/rx_bytes`
reads 153 MB in the same container). The report's disk and net columns will be 0 on the VMs until it passes them as
arguments.

## 8. Not covered by the laptop

eth1 + public/private split and real ssh; 3 loader hosts (only 1 × 3 processes here); os-tune's sysctls and the
`vm.max_map_count` refusal path on PROFILE=vm; 6g/10g heaps with AlwaysPreTouch; 2k-100k partitions (creation time,
warm, settle, group formation with 297 members); rates above 20k msg/s.

## 9. Cleanup

```sh
kafka/rehearsal/down.sh      # docker rm -f kb1 kb2 kb3 kl1; docker network rm kbench
```
Nothing left running; no volumes were created.
