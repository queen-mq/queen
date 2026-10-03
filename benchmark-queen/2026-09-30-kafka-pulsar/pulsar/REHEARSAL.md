# Pulsar rehearsal, 2026-09-30 (laptop, OrbStack Docker VM: 10 CPUs / 11.7 GB shared with the Kafka and mqload agents)

Rig: `benchvm:24.04` (Ubuntu 24.04, OpenJDK 21.0.12.1, arm64) on network `pbench` 10.202.0.0/24: pb1 .2, pb2 .3, pb3 .4
(`--memory 1300m --cpus 2`, each ZooKeeper + bookie + broker), pl1 .5 (`--memory 700m --cpus 2`, loader); tarball cache
mounted read-only at `/cache`. `rehearsal/hosts.env`: B_PUB=B_PRIV=(.2 .3 .4), L_PUB=L_PRIV=(.5),
SSH=`common/dexec.sh`, PROFILE=local, CACHE=/cache, REMOTE_ROOT=/root/bench. All commands from the harness root.

```
S=<scratchpad>/pstage                                  # staging dir for deploy.sh (see rig.sh: deploy reads hosts.env
export HOSTS_ENV=$PWD/pulsar/rehearsal/hosts.env       #  from the harness root; the stage's root hosts.env = rehearsal's)
export RUNS=$PWD/pulsar/rehearsal/runs
pulsar/rehearsal/rig.sh up
STAGE=$S pulsar/rehearsal/rig.sh deploy harness tune pulsar check    # = common/deploy.sh with BIN_ARCH=linux-arm64
pulsar/rehearsal/rig.sh loader-tools                                 # install.sh on pl1 for pulsar-admin / pulsar-perf
```
- deploy: harness copied to 4 hosts; os-tune skipped every sysctl a container cannot set (as designed);
  install.sh: tarball from `/cache`, `published sha512 = computed sha512` (8c0a63cc…6105971), 261 MB dist, jars
  `bookkeeper-server-4.17.3`, `pulsar-broker-4.2.4`, `zookeeper-jute-3.9.5`, ~2 s per node; `check`: nothing listening.

## 1. cluster.sh reset + health

`pulsar/cluster.sh reset` — first one 24 s: stop+wipe, mkconf (45 overrides per node), ZK ×3 quorum in 3 s
(`follower follower leader`, synced=2), `initialize-cluster-metadata` (cluster bench, web + broker service URLs of all
3), bookies ×3 writable in 3 s, brokers ×3 `/admin/v2/brokers/health` = ok in 10 s, tenant + namespace, health:
```
namespace bench/ns: bundles=48 persistence=E3/Qw3/Qa2 markDeleteRate=1.0 retention=0min/0MB
bookkeeper shell listbookies -rw:  ReadWrite Bookies : 10.202.0.2:3181, 10.202.0.3:3181, 10.202.0.4:3181
proof persistent://bench/ns/probe-health-…: ledger 9 … ensembleSize=3 writeQuorumSize=3 ackQuorumSize=2 ensemble=[…3 bookies…] (broker internalStats?metadata=true)
bookkeeper shell ledgermetadata -ledgerid 9:
  LedgerMetadata{formatVersion=3, ensembleSize=3, writeQuorumSize=3, ackQuorumSize=2, state=OPEN, digestType=CRC32C, …}
```
Resets afterwards: 16 s, 26 s, 27 s (inside run.sh), and a back-to-back pair at the end, both rc=0, 3/3 nodes
`zk=…/imok bookie=rw broker=ok`, proof 3/3/2 — 75 s and 84 s (bookies 18 s and brokers 45 s to become ready while the
shared VM was busy with the other agents' stacks). `cluster.sh stop` + `cluster.sh start` (no wipe): all healthy in
40 s, existing ledgers intact (`fo-partition-0: 2 ledgers`).

**Memory (changed the local profile).** With the brief's local sizes (ZK 128m, bookie 256m + 384m direct, broker 384m
+ 384m direct) an idle node sat at 1.24 of 1.27 GiB: ZGC heaps are a memfd (`shmem 768 MB` = 128+256+384, all
committed + pre-touched) plus `anon ~450 MB` (ZK 96, bookie 128, broker 207 MB). The first catch-up read below
**OOM-killed all three brokers** (`memory.events oom_kill 1` on pb1-3). Local profile now: ZK 64m, bookie 160m + 256m
direct, broker 256m + 256m direct (caches unchanged: 32 MB). After that: `shmem 480 MB`, anon ≤ 540 MB, no more OOM in
any test. The VM profile is untouched (checked by rendering it on pb2: ZK 1g; bookie 3g + 6g direct, write 1536 /
read-ahead 1024 / RocksDB 512 MB; broker 6g + 6g direct, ML cache 2048 MB).

**RocksDB block cache.** Bookie log: `Searching for a RocksDB configuration file in /root/bench/pulsar/node/entry_location_rocksdb.conf`,
`Found a RocksDB configuration file`; `ledgers/current/locations/LOG`: `block_cache … capacity : 33554432` (the
rendered value; the dist's file would give 206150041). Also logged: `Write cache size: 32 MB`, `Read Cache: 32 MB`,
`"journalSyncData" : "true"`.

## 2. The tarball's own tools on pl1

```
common/dexec.sh 10.202.0.5 'T=/root/bench/pulsar/pc.sh; A="--admin-url http://10.202.0.2:8080"
  $T tool bin/pulsar-admin $A topics create-partitioned-topic persistent://bench/ns/perf -p 12
  $T tool bin/pulsar-admin $A topics create-subscription persistent://bench/ns/perf -s sub
  $T tool bin/pulsar-admin $A topics get-partitioned-topic-metadata persistent://bench/ns/perf'     # partitions 12
U=pulsar://10.202.0.2:6650,10.202.0.3:6650,10.202.0.4:6650
common/dexec.sh 10.202.0.5 "TOOL_HEAP=560m /root/bench/pulsar/pc.sh tool bin/pulsar-perf produce -u $U -r 10000 -s 256 -z LZ4 -time 30 -i 10 persistent://bench/ns/perf"
common/dexec.sh 10.202.0.5 "TOOL_HEAP=560m /root/bench/pulsar/pc.sh tool bin/pulsar-perf consume -u $U -ss sub -st Failover -time 25 -i 5 persistent://bench/ns/perf"
```
- produce: **300,067 msgs**, 9,576-10,005 msg/s per 10-s window, 0 failures; aggregated latency p50 5.2 ms, p95 47.6,
  p99 137 ms, max 422 ms (journal fsync on the laptop's virtual disk).
- consume (the backlog, afterwards): **300,067 received**, ~30,000 msg/s catch-up, `ack failed 0`.
- pulsar-perf needs > 384 MB of heap just to start (every subcommand's 5-digit HdrHistograms) and reads only
  `PULSAR_EXTRA_OPTS`: first attempts died with `OutOfMemoryError: Java heap space` on `--help`; `TOOL_HEAP=560m` works,
  but then only one pulsar-perf fits in pl1's 700m, hence produce, then consume. Concurrent produce + consume (tail
  e2e) was done with pload (section 5).
- ledger metadata of the real topic: `pc.sh proof persistent://bench/ns/perf-partition-5` →
  `ledger 16 … ensembleSize=3 writeQuorumSize=3 ackQuorumSize=2`, same from `bookkeeper shell ledgermetadata -ledgerid 16`.

## 3. Balance

`pulsar/cluster.sh balance` on the 12-partition topic: `hash check 4/4 lookups agree` (CRC32 rule), topics 4/3/5 per
broker = ±25% OUT. `pulsar/cluster.sh balance -fix`:
```
fix move 1: bundle 0x8ffffff7_0x9555554c (1 topics) 10.202.0.4:8080 -> 10.202.0.3:8080 (destinationBroker): landed on 10.202.0.3:8080
balance after fix … topics 4/4/4, max deviation 0.0% (target ±10%): OK
```
1000 partitions (section 5, r4): 320/342/338 per broker = 4.0%, OK without moves.

## 4. One bookie stopped and started under load (E3/Qw3/Qa2, 3 bookies)

pulsar-perf produce 2,000 msg/s on a 6-partition topic; `pc.sh stop bookie` on pb3 at t=25 s, `pc.sh start bookie`
at t=92 s:
```
06:54:50  2000 msg/s  p99 22 ms
06:54:54  SIGTERM to bookie 3: its shutdown hook stalls; SIGKILL after STOP_WAIT=30 s (06:55:25)
06:55:00  2000 msg/s  p95 504 ms p99 913 ms
06:55:06  producer: PersistenceError "Not enough non-faulty bookies available" (3,814x), then "fenced" (29,952 retries)
06:55:11 … 06:56:02   0 msg/s (listbookies -rw = 2); broker: "Not enough bookies to create ledger with ensembleSize=3,
                     writeQuorumSize=3 and ackQuorumSize=2"; bookies: "ReplicationWorker failed to replicate Ledger"
06:56:02  bookie 3 back (rw=3 at +6 s); 06:56:12 window: 13,110 msg/s (the parked backlog, latency 25-58 s)
end: 220,032 sent of 220,000 scheduled, failure 0.0 msg/s
```
**Writes do not continue with 2 bookies**: E=3 on exactly 3 bookies leaves no bookie for an ensemble change and no
3-bookie ensemble for a new ledger, so every topic stops until the bookie returns; nothing is lost (the client parks
and resends). Same test with the opt-in `cluster.sh reset BROKER_SET="bookkeeper_delayEnsembleChange=true"` and a
crash stop (`STOP_WAIT=0`): **2,000 msg/s through the whole 40-s outage, 0 send errors**, p99 128-245 ms, one window
with p99 1.68 s when the bookie rejoined; 200,024 sent. (It only lasts until the next ledger rollover, which needs 3
bookies; the outage's entries have 2 copies.)

## 5. run.sh end to end with pload (mqload/bin/linux-arm64/pload)

```
pulsar/run.sh r2-failover 15000 PARTITIONS=12 CONSUMERS=4 DURATION=45s RAMP=5s PLOAD_ENV=GOMEMLIMIT=160MiB
pulsar/run.sh r3-keyshared 12000 PARTITIONS=12 ENTITIES=100000 SUB_TYPE=key_shared CONSUMERS=4 DURATION=40s RAMP=5s JOURNALS=2 PLOAD_ENV=GOMEMLIMIT=160MiB
pulsar/run.sh r4-p1000 10000 PARTITIONS=1000 CONSUMERS=4 DURATION=30s RAMP=5s PLOAD_ENV=GOMEMLIMIT=160MiB
report/report.py pulsar/rehearsal/runs/r2-failover pulsar/rehearsal/runs/r3-keyshared --loaders
```
| run | result | numbers |
|---|---|---|
| r1-failover (15k, 12 p) | FAILED, fixed | one pload OOM-killed in pl1's 700m (3 Go processes, GOGC=400, 642 MB); then run.sh waited 180 s for **zombies**: `sleep infinity` as PID 1 never reaps, `kill -0` says alive → rig now `docker run --init`, run.sh checks the process state |
| r2-failover | DONE in 92 s | 3/3 READY in 3 s; 637,600 pushed, 637,500 consumed (last in-flight unit, drain 0), 0 errors; produce p50 10-11 ms p99 72-74 ms; e2e p50 14 ms p99 111 ms; broker+bookie+zk 1.0 core total (3 nodes) |
| r3-keyshared (100k keys, JOURNALS=2) | DONE in 80 s | 450,000 pushed, 449,900 consumed, 0 errors; produce p99 68-79 ms; e2e p50 10 p99 78 ms; key-based batching on; bookie cookies on journal-0 and journal-1 |
| r4-p1000 (1000 p) | harness OK, loader saturated | subscription on 1000 partitions in 28 s, warm 3.3 s, balance 4%, 3/3 READY in 37 s; then pl1 OOM-killed one pload (686 MB, 19k goroutines) and produce p50 went to 21-27 s; brokers fine (GC pauses ≤ 0.9 ms, no allocation stalls, 0 errors) |

Each DONE dir holds run.env, run.log, cluster-reset/create/health/balance, pload-cmd, stats before/after, threads-mid,
loader1/ (logs, JSON, samples), node1-3/ (samples, logs.tgz, rendered configs). A re-run of a DONE tag prints
`already DONE … skipped`.

## 6. Other checks

- Format as needed: `cluster.sh stop`, `rm -rf /root/bench/data/pulsar/bookie` on pb3, `WAIT=40 cluster.sh start` →
  bookie 3 refuses ("There are directories without a cookie, and this is neither a new environment…",
  InvalidCookieException) → `node 3: bookie down with a cookie error: bookieformat -deleteCookie + restart` → rw=3,
  brokers healthy. (The first version grepped the log for "cookie" and would also have matched the harmless
  "Stamping new cookies" of a first start; it now needs InvalidCookieException in the log tail.)
- `pulsar/run.sh r5-smoke 8000 PARTITIONS=6 CONSUMERS=2 DURATION=20s RAMP=5s TOPIC_TIMEOUT=600s …`: DONE in 91 s after
  the last edits (139,900 pushed, 0 errors, produce p99 77-87 ms).
- Port guard: `python3 -m http.server 9092` on pb1, `pc.sh start broker` → `another system listens on 9092 … refusing
  to start broker`, rc=1; without the listener it starts.
- `DRY=1 pulsar/grid.sh A B C D E F`: 25 run.sh calls as SPEC §7 (A 5 rates, B 5 partition counts, C 6, D 2, E 6
  steps, F 1).
- `pc.sh stats`: RSS/threads/fds, ZGC cycles, pauses (max 0.12-1.74 ms), allocation stalls (ZK's 64m heap: up to
  44 ms), top ERROR lines, journal/entry-log/RocksDB sizes, bench/ns broker metrics. `pc.sh threads`: per-thread CPU
  by name (`ForceWriteThread`, `BookieJournal`, `bookie-io`, `pulsar-io`, `BookKeeperClientWorker-OrderedExecutor`, …).
- Cleanup: `pulsar/rehearsal/rig.sh down` → containers=0 network=0.
