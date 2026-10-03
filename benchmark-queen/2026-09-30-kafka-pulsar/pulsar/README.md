# Pulsar 4.2.4 on Queen's 3-node rig

Three broker droplets, each running **ZooKeeper + bookie + broker** (the standard small production layout), tuned the
way a Pulsar/BookKeeper operator would run them on 16 vCPU / 31 GB / one local ext4 disk, driven by `pload` on the 3
loaders. `../SPEC.md` is the contract (§0 fairness, §5 settings + memory budget, §6 scripts, §7 matrix); this file says
how it is built, why each value, and what bites. `REHEARSAL.md` is the local proof.

Durability class (print it next to every number): **E3 / Qw3 / Qa2, `journalSyncData=true`** — every entry on 3
bookies, acked after 2 bookie journal fsyncs. Same as Queen (3 copies, ack after 2 fsyncs).

## Files

| file | runs on | what |
|---|---|---|
| `install.sh` | node | JDK 21 (apt, only if missing) + `apache-pulsar-4.2.4-bin.tar.gz` from `$CACHE` or downloads.apache.org (archive.apache.org fallback), sha512 checked against the published `.sha512` and a pinned value, unpacked to `$REMOTE_ROOT/pulsar/dist`. Idempotent. |
| `mkconf.sh` | node | renders this node's `zookeeper.conf`, `bookkeeper.conf`, `broker.conf`, `entry_location_rocksdb.conf` and per-process JVM env into `$REMOTE_ROOT/pulsar/node/` by **copying `conf-defaults/` and overriding keys**; writes `overrides.txt` (file, key, default → value) and `overrides.diff` (diff -u against the defaults). |
| `pc.sh` | node | `start/stop/restart <zk\|bookie\|broker\|all>`, `wipe`, `health`, `stats`, `threads`, `mark`, `mkconf`, `format-bookie`, `tool`; plus the cluster helpers cluster.sh runs on node 1 (`zkquorum`, `init-metadata`, `bookies`, `brokers`, `ns-setup`, `proof`, `balance`). |
| `padmin.py` | node | the admin REST calls (namespace policies, broker health, bookie lists, the E/Qw/Qa proof, balance). Runs on a node because the private IPs are not reachable from the Mac. |
| `cluster.sh` | Mac | `reset [KNOB=V...]`, `start`, `stop`, `health`, `balance [-fix]`, `stats`, `mark`, `threads`, `mkconf`. |
| `run.sh` | Mac | one matrix point, collected into `runs/pulsar/<tag>/` (see below). |
| `grid.sh` | Mac | SPEC §7 (A B C D F; E optional) as run.sh calls; skips points with `DONE`; `DRY=1` prints them. |
| `lib.sh` | both | hosts.env loading and helpers (bash 3.2-clean: the Mac's `/bin/bash` is 3.2). |
| `conf-defaults/` | — | the exact 4.2.4 defaults (the `.conf` files are byte-identical to the tarball's `conf/`; `bkenv.sh` there is the Docker image's, one extra snappy line — unused: bin/pulsar sources the dist's own env scripts). |
| `rehearsal/` | Mac | `hosts.env` + `rig.sh` for the local 4-container rehearsal, and its `runs/`. |

## How to run (VM day)

```
cp hosts.env.example hosts.env; vi hosts.env          # IPs, PROFILE=vm, CACHE= (empty: download)
(cd mqload && ./build.sh)
common/deploy.sh harness tune pulsar clock check       # ships, tunes, installs Pulsar on the 3 brokers; runs nothing
pulsar/cluster.sh reset                                # ~1 min: ZK x3, metadata, bookies x3, brokers x3, bench/ns, health
pulsar/cluster.sh health                               # anytime; includes the E3/Qw3/Qa2 proof
DRY=1 pulsar/grid.sh                                   # the run.sh calls of the matrix
pulsar/grid.sh A B C D                                 # then look at B, and:
SOAK_PARTITIONS=<best B> pulsar/grid.sh F
report/report.py runs/pulsar/* --loaders
```
One point by hand: `pulsar/run.sh pB-p2000 1000000 PARTITIONS=2000 CONSUMERS=33` (KEY=VAL list in `run.sh` header;
cluster knobs too, e.g. `JOURNALS=2`). Nothing may run on the brokers while Pulsar is measured and vice versa: every
`pc.sh start` refuses while Queen (6632/7400) or Kafka (9092/9093) listens on the node (`FORCE=1` overrides), and
`deploy.sh check` lists who listens. After Pulsar: `pulsar/cluster.sh stop`.

`run.sh` steps: cluster reset → `pload -create-only -warm` on loader 1 (tenant/namespace exist, partitioned topics,
subscription `sub` on every partition **before** any producer, one warm message per partition consumed) →
`cluster.sh health` (proof on the run's own topic) → `balance -fix` → samplers → `PROCS_PER_LOADER` (3) pload per
loader (`-loader-index i -loaders N -cons-offset i*C -cons-total N*C -rate total/N -local-src <this host's indices>
-start-file`) → wait for N `READY` (`READY_TIMEOUT` 300 s) → `pc.sh mark` + stats → start instant = loader 1's clock +
5 s into every start file → thread snapshot at mid-run → wait → collect → `DONE` only if all N printed `[final]`.

Run dir `runs/pulsar/<tag>/`: `run.env` (every parameter, `SYSTEM=pulsar`, `SHAPE`, `DURABILITY`, `START_MS`),
`run.log`, `cluster-reset.txt`, `create.txt`, `health.txt`, `balance.txt`, `pload-cmd.txt`, `stats-before.txt`,
`stats-after.txt` (GC pauses/allocation stalls/errors since mark), `threads-mid.txt`, `loader<j>/` (pload logs + JSON,
sampler), `node<i>/` (sampler, `logs.tgz` = zk/bookie/broker logs + GC logs + stdout, `node/` = the rendered configs
actually used + `overrides.txt/diff`). A failed point gets `FAILED` (reason) and is re-run by grid.sh; an existing
incomplete dir is moved to `<tag>.prev-<time>`.

## Layout on a node

| what | where |
|---|---|
| dist (PULSAR_HOME) | `/root/bench/pulsar/dist` |
| rendered configs + env | `/root/bench/pulsar/node/` |
| ZooKeeper snapshots / txn log | `/root/bench/data/pulsar/zk/data` (myid), `.../zk/txlog` |
| bookie journal(s) / ledgers | `/root/bench/data/pulsar/bookie/journal` (`journal-0,journal-1` with `JOURNALS=2`), `.../bookie/ledgers` (entry logs + RocksDB `locations/` `ledgers/`) |
| logs (log4j RollingFile, stdout `.out`, GC logs) | `/root/bench/logs/pulsar/{zk,bookie,broker,tools}/` |
| ports | ZK 2181/2888/3888, admin 9990, metrics **8001**; bookie 3181, http 8000; broker 6650, 8080 |

## Settings (every override; everything else is the 4.2.4 default)

| file: key | default → value | why |
|---|---|---|
| broker: `managedLedgerDefaultEnsembleSize / WriteQuorum / AckQuorum` | 2/2/2 → **3/3/2** | the durability class (= Queen's); the namespace policy repeats it (`ns-setup`, mark-delete rate 1.0 = broker default) |
| bookie: `journalSyncData` | true → true (explicit) | fsync before ack; `standalone.conf` ships `false` (08-13 trap), so it is pinned here |
| bookie: `journalDirectories` / `ledgerDirectories` | → 1 journal + 1 ledger dir on the one disk | one disk per box; `JOURNALS=2` = two journal dirs (two journal + force-write threads, ledgers split by id) |
| bookie: `journalPreAllocSizeMB` | 16 → 128 | fewer preallocation stalls at ~300 MB/s of journal |
| bookie: `dbStorage_writeCacheMaxSizeMb` | 25% of direct → 1536 | explicit; BK splits it in two 768 MB halves (active + being flushed) |
| bookie: `dbStorage_readAheadCacheMaxSizeMb` | 25% of direct → 1024 | explicit; tailing reads come from the broker cache, catch-up reads from here |
| bookie: RocksDB block cache | 196 MB → **512 MB** | set as `block_cache=` in a rendered `entry_location_rocksdb.conf` + `entryLocationRocksdbConf=` pointing at it. `dbStorage_rocksDB_blockCacheSize` is **ignored** by BK 4.17 while that file exists (it always does: `conf/`); it is set too, for the record. Verified: RocksDB `LOG` shows the rendered capacity. |
| bookie: `gcWaitTime` | 900000 (default, kept) | 09-30 review: a 1-min scan of every ledger is churn at 100k partitions; the soak fits the disk without it |
| bookie: `httpServerEnabled` / `httpServerPort` | false → true / 8000 | `/api/v1/bookie/state`, `list_bookies` for health; `/metrics` is served there too (the Prometheus provider does not open its own port when the http server is on) |
| bookie: `advertisedAddress`, `metadataServiceUri`, `zkServers` | → private IP, `zk+hierarchical://ip1:2181;ip2:2181;ip3:2181/ledgers`, same list | identity + metadata; `zkServers` (deprecated) kept equal so no tool falls back to localhost |
| broker: `managedLedgerMin/MaxLedgerRolloverTimeMinutes` | 10/240 (defaults, kept) | 09-30 review: 2/5 min = ~333 rollovers/s of ZooKeeper churn at 100k partitions; 70-s points never roll; a 15-min soak writes ~135 GB (lz4) per bookie, fits 387 GB |
| broker: `managedLedgerCacheSizeMB` | 20% of direct → 2048 | tailing reads served from the broker cache |
| broker: `exposeTopicLevelMetricsInPrometheus` | true → false | 1.4 GB `/metrics` at 150k topics (08-13 trap) |
| broker: `brokerDeleteInactiveTopicsEnabled` | true → false | topics vanish after 60 s idle (08-13 trap) |
| broker: `allowAutoTopicCreation` | true → false | topics are created explicitly (partitioned); a typo fails instead of making a 1-partition topic |
| broker: `defaultNumberOfNamespaceBundles` (+ ns created with 48) | 4 → 48 | 16 bundles per broker: topics spread evenly |
| broker: `loadBalancerSheddingEnabled`, `…AutoBundleSplitEnabled`, `…AutoUnloadSplitBundlesEnabled` | true → false | no topic moves inside a run; placement is checked (and fixed) after create by `balance` |
| broker: `forceDeleteNamespaceAllowed` / `forceDeleteTenantAllowed` | false → true | fast cleanup between points |
| broker: `clusterName`, `metadataStoreUrl`, `configurationMetadataStoreUrl`, `advertisedAddress` | → `bench`, `zk:ip1:2181,ip2:2181,ip3:2181` (both), private IP | the cluster |
| zk: `server.1..3`, `dataDir`, `dataLogDir` | → private IPs `:2888:3888`, data disk | the ensemble; `myid` written at start |
| zk: `metricsProvider.httpPort` | 8000 → 8001 | 8000 is the bookie's http port on the same box |
| zk: `4lw.commands.whitelist` | (`*` from bin/pulsar's `-D`) → `ruok,mntr,srvr,stat,conf,isro,envi,cons,wchs` | health (`ruok`, `mntr`) without the expensive ones (`dump`, `wchp`) on a big tree |
| GC (all three) | Pulsar's JDK 21 default, **not overridden** | `-XX:+UseZGC -XX:+ZGenerational -XX:+PerfDisableSharedMem -XX:+AlwaysPreTouch`, GC log `-Xlog:gc*,safepoint` async |

Which env var sizes which JVM (read from 4.2.4's `bin/pulsar`, `conf/pulsar_env.sh`, `conf/bkenv.sh`): ZooKeeper and
broker = `PULSAR_MEM`; bookie = `BOOKIE_MEM`, which **falls back to `PULSAR_MEM`** when that is set (bin/pulsar sources
bkenv.sh for every command but pulsar_env.sh not for the bookie). The rendered `bookie.env` unsets `PULSAR_MEM`. GC
flags come from `PULSAR_GC`/`BOOKIE_GC`, left unset so Pulsar's defaults apply.

Client side (pload, SPEC §4): batching 5 ms / 1000 msgs / 128 KB, lz4, KeyBasedBatchBuilder for Key_Shared, receiver
queue 1000, ack grouping 100 ms, batch-index acks (broker default `acknowledgmentAtBatchIndexLevelEnabled=true`).

## Memory budget per node (31 GB)

| process | PROFILE=vm | PROFILE=local (rehearsal, 1300m container) |
|---|---|---|
| ZooKeeper | heap 1 GB | 64 MB |
| bookie | heap 3 GB + direct ≤ 6 GB (write cache 1.5 GB, read-ahead 1 GB, rest Netty) + RocksDB block cache 512 MB (native) + memtables (≤ 4 × 64 MB) | 160 MB + ≤ 256 MB direct, caches 32 MB |
| broker | heap 6 GB + direct ≤ 6 GB (managed-ledger cache 2 GB, rest Netty) | 256 MB + ≤ 256 MB direct, ML cache 32 MB |
| JVM overhead (3 JVMs: metaspace, code, threads, GC) | ~1.5 GB | ~0.45 GB measured |
| total | ≈ 24 GB worst case → ~7 GB page cache | ≈ 1.0 GB non-reclaimable under load (measured) |

ZGC heaps are memfd-backed and fully committed at start (`Xms=Xmx` + AlwaysPreTouch): they show up as **shmem**
(`Shmem`, counted in `Cached` in /proc/meminfo and in `free`'s buff/cache), not as anonymous memory. On a droplet
`Cached` is ~10 GB higher than the real page cache for that reason — read the sampler's `Cached_kb` with that in mind.
Memory knobs: `MEM_ZK`, `MEM_BOOKIE`, `MEM_BROKER` (whole JVM strings) on `cluster.sh reset` / `run.sh`.

## Balance

A partitioned topic's partitions are spread over the namespace's 48 bundles by the broker's rule: CRC32 of the full
partition name (`NamespaceBundleFactory`); padmin computes the same and checks it against the lookup API on a sample
(4/4 agree in the rehearsal). The modular load manager (4.2.4's default) spreads **bundles** evenly (16/16/16), but a
bundle holds a random number of partitions. Simulated with that hash and a random 16/16/16 placement, the worst
broker's topic count is off the mean by (median / p90): 48 partitions 12.5% / 25%, 200: 8.5% / 16%, 2k: 3.2% / 5.9%,
10k: 1.0% / 1.8%, 100k: 0.2% / 0.4% — so only points A, D and B-200 need moves. `balance`
prints bundles + topics per broker; `-fix` unloads, from the heaviest broker, the bundle whose move best narrows the
spread, with `destinationBroker=` the lightest (honoured by ModularLoadManagerImpl in 4.2.4: verified), until every
broker is within ±10% or no whole-bundle move helps. Bundles holding topics but never looked up are assigned first.

## Opt-in knobs (not used by the grid)

`JOURNALS=2`; `ZK_SET` / `BOOKIE_SET` / `BROKER_SET="k=v;k=v"` (any extra override, recorded in `overrides.txt`);
`MEM_*`. Tested: `BROKER_SET="bookkeeper_delayEnsembleChange=true"` (the broker passes `bookkeeper_*` to its BK
client) keeps E3/Qw3/Qa2 topics writable while one of the 3 bookies is down (entries of the outage then have 2 copies,
like Kafka at ISR 2 or Queen with a node down) — until the next ledger rollover, which needs 3 bookies again.

## Traps (08-13 and new)

- **Topic-level metrics** (08-13): `/metrics` is 1.4 GB at 150k topics → off. **Inactive-topic deletion** (08-13):
  topics vanish after 60 s idle → off. **Standalone** has `journalSyncData=false` (08-13): never benchmark standalone.
- **`dbStorage_rocksDB_blockCacheSize` does nothing** since BK 4.15 while `conf/entry_location_rocksdb.conf` exists
  (relative path, found because bin/pulsar `cd`s to PULSAR_HOME): the block cache is that file's `block_cache=`.
- **BOOKIE_MEM falls back to PULSAR_MEM**: an exported PULSAR_MEM silently gives the bookie the broker's heap.
- **Tools pre-touch Pulsar's default 2 GB heap**: `bin/pulsar initialize-cluster-metadata`, `bin/bookkeeper shell`
  and `pulsar-perf` source pulsar_env/bkenv (ZGC + AlwaysPreTouch + `-Xms2g`). `pc.sh tool` gives them small heaps and
  serial GC. **pulsar-perf reads only `PULSAR_EXTRA_OPTS`** (not PULSAR_MEM/GC) and needs > 384 MB of heap just to
  print `--help` (every subcommand's 5-digit HdrHistograms are allocated at start): `TOOL_HEAP=560m`.
- **E3/Qw3/Qa2 on exactly 3 bookies**: no spare bookie for an ensemble change, so losing one bookie **stops all writes**
  (every ledger uses all 3). Observed: 0 msg/s for the whole 55 s outage (`Not enough non-faulty bookies`, then
  `fenced`), no loss, the backlog flushed when the bookie returned. Bookie AutoRecovery (on by default) logs
  `ReplicationWorker failed to replicate` meanwhile: there is nowhere to re-replicate. A failure test needs 4+
  bookies or the delayEnsembleChange knob above.
- **A bookie under load ignores SIGTERM** for > 30 s (shutdown hook stalls): pc.sh SIGKILLs after `STOP_WAIT` (30 s;
  `reset` uses 5 s — the data is wiped anyway).
- ZooKeeper 3.9's `mntr` reports `zk_synced_followers` as `2.0`.
- pload's `-persistence E,Qw,Qa` would POST `managedLedgerMaxMarkDeleteRate: 0` (= every mark-delete persisted to the
  cursor ledger); run.sh never passes it — the namespace policy is `cluster.sh`'s (rate 1.0, the broker default).
- All ports listen on 0.0.0.0 (Pulsar defaults), i.e. on the droplets' public IPs too: keep the droplets behind a
  VPC-only cloud firewall.
- Rehearsal only: ZGC's memfd heaps count against a container's memory limit as shmem; containers need an init
  (`docker run --init`) or exited daemons stay zombies that `kill -0` reports alive (run.sh checks the process state).
