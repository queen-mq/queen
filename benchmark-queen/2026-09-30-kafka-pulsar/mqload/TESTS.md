# mqload tests, 2026-09-30 (local)

## Environment

- **Docker VM:** 10 CPUs and 11.7 GB, shared with the Kafka and Pulsar harness agents' clusters (kb1-3/kl1, pb1-3/pl1).
  Their load was bursty (up to ~5 of 10 CPUs), so latency tails and CPU readings are noisy. Where it matters, the
  VM-wide busy CPU during the run is recorded (from /proc/stat).
- **Network:** `mqtest` 10.203.0.0/24. Removed at the end, together with every container and the 3 anonymous volumes
  the kafka image created.
- **Kafka:** `apache/kafka:4.3.1` single KRaft node at 10.203.0.10. `--memory 1g --cpus 3`, `-Xmx512m`, RF 1,
  `group.initial.rebalance.delay.ms=0`.
- **Pulsar:** `apachepulsar/pulsar:4.2.4 bin/pulsar standalone -nfw -nss --advertised-address 10.203.0.20`.
  `--memory 1536m --cpus 3`, `PULSAR_MEM="-Xms512m -Xmx512m -XX:MaxDirectMemorySize=512m"`, E/Qw/Qa 1/1/1, batch-index
  acks on.
- **Loader:** `benchvm:24.04` at 10.203.0.5 (`--memory 768m --cpus 4`), running `bin/linux-arm64/{kload,pload}`
  mounted read-only. Kafka and Pulsar never ran at the same time; container memory limits totaled 1.8 GB (Kafka
  phase) and 2.3 GB (Pulsar phase).
- **Driver:** the scripts are in `localtest/` (run inside the loader container with `/work` mounted).
  `run2.sh <tool> <tag> 3 -- <args>` starts two processes (`-loader-index 0/1 -loaders 2 -consumers 3
  -cons-offset 0/3 -cons-total 6 -local-src 0-1 -start-file /work/<tag>.start -out /work/<tag>-<i>.json`), waits for
  both READY, then writes the start file (now + 3 s, write then mv). With two processes only the sums are
  meaningful: every consumer group or subscription spans both, so a process consumes the other's messages.
- Numbers below come from the `-out` JSONs and the logs. "prod" is produce latency (scheduled → last broker ack of the
  unit), "e2e" is scheduled → consumer receive, in ms, as the worst process. "cpu" is the process's CPU over the
  steady windows.

## Unit tests: `GOWORK=off go test -count=1 -race ./...`, all PASS

| test | what it checks | result |
|---|---|---|
| TestPacerOfferedRate | offered = schedule. 20000 u/s × 2 s → 40000 (+0.000%). 20000 u/s, ramp 1 s → 30005 vs 30000 (+0.017%). 300 u/s → 600. 50000 u/s × 1.5 s → 75000. Every run ends ≤ 1 ms after its end | PASS |
| TestPacerRampShape | F⁻¹ schedule: the first half of a 1 s ramp holds 0.2559 of its units (t² wants 0.25) | PASS |
| TestShedWhenCapFullAndPacerNeverBlocks | cap 3 units, sender never completes, 20000 msg/s for 1 s: 2002 units offered, 1999 shed, 3 sent, producing took 1.012 s (no block); msgs = units × 10 | PASS |
| TestUnitCompletion | a unit completes on its last MsgDone; one failed message fails its unit (pushErr); latency samples = achieved units | PASS |
| TestPickerRRCoversSpaceFromLoaderOffset | rr over 3 topics × 7 partitions covers all 21 pairs in 21 picks; first pick = (0, loader_index·space/loaders) | PASS |
| TestPickerActiveWindowExactlyActivePerSecond | rotate and scatter, 100000 entities, `-active 500`, 2 topics: exactly 500 distinct per second per topic, 5000 after 10 s; `-dist rr -active -active-policy scatter` = scatter window | PASS |
| TestZipfSkew | rejection-inversion zipf, s = 1.1, 1.0, 0.8 over 10000: P(1), P(2), P(10), P(100) within 5% of k^-s/H; topic zipf s=1 over 10: each P(k) within 0.005 | PASS |
| TestPickerZipfPermutation | rank → entity is a bijection; rank 1 (entity 0) is hottest, share ≥ 0.1 | PASS |
| TestTopicZipfPicker | topic counts zipf-ordered | PASS |
| TestStampParseRoundTrip | 100 stamped messages are valid JSON with ts/src and all event fields; ParseStamp round trip; warm message and garbage rejected; payload cap = len; units rotate the pool. Events are ~290 B at `-payload 256` (goload's fixed fields exceed 256) and ~1024 B at 1024 | PASS |
| TestPercentileParityWithGoload | 20 random histograms (≤100k samples, 1 µs..100 s): bucket index and p50/p90/p99/p999/p100 identical to an independent copy of goload's olBucketIndex/olBucketMid/olPercentile; 1500 µs → 1.496 ms | PASS |
| TestConsumerTopicAssignment | cons-total ≥ T: ci → ci % T, members per topic = MembersOfTopic. cons-total < T (1000 topics / 297): every topic exactly one owner, t % 297 == ci | PASS |
| TestLocalSrcAndDurations | `-local-src 3-5,8`; `-duration 65` = 65 s; `-ramp 2.5`; batch defaults 100 (batch) / 1 (keyed) | PASS |
| TestProcSim | 20 × 10 msgs × 200 µs sleeps 40 ms ± slack | PASS |
| TestConsumerPartitionsOwnedExactlyOnce | 11 shapes (T=1 with P 5..100000 and cons-total 6..297; cons-total ≥ T; < T; = T): every partition of every topic owned by exactly one consumer, only by readers of that topic, deterministic and sorted; T=1 equals `p % cons-total == ci`; idle = max(0, cons-total − P) | PASS |
| TestProducerShard | shards `p % n == i` cover each partition once (P up to 100000, n 2..9). Sharded picker only returns its own partitions for rr/rotate/scatter/zipf; rr covers the shard; rotate/scatter give ceil(90/9) = 10 per second per process; 9 rr shards cover all 1000; an empty shard is an error | PASS |
| TestKeyPartitionerMatchesJava (kload) | independent transcription of Java Utils.murmur2 matches Kafka's golden vectors; franz-go StickyKeyPartitioner(nil) = Java placement for keys e0..e199999 over 1, 7, 12, 48, 200, 1000, 100000 partitions | PASS |
| TestConsumerLayout (pload) | failover/exclusive: every `<topic>-partition-<p>` subscribed exactly once over all consumers (P = 100000, 200 with 97 idle, 10 × 1000, 1000 × 100); shared/key_shared/non-partitioned: whole topics; printed totals = summed | PASS |

## (a) kload, 1 topic × 12 partitions, batch mode, 2 processes, 20k msg/s total, 30 s + 10 s drain

Setup and load commands:

```
kload -brokers 10.203.0.10:9092 -rf 1 -min-isr 1 -topic ta3 -partitions 12 -create-only -warm
run2.sh kload ta3 3 -- -brokers 10.203.0.10:9092 -topic ta3 -partitions 12 -create=false -rate 10000 -duration 30s -ramp 5s -report 5s -drain 10s [-group-protocol classic]
```

| run | protocol | pushed = popped = acked (sum) | errors | prod p50 / p99 | e2e p50 / p99 | cpu per process | group stable |
|---|---|---|---|---|---|---|---|
| ta | KIP-848 | 550000 = 550000 = 550000 (12 warm seen, not counted) | 0 | 9.5 / 375 | 10.6 / 391 | 19.6 / 18.3 % | 14.2 s |
| ta2 | KIP-848 | 550100 = 550100 = 550100 | 0 | 7.0 / 20.9 | 7.2 / 23.2 | 8.9 / 8.6 % | 13.1 s |
| ta3 | KIP-848 | 550100 = 550100 = 550100 | 0 | 7.8 / 1450 | 8.2 / 1745 | 18.9 / 18.9 % | 13.1 s |
| ta4 | classic | 550100 = 550100 = 550100 | 0 | 7.3 / 39.2 | 7.7 / 42.8 | 12.7 / 12.9 % | 4.1 s |

**PASS**: pushed == popped == acked, zero errors, and e2e p50 7–10 ms. Two tail events were not the loader:

- ta's first window had p99 1.24 s in both processes. It was the brand-new broker JVM's first load (JIT); ta2, run
  right after with `MQLOAD_SLOW_MS=300`, printed no unit ≥ 300 ms and had a first-window p99 of 25 ms.
- ta3's 4th window stalled ~1.7 s in produce and e2e at once, in both processes (two separate Go runtimes), so the
  stall was on the broker or VM side.

**KIP-848 stability bug found and fixed.** tq1: 1 process, 2 consumers, load started right after STABLE. The first
local judgement accepted "all 12 partitions held locally + 3 s quiet", but consumer c1 held all 12 while the target
had already moved 6 to c0. The franz-go client log shows c0 receiving partitions 6-11 only at the next heartbeat,
10 s after joining, and the e2e p99 of that window was 4751 ms. Every group is now also judged group-wide
(ConsumerGroupDescribe: Stable, epoch == assignment epoch, member epochs, assignment == target; classic:
DescribeGroups).

- tq2 (KIP-848, fixed): STABLE after 13.2 s (Assigning → Reconciling → Stable); e2e p99 in the first window 28.8 ms,
  then ≤ 14 ms.
- tq3 (classic): STABLE after 4.1 s; first window 22.4 ms.

**KIP-848 CPU.** Sequential runs showed 17.7% (tp1, KIP-848) vs 8.5% (tp2, classic), but the produce path alone
differed ~2x between them, so VM noise confounded the comparison. Run side by side in the same window (tab1 KIP-848
vs tab2 classic, 10k msg/s, 6 consumers each), they measured 11.5% vs 12.2%. **Decision: `-group-protocol consumer`
(KIP-848) is the default.** It works, delivers everything and costs the same CPU; classic stays available.

## (b) kload, 5 topics × 4 partitions, keyed mode, zipf over 10000 entities, 2 processes

```
kload -brokers 10.203.0.10:9092 -rf 1 -min-isr 1 -topic tb -topics 5 -partitions 4 -create-only -warm
run2.sh kload tb 3 -- -brokers 10.203.0.10:9092 -topic tb -topics 5 -partitions 4 -create=false -mode keyed -batch 1 -batch-max 20 -entities 10000 -dist zipf -topic-dist zipf -proc-us 100 -rate 10000 -duration 30s -ramp 5s -report 5s -drain 10s
```

- tb: 52383 units (avg 10.5 msgs). pushed = popped = acked = 550238 (20 warm seen, not counted). 0 errors. prod p50
  6.1 / p99 31 ms, e2e p50 6.6 / p99 37 ms.
- 3 groups per process (`tb-g-t0` has 2 members, one per process); stable after 13.2 s.
- Consumed split 58/42% between processes, as zipf(1.0) topic weights predict for this member layout.
- cpu 31.0 / 27.4%. **PASS**
- tb2 (cons-total 2 < T 5, one process: groups `tb2-g-c0` owning t0,t2,t4 and `tb2-g-c1` owning t1,t3): pushed =
  popped = acked = 176005, 0 errors. **PASS**

## (c) pload, partitioned topic × 12, failover, batch mode, 2 processes

```
pload -url pulsar://10.203.0.20:6650 -admin http://10.203.0.20:8080 -bundles 4 -topic pc2 -partitions 12 -create-only -warm
run2.sh pload pc2 3 -- -url pulsar://10.203.0.20:6650 -admin http://10.203.0.20:8080 -topic pc2 -partitions 12 -create=false -rate 10000 -duration 30s -ramp 5s -report 5s -drain 10s -sub-type failover
```

- Setup: tenant created (allowedClusters [standalone]), namespace with 4 bundles, topic, and subscription `sub`
  verified on all 12 partitions before any producer. Warm: 12 messages sent and consumed in 1.1 s.
- pc (first build: whole-topic consumers, one partitioned producer per process): pushed = popped = acked = 550000, 0
  errors, prod p50 7.6 / p99 12.0, e2e p50 6.9 / p99 17.3, cpu 16.4 / 16.3%. **PASS**
- pc2 (final build: consumer slices + producer shard, the new defaults):
  - process 0 produced to partitions 0,2,…,10 and process 1 to 1,3,…,11, 6 lazy per-partition producers each
    (creation 7–9 ms);
  - 12 consumer registrations in total (each partition once);
  - pushed = popped = acked = 549900, 0 errors, prod p50 7.2, e2e p50 7.7;
  - p99 191 / 220 ms, from two windows where both processes spiked together (VM).
  - **PASS**

## (d) pload, Key_Shared, keyed mode, zipf over 10000 entities, 2 processes

```
pload … -topic pd3 -partitions 12 -sub-type key_shared -create-only -warm
run2.sh pload pd3 3 -- … -topic pd3 -partitions 12 -create=false -sub-type key_shared -mode keyed -batch 1 -batch-max 20 -entities 10000 -dist zipf -rate 10000 -duration 30s -ramp 5s -report 5s -drain 10s
```

KeyBasedBatchBuilder is on automatically for key_shared.

- pd (first build): pushed = popped = acked = 551258, 0 errors, prod p50 5.3 / p99 15.3, e2e p50 9.8 / p99 20.9, cpu
  36.5 / 33.7%. **PASS**
- pd3 (final build): whole-topic consumers (72 registrations) and one partitioned producer with key routing, as
  required for key_shared. pushed = popped = acked = 549685, 0 errors, prod p50 5.7 / p99 13.6, e2e p50 10.3 / p99
  21.4. cpu 51.6 / 53.8%: same code path as pd, so the difference is VM noise. **PASS**

## (e) overload: tiny in-flight cap, offered far above capacity

The 20k msg/s limit is respected by making capacity tiny instead: `-max-inflight 1` plus a 50 ms linger or batch
delay gives ~19 units/s (~1.9k msg/s) against 20k msg/s offered (~11x).

```
kload -brokers 10.203.0.10:9092 -topic te -partitions 12 -create=false -rate 20000 -max-inflight 1 -linger 50ms -duration 20s -ramp 0 -report 5s -drain 3s -consumers 2
pload … -topic pe2 -partitions 12 -create=false -rate 20000 -max-inflight 1 -batch-delay 50ms -duration 20s -ramp 0 -report 5s -drain 3s -consumers 2
```

- te (kload): windows offered 19979 / 20018 / 20000 / 20002 msg/s, achieved ~1800/s, shed ~18200/s. Final: units
  4000 offered = 360 achieved + 3640 shed. prod p50 52.5 ms (the linger). Wall 37 s = 13 s group settle + 20 s + 3 s
  drain + 1 s: the pacer never blocked. **PASS**
- pe (pload): offered 19983–20036/s, achieved ~2000/s, shed ~18000/s; 4000 = 389 + 3611; wall 23 s. **PASS**
  - First-window e2e p99 was 938 ms: the consumers subscribed right before the load, and failover moves the active
    consumer 1 s after a join. Fixed with the `-settle 2s` wait before READY.
  - pe2 (after the fix): offered 19983–20018/s; 4000 = 407 + 3593; first-window e2e p99 56 ms. **PASS**

## (f) every printed window and final line parses

```
python3 check_lines.py /work
```

Each line is checked against:

- Queen's regex
  `^\[(\d\d):(\d\d):(\d\d)\] offered=\s*(\d+)/s achieved=\s*(\d+)/s shed=\s*(\d+)/s .*?p99=\s*([\d.]+).*?ack=\s*(\d+)/s.*?e2e p50=([\d.]+) p99=([\d.]+)`;
- the full SPEC §3 window format;
- the full `[final]` format followed by `load_cpu=`.

The parsed values are also compared with the `-out` JSON windows.

Result: 53 kload/pload logs, **363 window lines and 52 [final] lines, 0 not matching; 319 windows cross-checked
against the JSON, 0 mismatches.** The one log without a final is pk5 (OOM-killed, below). **PASS**

## P = 1000 partitioned topic, low rate (pload, final build, 2 processes)

```
pload … -topic pk1000 -partitions 1000 -create-only -warm
run2.sh pload pk1000 3 -- … -topic pk1000 -partitions 1000 -create=false -rate 1000 -batch 10 -duration 30s -ramp 5s -report 5s -drain 5s
```

- Setup of a 1000-partition topic (same shape, pi1000): topic 1.2 s, subscription 7.9 s, verified on all 1000
  partitions in 0.3 s; warm 1000 messages.
- Each process: shard of 500 partitions, 500 per-partition producers created lazily (avg 2.2 ms, 0 errors).
- Consumer slices: 501 + 499 = 1000 registrations (each partition once).
- pushed = popped = acked = 54990, 0 errors. prod p50 6.0 / p99 11.6, e2e p50 6.4 / p99 15.6.
- Max RSS 179 / 180 MB. **cpu 47.6 / 47.0% per process at only 1000 msg/s**, the per-partition producer cost below.
- **PASS** (functionally)

## pload per-partition cost (1 process, 100 msg/s, 1 consumer unless noted)

| run | layout | cpu % (windows) | goroutines | max RSS |
|---|---|---|---|---|
| pi12 | 12 partitions, 1 partitioned producer + 1 consumer | 6.0, 6.9, 7.0 | 96 | 33 MB |
| pi1000 | 1000 partitions, same | 74.0, 64.0, 52.6 | 7012 | 141 MB |
| producer only | 1000 partition producers | 72.8, 55.8, 52.9 | 3010 | 84 MB |
| consumer only | 1000 partition consumers | 5.6, 5.0, 4.3 (7.0, 8.1 idle) | 4011 | 81 MB |
| 6 consumers, whole topic | 6000 registrations, rate 0 | 41.3, 38.1 | 24023 | |
| 6 consumers, one slice each | 1000 registrations, rate 0 | 6.0, 6.3 | 5017 | |

pulsar-client-go v0.21.0 gives every partition producer a free-running `BatchingMaxPublishDelay` ticker, 200
wakeups/s at 5 ms, plus a 512 KB LZ4 hash table (`NewLz4Provider`). Every partition consumer runs a 100 ms
ack-grouping ticker and allocates its own LZ4 table on first compressed message.

That is **~0.55 cores per 1000 partition producers per process at idle**, and ~0.06 cores per 1000 consumer
registrations.

## Other pload paths (final build, 2 processes, 5k msg/s each, 12 s)

- px1, exclusive, slices: 12 registrations; pushed = popped = acked = 110000; 0 errors. **PASS**
- px2, shared, whole topics: 72 registrations; 110000 each; 0 errors. **PASS**
- px0, non-partitioned topic (`-partitions 0`), failover: 6 registrations, one active consumer; 110000 each; 0 errors.
  **PASS**
- pk6: SIGTERM mid-run gives a graceful stop with `[final]` and `load_cpu` printed. **PASS**
- pk5: Key_Shared, keyed, 100k zipf keys, 20k msg/s, one process. OOM-killed by the 768 MB container limit.
  - Just before, in-flight hit the 5000 cap and p50 was 618 ms: GC thrash near the limit.
  - Cause: key-based batching creates a batch container, including a new 512 KB LZ4 table, per key per flush, and
    GOGC=400 lets that garbage reach ~5x the live heap.
  - The same run succeeded later (pk7). Max RSS in pd1 (Key_Shared, batch mode, 1M keys) was 552 MB, and 427 MB in
    pd2 (failover).
  - Not an issue on 31 GB loaders; set `GOMEMLIMIT` on small hosts.

## Loader CPU per 100k msg/s (produce + consume in one process, 20k msg/s, 6 consumers, 12 partitions, 40 s)

`cpu2.sh <tool> <tag> <args>` = `-rate 20000 -duration 40s -ramp 10s -report 5s -consumers 6 -out …`, plus the
VM-wide busy CPU over the run.

| loader | shape | steady cpu | cores per 100k msg/s | VM busy |
|---|---|---|---|---|
| kload | batch (units of 100) | 24.2% (kc5) | **1.21** (noisy runs kc1/kc2: 1.27, 1.55) | 6% |
| kload | keyed 1..20, zipf 100k keys | 48.1% (kk5) | **2.41** (kk1/kk2: 2.17, 3.09; 4.8–5.7 under heavy VM load) | 11% |
| pload | batch, failover, shard producers (pc7) | 36.1% | **1.80** | 5% |
| pload | batch, failover, partitioned producer (pc8) | 36.1% | 1.80 | 6% |
| pload | Key_Shared, batch mode, 1M keys (pd1, matrix D shape) | 53.6% | **2.68** | 11% |
| pload | Key_Shared, keyed 1..20, zipf 100k keys (pk7) | 64.7% | **3.23** | 11% |

Why keyed kload costs twice batch: its consumers polled ~1090 times/s with ~16 records per poll (batch mode: ~160/s
with ~105 per poll), and it committed once per poll. That is the SPEC's fetch.min.bytes=1 + commit-per-poll shape,
at ~3.3k msg/s per consumer, about the per-consumer rate of the 1M/297-consumer grid.

These are per-process rates of 20k msg/s. At 111k msg/s per process, fixed costs amortize, so treat them as upper
bounds, but add the pload per-partition idle cost above: P/loaders × ~0.55 cores per 1000 partition producers.
