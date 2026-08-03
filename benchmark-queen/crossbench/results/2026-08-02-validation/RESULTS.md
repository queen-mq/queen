# CM-BENCH — validation pass, 2026-08-02

**This is a rig-validation pass, not the campaign.** Runs are 180 s (the campaign
uses 1200 s), there is no ceiling sweep and no cardinality sweep, and two of the
adapters have known quality gaps listed at the bottom. Read the numbers as
"the rig works and here is roughly where each system sits", not as results to
publish.

## Setup

| | |
|---|---|
| broker VM | 104.248.245.59 — 8 vCPU, 16 GB, 96 GB disk, Ubuntu 6.8 |
| loader VM | 159.89.104.168 — same spec |
| link | private network 10.114.0.0/20 |
| workload | SPEC.md §1, P = 1000 properties, R = 2000 ev/s, 180 s + 20 s ramp + 90 s drain |
| demanded | 12 000 deliveries/s, 325 ordered lanes |
| harness | `crossbench` @ branch `benchcomp` |

**Why R = 2000 and not the 5000 design point:** pgmq saturated the 8-core broker
at ~2450 ev/s (see below), so 2000 is the highest rate every system can serve.
The design point has to be set by the weakest system or the table compares a
saturated system with three idle ones.

## Cost to serve — R = 2000 ev/s, all PASS

All runs: 0 gaps, 0 order violations, 0 dups, 0 shed, full offered rate served.

| system | broker cores | e2e p50 | e2e p99 | disk write | physical queues | ordered lanes | members / conns | publishes per event |
|---|---|---|---|---|---|---|---|---|
| **Kafka** (default durability) | **1.73** | 143 ms | 185 ms | 4.8 MB/s | 4 topics | 200 / topic | 48 / 49 | 2.0 |
| **pgmq** | 4.83 | 262 ms | 524 ms | 20.2 MB/s | 12 queues | group per property | 96 / 64 | 6.0 |
| **Queen** wildcard | 5.51 (1.41 broker + 4.09 PG) | 962 ms | 5932 ms | 20.5 MB/s | 4 queues | 1000 / queue | 96 / 1 | 2.0 |
| **Queen** targeted | 5.27 (1.23 + 4.05) | 4194 ms | 8389 ms | 19.5 MB/s | 4 queues | 1000 / queue | 96 / 1 | 2.0 |
| **RabbitMQ** classic | 6.36 | **93 ms** | **131 ms** | 1.0 MB/s | 1200 queues | 100 / group | 1200 / 7 | 6.0 |

Loader CPU stayed between 0.37 and 0.61 cores (5-8% of 8) on every run, so
SPEC.md §5.1 is satisfied by a wide margin: the loader was never the constraint.

## What the pass established

**pgmq's ceiling on 8 cores is ~2450 ev/s.** Offered 5000, it published 2449/s
and shed 280 k events while pinning the broker at 5.36 cores mean / 7.96 max.
That is a genuine CPU-bound ceiling, not a loader artefact — the loader sat at
0.41 cores throughout.

**pgmq reorders when saturated, not when serving.** The 5000 ev/s run produced 14
order violations; the 2000 ev/s run produced none. At saturation e2e latency ran
to 40 s against a 60 s visibility timeout, so leases expired while work was still
queued and a second reader picked up a key already in flight. Report this as a
property of the saturated regime, never as "pgmq reorders".

**Kafka's durability tier is worth about 40% of its throughput, and that is the
number that matters for comparing it with Postgres-backed systems.** At its
default (lazy flush, replication-based durability) it served 2000 ev/s on 1.73
cores. Forced to `flush.messages=1` — fsync per record, the tier that matches
Postgres `synchronous_commit=on` — it served only ~1250 ev/s, e2e p50 rose to
31 s, and disk writes went from 4.8 to 41.2 MB/s. **Caveat that must travel with
this number:** fsync-per-record is not how Kafka is meant to be durable; its real
answer is replication across a cluster, which a single node cannot express. The
honest reading is "on one node, matched to PG's durability, Kafka costs 40% of
its throughput", not "Kafka is slow".

**Queen currently costs more than pgmq on the same substrate and is slower than
everything else here.** 5.51 cores against pgmq's 4.83 at the same rate, with p50
962 ms against 262 ms. This is a loss and it goes in the report (SPEC.md §5.6).
It is also the most interesting thing the pass found, and the ceiling and
cardinality sweeps are what will say whether it is a fixed per-partition cost
(4000 partitions being maintained for only 12 000 deliveries/s) or a per-message
one.

**RabbitMQ is the fastest and the most CPU-hungry.** 93 ms p50 — half of Kafka's —
while burning 6.36 of 8 cores and needing 1200 queues and 1200 consumers to do
it. It also wrote almost nothing to disk (1.0 MB/s against pgmq's 20.2), which
wants explaining before the number is quoted: classic queues keep messages in
memory when consumers keep up.

## Two harness bugs the pass caught

**Queen adapter ignored `Reset`.** It configured the queues but never dropped
them, so the second Queen run inherited the first run's messages and consumer-group
cursors. Properties received the previous run's tail (seq 170) before the new
run's seq 0, producing **32 640 "order violations" that belonged to nobody** and
which all five fan-out groups faithfully reproduced. The identical per-group
violation counts are what gave it away. Fixed: reset now drops queues and
consumer groups. Both Queen modes then verified clean.

**The RabbitMQ image will not boot with a volume on `/var/lib/rabbitmq`.** Docker
seeds a new named volume from the image and the `.erlang.cookie` lands unreadable
by the uid the entrypoint drops to. Fixed by mounting the volume at
`/var/lib/rabbitmq/mnesia` instead, which keeps message storage on the volume.

## Known gaps — do not publish these numbers yet

1. **The Queen targeted mode measured here is my sweeper, not Queen's capability.**
   Each worker owns 125 partitions and visits each once per sweep behind a 250 ms
   floor, so a message waits up to a full sweep for pickup. That is why targeted
   came out *slower* than wildcard (4194 ms against 962 ms p50), the opposite of
   July. The sweeper needs work before the targeted column means anything.
2. **Only one rate, one cardinality, 180 s.** No ceiling for Kafka, Rabbit or
   Queen; no cardinality sweep, so there is no cost model yet.
3. **RabbitMQ ran classic queues only.** The quorum-queue run, which is the tier
   comparable with fsync, has not been done.
4. **Queen's p99 of ~6 s at 2000 ev/s is unexplained** and worth a look before
   the campaign: it is an order of magnitude above the others at a rate nothing
   is struggling with.

## Files

`result.json` per run, plus 1 Hz `broker-*.csv` / `loader-*.csv` samples.
Re-verify any run offline with `cmbench -verify-only <dir> -properties 1000`.
