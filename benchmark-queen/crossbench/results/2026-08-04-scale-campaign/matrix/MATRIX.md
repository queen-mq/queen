# Comparison matrix — 32-core rig, sparse vs dense, full resource telemetry

All four systems, same machine (32 vCPU / 62 GB, Xeon 8358), same resource
parity (`CM_CPUS=32 CM_MEM=56g`, Kafka heap 16 GB, Postgres resized for the box,
`synchronous_commit=on` / `fsync=on` unchanged), same channel-manager topology,
1000 ordered lanes per topic, 60 s rated + 5 s ramp + 30 s drain, fresh volumes
before every cell.

Broker CPU/RSS/disk are 1 Hz cgroup samples over the active window
(`samples/*.csv`); loader was sampled too and never exceeded ~13% of 32 cores,
so no cell is void under the SPEC §5.1 gate.

## Sparse — 2 000 ev/s, d = 2 msg/s per lane (12 000 deliveries/s demanded)

| system | p50 ms | p95 ms | deliv/s | % rate | cores | RSS GB | disk W MB/s | phys. queues | consumers | correctness |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|:--:|
| **pgmq** | **55.1** | 65.5 | 11 700 | 100% | 6.52 | 2.22 | 65.4 | 12 | 96 | PASS |
| **Queen** | 71.5 | 92.7 | 11 700 | 100% | 7.43 | **1.84** | **50.5** | **4** | 48 | PASS |
| **Rabbit** | 92.7 | 120.2 | 11 699 | 100% | 7.01 | 2.56 | 62.0 | **12 000** | **12 000** | PASS |
| **Kafka** | 142.9 | 170.0 | 11 700 | 100% | **2.19** | 11.63 | **14.6** | 4 | 48 | PASS |

Everyone serves the offered rate. Latency order is pgmq < Queen < Rabbit <
Kafka; Kafka is 3x cheaper on CPU than anyone else and 2.6x slower.

## Dense — 12 000 ev/s, d = 12 msg/s per lane (72 000 deliveries/s demanded)

| system | p50 ms | p95 ms | deliv/s | % rate | cores | RSS GB | disk W MB/s | shed | correctness |
|---|---:|---:|---:|---:|---:|---:|---:|---:|:--:|
| **Queen** | **340.0** | **881.7** | **69 110** | **96%** | 16.04 | 4.49 | 77.5 | **0** | PASS |
| **Kafka** | 2 287.0 | 5 439.3 | 64 252 | 89% | **3.92** | 12.18 | **22.1** | 0 | PASS |
| **pgmq** | 11 863.3 | 16 777.2 | 45 622 | 63% | 19.26 | 5.06 | 110.8 | 104 780 | PASS |
| **Rabbit** | 19 951.6 | 39 903.2 | 19 666 | 27% | 9.68 | 2.73 | 161.8 | 363 942 | PASS |

Only Queen and Kafka keep up. Queen is 6.7x faster than Kafka on p50 and serves
more of the offered rate. pgmq and RabbitMQ both fall over and shed.

## The crossover, in one pair of numbers

Going from d = 2 to d = 12, at constant lane count:

| system | sparse p50 | dense p50 | degradation |
|---|---:|---:|---:|
| pgmq | 55.1 | 11 863.3 | **215x** |
| Rabbit | 92.7 | 19 951.6 | 215x |
| Kafka | 142.9 | 2 287.0 | 16x |
| **Queen** | 71.5 | 340.0 | **4.8x** |

pgmq goes from best in the campaign to second-worst. The mechanism is in its own
SQL: `read_grouped_head` has no depth batching — at most one message per group
per read — so at d = 12 it needs twelve times the reads for the same work, and
its `fifo_groups` scan is over the whole table each time. Queen amortises over
the partition visit, which is why its cost curve bends the other way.

## Cost side

Kafka is the CPU champion in both regimes (2.19 / 3.92 cores) but carries a
12 GB JVM heap against Queen's 1.8-4.5 GB, and does it at a weaker durability
tier on a single node (measured separately: forced to `flush.messages=1` it
serves ~1250 of 2000 offered ev/s).

pgmq burns the most CPU (19.26 cores) and the most disk (110.8 MB/s) in the
dense cell and still serves only 63% of the rate — it pays for the fan-out it
materialises (6 physical inserts per ingress event against Queen's 2).

RabbitMQ needs **12 000 queues and 12 000 consumers** to express what Queen
expresses with 4 queues and 48 consumers. On 32 cores that is survivable at
d = 2 (92.7 ms PASS, where the 8-core box could not drain it at all) and
disintegrates at d = 12.

## Caveats

- **Consumer counts are not perfectly matched.** Queen sparse ran 48 consumers
  (4/stage), Queen dense 96 (8/stage), pgmq 96, Kafka 48, Rabbit 12 000 (forced
  by its topology). Worker count was measured to have no effect on Queen's dense
  latency (679.9 ms at both 4 and 8 per stage), but this should be equalised
  before publication.
- **60 s cells.** Queen's instantaneous p50 drifts upward within a run because
  `completedRetentionSeconds` exceeds the run length and nothing is pruned;
  pgmq, which deletes on ack, does not drift. At 180 s the sparse gap probably
  widens in pgmq's favour and the dense result should hold.
- **This matrix runs every system at the SAME lane count**, which is fair for
  throughput but deliberately neutralises dynamic partitioning: with
  `-lanes == -properties` there is no head-of-line blocking for anyone. The
  separate HOL experiment is what exercises that axis.
- Broker CPU peaked at ~19 of 32 cores; the admission invariants
  (`ADMISSION_MIN/INIT=96`, `ADMISSION_MAX=128`, `DB_POOL_SIZE=160`) are
  inherited from the 8-core box and are probably still binding.
