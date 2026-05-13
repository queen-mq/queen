# Queen benchmarks

Results from benchmarking [Queen MQ](https://github.com/queen-mq/queen) on a 32 vCPU / 62 GiB host with PostgreSQL.

## Sessions

| Date | Image(s) | Tests | Summary |
|---|---|---|---|
| [`2026-04-25`](./2026-04-25/) | `0.14.0.alpha.3` | 14 h long-running production-load test | [long-running.md](./2026-04-25/long-running.md) |
| [`2026-04-26`](./2026-04-26/) | `0.14.0.alpha.3` + `0.12.19` | 18 short benchmarks across 4 axes (partition, batch, queue, consumer group) + version comparison | [README.md](./2026-04-26/README.md) |
| [`2026-04-26` (pipeline · ordered)](./2026-04-26/pipeline-queen.md) | `0.14.0.alpha.5` | 4-stage pipeline at 1 000 partitions (per-entity ordering), real `queen-mq` JS clients via pm2, 20-min run | [pipeline-queen.md](./2026-04-26/pipeline-queen.md) |
| [`2026-04-26` (pipeline · throughput)](./2026-04-26/pipeline-queen-throughput.md) | `0.14.0.alpha.5` | Same 4-stage pipeline at 10 partitions, batch=1 000 (weak ordering, max throughput), 10-min run | [pipeline-queen-throughput.md](./2026-04-26/pipeline-queen-throughput.md) |

## Quick navigation

- **How to reproduce these benchmarks**: [`2026-04-26/HOW-TO-RUN.md`](./2026-04-26/HOW-TO-RUN.md)
- **Throughput envelope**: Queen sustains ~100 k msg/s peak with batch=100 on a single node, ~28–39 k msg/s sustained at production-realistic batch=10. See [`2026-04-26/README.md`](./2026-04-26/README.md) for the full per-axis breakdown.
- **Realistic end-to-end pipeline (ordered config, 1 000 partitions)**: 3 688 msg/s sustained through a 4-stage `producer → worker → q2 fan-out × 2` topology, **p99 = 1.02 s**, **99.96 % delivery completeness**, **0 duplicates**, 3 deadlocks (all absorbed by failover). See [`pipeline-queen.md`](./2026-04-26/pipeline-queen.md).
- **Same pipeline, throughput-tuned config (10 partitions)**: **6 673 msg/s** (+81 %) end-to-end, p99 = 1.10 s, 9× tighter max latency (1.75 s), **3.2× lower Postgres CPU** thanks to bigger pop batches. See [`pipeline-queen-throughput.md`](./2026-04-26/pipeline-queen-throughput.md). Partition count is a continuous knob; the same engine subsumes Kafka-like (per-entity ordered) and RabbitMQ-like (competing consumers) workloads.
