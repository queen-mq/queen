# Queen benchmarks

Archived benchmark sessions of Queen 2.x: the harness, the raw output and the results of each.
The method and the rig are described at
[queenmq.com/benchmarks/methodology](https://queenmq.com/benchmarks/methodology/).

| Folder | What it holds |
|---|---|
| [`2026-09-30-kafka-pulsar/`](./2026-09-30-kafka-pulsar/) | Kafka, Redpanda and Pulsar on Queen's 3-node rig: `SPEC.md`, the runbook, the `mqload` loaders, each system's install and configuration scripts, `report/report.py`, and the Kafka, Redpanda, Pulsar, transaction and Kafka-client runs |
| [`2026-10-10-three-node-1m/`](./2026-10-10-three-node-1m/) | 2.2.0 against 2.1.0 on three machines at 1,000,000 msg/s over 500,000 partitions: both in and out with nothing shed, the apply thread of every node, memory with retention off (flat against 1.8 GB a minute), and the highest rate 2.2.0 carries there (1,400,000 msg/s with 100 messages a request, 60,000 to 70,000 with one) |
| [`2026-10-09-rows-window/`](./2026-10-09-rows-window/) | 2.2.0 against 2.1.0 on one VM, for the memory that follows the dedup window: a queue of 100 GB with retention off, reads from a cold disk, consumers at the tail, a million partitions, 500 quiet queues, a rolling upgrade |
| [`2026-09-27-jepsen-archive/`](./2026-09-27-jepsen-archive/) | Jepsen P8 to P10: the campaign results, the Jepsen stores and the binaries they tested |
| [`laravel-supervisors/`](./laravel-supervisors/) | The Laravel harness: Horizon on Redis against the Queen PHP and Rust supervisors |
| [`2026-09-30-laravel-supervisor-features/`](./2026-09-30-laravel-supervisor-features/) | Prefork, fast scale-up, event-driven and coordinated replicas of the Laravel supervisor |
| [`2026-10-01-laravel-horizon-raft/`](./2026-10-01-laravel-horizon-raft/) | Horizon against the Queen supervisor on the Raft broker |
| [`2026-10-01-linux-vm-horizon-raft/`](./2026-10-01-linux-vm-horizon-raft/) | The same comparison on a dedicated Linux virtual machine |
| [`2026-10-02-laravel-failure-matrix/`](./2026-10-02-laravel-failure-matrix/) | Failures, replicas, a soak and Laravel features |
| [`2026-10-02-laravel-partition-stripes/`](./2026-10-02-laravel-partition-stripes/) | More partition stripes than one pop checks out |
| [`2026-10-05-laravel-auto-prefetch/`](./2026-10-05-laravel-auto-prefetch/) | `prefetch` `'auto'` against a fixed prefetch |
| [`2026-10-05-laravel-worker-memory/`](./2026-10-05-laravel-worker-memory/) | Where the memory of a Laravel worker goes |
| [`2026-07-29-vm-campaign/goload/`](./2026-07-29-vm-campaign/goload/) | `goload`, the Go loader for Queen. The `mqload` loaders share its open-loop pacer, its histograms and its JSON payload |

Results of Queen 1.x, whose storage was PostgreSQL, are in the git history of this repository.
