import { line } from "@/lib/figure-spec";

// benchmarks/queen-vs-kafka.mdx, "3,000,000 msg/s": p99 at 3M msg/s of 256-byte messages on three GCP
// c4-standard-24-lssd brokers. The median of the three windows in seconds 30 to 60 (each window: the slowest load
// process). Queen 2.4 of 2026-10-15 (1,000 and 50,000 partitions of 10-16), Kafka of 10-14 on XFS with 24/12/12
// threads and writeback from 1 GiB, Pulsar and Redpanda of 10-16. Off the axis: Kafka at 25,000 (105 ms), Redpanda at
// 10,000 (782 ms). Not drawn because the rate was not carried: Kafka at 50,000 and 100,000, Pulsar at 100,000, Redpanda
// at 50,000. Kafka, Pulsar and Redpanda were not run past 100,000.
export default line({
  alt: "p99 end-to-end latency at 3,000,000 msg/s against the partition count (log scale), the median of three 10-second windows, on the same three GCP nodes. QueenMQ 2.4 reads 24 ms at 10 partitions, 17 at 100, 16.5 at 1,000, 20 at 10,000, 20 at 50,000, 15 at 100,000, 20 at 1,000,000 and 25 at 10,000,000. Kafka reads 11 ms at 10, 7.5 at 100, 8.1 at 1,000, 21 at 10,000 and 105 at 25,000, and does not carry 50,000. Pulsar reads 25 ms at 10 and 100, 20 at 1,000, 20 at 10,000 and 21 at 50,000, and does not carry 100,000. Redpanda reads 44 ms at 10, 14 at 100 and 1,000 and 782 at 10,000, and does not carry 50,000.",
  caption: "At 3M msg/s Kafka has the lowest p99 up to 1,000 partitions (7.5 to 11 ms). QueenMQ 2.4 reads 15 to 25 ms from 10 to 10M partitions; Pulsar 20 to 25 ms up to 50,000; Redpanda 14 ms at 100 and 1,000. Off the axis: Kafka 105 ms at 25,000 and Redpanda 782 ms at 10,000. Only QueenMQ carried 100,000 and more.",
  source: "benchmark-queen/2026-10-15-bytes-per-msg/runs/gcp, the fm3-* folders, and benchmark-queen/2026-10-12-three-node-new-forms/runs, a24-* (QueenMQ); benchmark-queen/2026-09-30-kafka-pulsar/runs: kafka/*-3000k-gcp24-xfs-t24, pulsar/1x*-3000k-gcp24*, redpanda/1x*-3000k-gcp24",
  x: { label: "partitions in the queue or topic", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "p99 end-to-end latency", format: "ms", ticks: [0, 10, 20, 30, 40, 50], min: 0, max: 54 },
  series: [
    { label: "QueenMQ 2.4", tone: "s1", points: [[10, 24], [100, 17], [1000, 16.5], [10000, 20], [50000, 20], [100000, 15], [1000000, 20], [10000000, 25]] },
    { label: "Kafka 4.3.1", tone: "s2", points: [[10, 11], [100, 7.5], [1000, 8.1], [10000, 21]] },
    { label: "Pulsar 4.2.4", tone: "s3", points: [[10, 25], [100, 25], [1000, 20], [10000, 20], [50000, 21]] },
    { label: "Redpanda 26.2.3", tone: "s4", points: [[10, 44], [100, 14], [1000, 14]] },
  ],
});
