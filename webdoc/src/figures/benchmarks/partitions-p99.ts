import { line } from "@/lib/figure-spec";

// benchmarks/partitions.mdx (and the home page): p99 at 3,000,000 msg/s of 256-byte messages against the partition
// count, three GCP c4-standard-24-lssd brokers with local NVMe. The median of the three windows in seconds 30 to 60
// (each window: the slowest load process). Queen 2.4 of 2026-10-15 (1,000 and 50,000 partitions of 10-16), Kafka 4.3.1
// of 10-14, Pulsar 4.2.4 and Redpanda 26.2.3 of 10-16. Hollow = not carried (Kafka at 50,000 and 100,000, Pulsar at
// 100,000, Redpanda at 50,000), drawn at the p99 the run read. Kafka, Pulsar and Redpanda were not run past 100,000.
export default line({
  alt: "p99 end-to-end latency at 3,000,000 msg/s against the partition count, both on log scales, on three GCP nodes with local NVMe. QueenMQ 2.4 stays between 15 and 25 ms from 10 to 10,000,000 partitions. Kafka reads 7.5 to 11 ms up to 1,000 partitions, 21 at 10,000 and 105 at 25,000, and does not carry 50,000 (32 s) or 100,000 (38 s). Pulsar reads 20 to 25 ms up to 50,000 and does not carry 100,000 (28 s). Redpanda reads 14 to 44 ms up to 1,000 partitions and 782 at 10,000, and does not carry 50,000 (8.8 s).",
  caption: "3,000,000 msg/s into one queue or topic, p99 end to end. QueenMQ 2.4 stays at 15 to 25 ms from 10 partitions to ten million. Hollow: the system did not carry the rate. Kafka, Pulsar and Redpanda were not run past 100,000 partitions.",
  source: "benchmark-queen/2026-10-15-bytes-per-msg/runs/gcp (fm3-*), benchmark-queen/2026-10-12-three-node-new-forms/runs (a24-*), benchmark-queen/2026-09-30-kafka-pulsar/runs (kafka, pulsar, redpanda)",
  x: { label: "partitions in the queue or topic", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "p99 end-to-end latency", log: true, format: "ms", ticks: [10, 100, 1000, 10000], min: 5, max: 60000 },
  series: [
    { label: "QueenMQ 2.4", tone: "s1", points: [[10, 24], [100, 17], [1000, 16.5], [10000, 20], [50000, 20], [100000, 15], [1000000, 20], [10000000, 25]] },
    { label: "Kafka 4.3.1", tone: "s2", points: [[10, 11], [100, 7.5], [1000, 8.1], [10000, 21], [25000, 105], { x: 50000, y: 32000, hollow: true }, { x: 100000, y: 38000, hollow: true }] },
    { label: "Pulsar 4.2.4", tone: "s3", points: [[10, 25], [100, 25], [1000, 20], [10000, 20], [50000, 21], { x: 100000, y: 28000, hollow: true }] },
    { label: "Redpanda 26.2.3", tone: "s4", points: [[10, 44], [100, 14], [1000, 14], [10000, 782], { x: 50000, y: 8800, hollow: true }] },
  ],
});
