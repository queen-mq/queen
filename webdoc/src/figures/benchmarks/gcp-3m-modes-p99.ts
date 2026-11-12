import { line } from "@/lib/figure-spec";

// Queen next to Kafka in its two consuming modes at 3M msg/s of 256-byte messages, 2026-10-14, three GCP
// c4-standard-24-lssd brokers. Streaming = a consumer group (one offset per partition); queue mode = a share group
// (KIP-932: a lock and an ack per record, 297 consumers, group.share.max.size raised to 300). The median of the three
// windows in seconds 30 to 60 (each the slowest of nine load processes). Kafka on XFS, 24/12/12 threads, writeback
// from 1 GiB. Queue mode was run at 100, 1,000 and 10,000 partitions only. benchmarks/queen-vs-kafka.mdx, "Kafka in
// queue mode".
export default line({
  alt: "p99 end-to-end latency at 3,000,000 msg/s against the partition count (log scale): QueenMQ 2.4, Kafka 4.3.1 with a consumer group and Kafka 4.3.1 with a share group. QueenMQ reads 24 ms at 10 partitions, 17 at 100, 16.5 at 1,000, 20 at 10,000 and 50,000, 15 at 100,000, 20 at 1,000,000 and 25 at 10,000,000. Kafka with a consumer group reads 11, 7.5, 8.1 and 21 ms from 10 to 10,000 partitions and 105 at 25,000 (not drawn). Kafka with a share group reads 9.3 ms at 100 partitions, 23 at 1,000 and 163 at 10,000.",
  caption: "In queue mode Kafka's p99 rises from 9 ms at 100 partitions to 23 at 1,000 and 163 at 10,000, where QueenMQ 2.4 reads 16.5 and 20. In streaming it stays at 8 to 21 ms up to 10,000 (105 at 25,000, off the axis).",
  source: "benchmark-queen/2026-10-12-three-node-new-forms/runs, the fm3-* folders of benchmark-queen/2026-10-15-bytes-per-msg/runs/gcp and a24-* (QueenMQ); benchmark-queen/2026-09-30-kafka-pulsar/runs/kafka, the *-3000k-gcp24-xfs-t24 and *-3000k-gcp24-xfs-t24-share folders (Kafka)",
  x: { label: "partitions in the queue or topic", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "p99 end-to-end latency", format: "ms", ticks: [0, 40, 80, 120, 160], min: 0, max: 176 },
  series: [
    { label: "QueenMQ 2.4", tone: "s1", points: [[10, 24], [100, 17], [1000, 16.5], [10000, 20], [50000, 20], [100000, 15], [1000000, 20], [10000000, 25]] },
    { label: "Kafka, consumer group (streaming)", tone: "s2", points: [[10, 11], [100, 7.5], [1000, 8.1], [10000, 21]] },
    { label: "Kafka, share group (queue mode)", tone: "s3", points: [[100, 9.3], [1000, 23], [10000, 163]] },
  ],
});
