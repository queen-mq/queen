import { line } from "@/lib/figure-spec";

// benchmarks/queen-vs-kafka.mdx, "The ceiling at 1,000 partitions": p99 against the offered rate on one queue or
// topic of 1,000 partitions, a fresh cluster each rate. Queen 2.4 of 2026-10-15 (3,000,000: the median of its two A/B
// runs, 15 and 17 ms), Kafka of 10-14 on XFS with thread pools for 24 vCPU. Queen did not carry 5,500,000; Kafka at
// 10,000,000 read 375 ms and is left off the axis.
export default line({
  alt: "p99 end-to-end latency against the offered rate on 1,000 partitions, QueenMQ 2.4 and Kafka 4.3.1 on the same three GCP nodes, the median of three 10-second windows. QueenMQ reads 16 ms at 3,000,000 msg/s, 28 at 3,500,000, 52 at 4,000,000, 62 at 4,500,000 and 87 at 5,000,000, and does not carry 5,500,000. Kafka reads 8.1 ms at 3,000,000, 8.3 at 4,000,000, 8.8 at 5,000,000, 10.8 at 6,000,000, 10.3 at 7,000,000 and 12.5 at 8,000,000, and 375 ms at 10,000,000.",
  caption: "On 1,000 partitions Kafka's p99 stays between 8 and 13 ms from 3M to 8M msg/s; at 10M it reads 375 ms (not drawn). QueenMQ 2.4's rises from 16 ms at 3M to 87 ms at 5M, and 5.5M is not carried.",
  source: "benchmark-queen/2026-10-15-bytes-per-msg/runs/gcp, the bpm*-fix and fxc-p1000-r* folders (QueenMQ); benchmark-queen/2026-09-30-kafka-pulsar/runs/kafka, the 1x1000-<rate>k-gcp24-xfs-t24 folders (Kafka)",
  x: { label: "offered msg/s", ticks: [3000000, 4000000, 5000000, 6000000, 7000000, 8000000], min: 2800000, max: 8300000 },
  y: { label: "p99 end-to-end latency", format: "ms", ticks: [0, 20, 40, 60, 80, 100], min: 0, max: 110 },
  series: [
    { label: "QueenMQ 2.4", tone: "s1", points: [[3000000, 16], [3500000, 28], [4000000, 52], [4500000, 62], [5000000, 87]] },
    { label: "Kafka 4.3.1", tone: "s2", points: [[3000000, 8.1], [4000000, 8.3], [5000000, 8.8], [6000000, 10.8], [7000000, 10.3], [8000000, 12.5]] },
  ],
});
