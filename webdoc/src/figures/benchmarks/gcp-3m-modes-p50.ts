import { line } from "@/lib/figure-spec";

// The p50 of the same runs as gcp-3m-modes-p99.ts (same statistic, same folders), on the same page.
export default line({
  alt: "p50 end-to-end latency at 3,000,000 msg/s against the partition count (log scale). QueenMQ 2.4 reads 10 ms at 10 partitions and 6.9 to 8.9 from 100 to 10,000,000. Kafka with a consumer group reads 6.0 to 8.4 ms from 10 to 10,000 partitions and 58 at 25,000. Kafka with a share group reads 6.3 ms at 100 partitions, 6.4 at 1,000 and 78 at 10,000.",
  caption: "Kafka's median stays near 6 ms in both modes up to 1,000 partitions, against QueenMQ 2.4's 7 to 10. In queue mode it reaches 78 ms at 10,000, where QueenMQ reads 7.1.",
  source: "as the p99 figure above",
  x: { label: "partitions in the queue or topic", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "p50 end-to-end latency", format: "ms", ticks: [0, 20, 40, 60, 80], min: 0, max: 88 },
  series: [
    { label: "QueenMQ 2.4", tone: "s1", points: [[10, 10], [100, 7.5], [1000, 6.9], [10000, 7.1], [50000, 7.7], [100000, 7.1], [1000000, 7.5], [10000000, 8.9]] },
    { label: "Kafka, consumer group (streaming)", tone: "s2", points: [[10, 6.4], [100, 6.0], [1000, 6.0], [10000, 8.4], [25000, 58]] },
    { label: "Kafka, share group (queue mode)", tone: "s3", points: [[100, 6.3], [1000, 6.4], [10000, 78]] },
  ],
});
