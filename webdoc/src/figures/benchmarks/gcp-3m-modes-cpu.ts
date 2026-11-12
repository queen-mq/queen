import { line } from "@/lib/figure-spec";

// Broker CPU of the same runs as gcp-3m-modes-p99.ts: the three brokers summed (of 72 vCPU) over the loaded window.
// Hollow = not carried (Kafka streaming at 50,000 and 100,000 partitions). benchmarks/queen-vs-kafka.mdx, "Kafka in queue mode".
export default line({
  alt: "Cores used by the three brokers together at 3,000,000 msg/s against the partition count (log scale). QueenMQ 2.4 uses 21 cores at 10 partitions and 24.6 to 27.7 from 100 to 10,000,000. Kafka with a consumer group uses 14.9 at 10, 18.5 at 100, 21.0 at 1,000, 52.8 at 10,000 and 57.0 at 25,000, and does not carry 50,000 or 100,000. Kafka with a share group uses 24.6 at 100, 35.0 at 1,000 and 52.2 at 10,000.",
  caption: "Queue mode costs Kafka cores: 6 more than streaming at 100 partitions and 14 more at 1,000, 42% above QueenMQ there. QueenMQ's CPU does not move with the partition count.",
  source: "the grid rows of the QueenMQ 2.4 runs; report/report.py over the Kafka folders above",
  x: { label: "partitions in the queue or topic", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "cores, three brokers together (of 72)", ticks: [0, 20, 40, 60], min: 0, max: 64 },
  series: [
    { label: "QueenMQ 2.4", tone: "s1", points: [[10, 21.0], [100, 27.7], [1000, 24.6], [10000, 24.8], [50000, 24.8], [100000, 25.0], [1000000, 24.9], [10000000, 25.4]] },
    { label: "Kafka, consumer group (streaming)", tone: "s2", points: [[10, 14.9], [100, 18.5], [1000, 21.0], [10000, 52.8], [25000, 57.0], { x: 50000, y: 50.2, hollow: true }, { x: 100000, y: 60.5, hollow: true }] },
    { label: "Kafka, share group (queue mode)", tone: "s3", points: [[100, 24.6], [1000, 35.0], [10000, 52.2]] },
  ],
});
