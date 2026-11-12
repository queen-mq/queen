import { line } from "@/lib/figure-spec";

// benchmarks/queen-vs-kafka.mdx, "3,000,000 msg/s": the three brokers' cores summed (of 72 vCPU) over the loaded
// window, same runs as gcp-3m-p99.ts. Redpanda: its shards' busy time (redpanda_cpu_busy_seconds_total), because its
// reactors poll and the OS counts 66 to 71 cores at every point. Hollow = not carried.
export default line({
  alt: "Cores used by the three brokers together at 3,000,000 msg/s against the partition count (log scale). QueenMQ 2.4 uses 21 cores at 10 partitions and 24.6 to 27.7 from 100 to 10,000,000. Kafka uses 14.9 at 10, 18.5 at 100, 21.0 at 1,000, 52.8 at 10,000 and 57.0 at 25,000, then 50.2 at 50,000 and 60.5 at 100,000, where it did not carry the rate. Pulsar uses 5.4 at 10, 11.4 at 100, 15.1 at 1,000, 17.2 at 10,000 and 21.7 at 50,000, and 22.8 at 100,000, where it did not carry the rate. Redpanda's shards are busy 8.8 cores at 10, 19.9 at 100, 22.4 at 1,000 and 30.7 at 10,000, and 33.8 at 50,000, where it did not carry the rate.",
  caption: "Pulsar uses the least CPU at every count it carried. QueenMQ's does not move with the partition count, 21 to 28 cores. Kafka needs about twice QueenMQ's from 10,000 partitions; Redpanda's grows from 9 to 31 busy cores up to 10,000. Hollow: not carried.",
  source: "the grid rows of the QueenMQ runs (cores of the three brokers); report/report.py over the Kafka, Pulsar and Redpanda runs, Redpanda's busy.txt",
  x: { label: "partitions in the queue or topic", log: true, ticks: [10, 100, 1000, 10000, 100000, 1000000, 10000000], min: 7, max: 14000000 },
  y: { label: "cores, three brokers together (of 72)", ticks: [0, 20, 40, 60], min: 0, max: 64 },
  series: [
    { label: "QueenMQ 2.4", tone: "s1", points: [[10, 21.0], [100, 27.7], [1000, 24.6], [10000, 24.8], [50000, 24.8], [100000, 25.0], [1000000, 24.9], [10000000, 25.4]] },
    { label: "Kafka 4.3.1", tone: "s2", points: [[10, 14.9], [100, 18.5], [1000, 21.0], [10000, 52.8], [25000, 57.0], { x: 50000, y: 50.2, hollow: true }, { x: 100000, y: 60.5, hollow: true }] },
    { label: "Pulsar 4.2.4", tone: "s3", points: [[10, 5.4], [100, 11.4], [1000, 15.1], [10000, 17.2], [50000, 21.7], { x: 100000, y: 22.8, hollow: true }] },
    { label: "Redpanda 26.2.3 (busy shards)", tone: "s4", points: [[10, 8.8], [100, 19.9], [1000, 22.4], [10000, 30.7], { x: 50000, y: 33.8, hollow: true }] },
  ],
});
