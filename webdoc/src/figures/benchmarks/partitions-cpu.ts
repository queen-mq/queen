import { line } from "@/lib/figure-spec";

// benchmarks/partitions.mdx, "CPU": cores used at 1,000,000 msg/s offered,
// summed over the three brokers. Hollow: behind. Queen's 2,000 point not run.
export default line({
  alt: "Cores used at 1,000,000 msg/s offered against the partition count, log scale, summed over three brokers. Queen stays between 17.2 and 18.4 cores from 200 to 10,000,000 partitions. Kafka goes from 9.7 cores at 200 partitions to 28.3 at 10,000 and 32.4 at 100,000, where it had fallen behind. Redpanda goes from 14.2 to 32.5 at 10,000 and 38.7 at 100,000. Pulsar goes from 7.1 to 10.1 at 10,000 and 38.2 at 100,000.",
  caption: "Queen's cost does not move with the partition count: a partition is a few rows in memory. The others keep machinery per partition, which is cheap at 200 and grows with every one added. Hollow marks: the system had fallen behind.",
  source: "benchmark-queen/2026-09-30-stage03-3node, benchmark-queen/2026-09-30-kafka-pulsar",
  x: { label: "partitions in the queue", log: true, ticks: [100, 1000, 10000, 100000, 1000000, 10000000], min: 150, max: 13000000 },
  y: { label: "cores, three brokers", ticks: [0, 10, 20, 30, 40], min: 0, max: 40, format: "int" },
  series: [
    { label: "Queen", points: [[200, 17.2], [10000, 18.4], [50000, 18.0], [100000, 18.3], [1000000, 18.3], [10000000, 17.3]] },
    { label: "Kafka", points: [[200, 9.7], [2000, 15.3], [10000, 28.3], [50000, 29.2], { x: 100000, y: 32.4, hollow: true }] },
    { label: "Redpanda", points: [[200, 14.2], [2000, 16.2], [10000, 32.5], { x: 50000, y: 35.1, hollow: true }, { x: 100000, y: 38.7, hollow: true }] },
    { label: "Pulsar", points: [[200, 7.1], [2000, 7.8], [10000, 10.1], { x: 50000, y: 37.4, hollow: true }, { x: 100000, y: 38.2, hollow: true }] },
  ],
});
